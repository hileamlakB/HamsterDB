/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#define _DEFAULT_SOURCE
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <fcntl.h>

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <sys/socket.h>
#include <sys/un.h>
#include <pthread.h>

#include "common.h"
#include "message.h"
#include "utils.h"
#include "thread_pool.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client()
{
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("-- Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
    {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1)
    {
        log_err("client connect failed: ");
        return -1;
    }

    log_info("-- Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

void communicate_server(int client_socket, message send_message, bool wait)
{

    message recv_message;
    int len = 0;
    if (send(client_socket, &(send_message), sizeof(message), 0) == -1)
    {
        log_err("Failed to send message header.");
        exit(1);
    }

    // Send the payload (query) to server
    if (send(client_socket, send_message.payload, send_message.length, 0) == -1)
    {
        log_err("Failed to send query payload.");
        exit(1);
    }

    bool is_done = false;
    // Always wait for server response (even if it is just an OK message)
    while (!is_done)
    {
        if ((len = recv(client_socket, &recv_message, sizeof(message), 0)) > 0)
        {

            if ((recv_message.status == OK_WAIT_FOR_RESPONSE || recv_message.status == OK_DONE) &&
                (int)recv_message.length > 0)
            {
                // Calculate number of bytes in response package
                int num_bytes = (int)recv_message.length;
                char payload[num_bytes + 1];

                // Receive the payload and print it out
                if ((len = recv(client_socket, payload, num_bytes, 0)) > 0)
                {
                    payload[num_bytes] = '\0';
                    printf("%s\n", payload);
                }
            }

            if (!wait)
            {
                is_done = true;
            }

            if (recv_message.status == PRINT_COMPLETE)
            {
                is_done = true;
            }
        }
        else
        {
            if (len < 0)
            {
                log_err("Failed to receive message.");
            }
            else
            {
                log_info("-- Server closed connection\n");
            }

            if (strncmp(send_message.payload, "shutdown", 8) == 0)
            {
                exit(0);
            }

            exit(1);
        }
    }
}

// flush_load - flushes at most one page size of column data from the load buffer
// to backend
void flush_load(int client_socet, char *msg, size_t size)
{

    message send_message;
    send_message.status = OK_DONE;

    send_message.length = size;

    send_message.payload = msg;

    communicate_server(client_socet, send_message, false);
}

typedef struct reader
{
    char *read_map;
    char **write_maps;
    size_t num_columns;
    size_t read_size;
    size_t write_location;
} reader;

void load_file3(int client_socket, char *file_name)
{
    // not tested

    struct stat st;
    stat(file_name, &st);

    // Map file into memory using mmap
    size_t size = st.st_size;
    int fd = open(file_name, O_RDONLY);
    char *file = (char *)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);

    // Read the header
    String header = read_line(file);

    // get the number of columns
    size_t num_columns = 0;
    for (size_t i = 0; i < header.len; i++)
    {
        if (header.str[i] == ',')
        {
            num_columns++;
        }
    }
    num_columns += 1;

    // extract column
    char *column_names[num_columns];
    for (size_t i = 0; i < num_columns; i++)
    {
        column_names[i] = strsep(&header.str, ",");
    }

    // create different temporary files for each column
    // and each thread

    tmp_file tmp_files[num_columns];

    for (size_t i = 0; i < num_columns; i++)
    {
        tmp_files[i] = create_tmp_file(
            column_names[i],
            size * (MAX_INT_LENGTH + 1),
            true,
            true);
    }

    // read the file and copy the data into the temporary files
    size_t i = 0;
    size_t write_location[num_columns];
    for (size_t i = 0; i < num_columns; i++)
    {
        write_location[i] = sprintf(tmp_files[i].map, "load(%s,%012d,", column_names[i], 0);
    }
    while (i < size)
    {

        String line = read_line(file + i);
        for (size_t j = 0; j < num_columns; j++)
        {

            zeropadd(strsep(&line.str, ","), tmp_files[j].map + write_location[j]);
            tmp_files[j].map[write_location[j] + MAX_INT_LENGTH] = ',';
            write_location[j] += MAX_INT_LENGTH + 1;
        }

        i += line.len + 1;
        free(line.str);
    }
    // update size
    for (size_t i = 0; i < num_columns; i++)
    {
        sprintf(tmp_files[i].map + strlen(column_names[i] + 1), "%012d", (int)(write_location[i] + 1));
        tmp_files[i].map[write_location[i]] = ')';
        write_location[i] += 1;
    }
    // flush the data to the backend
    for (size_t i = 0; i < num_columns; i++)
    {
        flush_load(client_socket, tmp_files[i].map, write_location[i]);
        // unmup an close temporary file
        munmap(tmp_files[i].map, tmp_files[i].size);
        close(tmp_files[i].fd);
        free(tmp_files[i].file_name);
    }
}

void load_file4(int client_socket, char *file_name)
{
    //  not tested
    // sends the raw data to the backend in chunks page_size
    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);

    int fd = open(file_name, O_RDONLY);

    struct stat st;
    stat(file_name, &st);
    size_t size = st.st_size;
    char *file = (char *)mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);

    // Read the header
    String header = read_line(file);

    // get the number of columns
    size_t num_columns = 0;
    for (size_t i = 0; i < header.len; i++)
    {
        if (header.str[i] == ',')
        {
            num_columns++;
        }
    }
    num_columns += 1;

    // extract column
    // char *column_names[num_columns];
    // for (size_t i = 0; i < num_columns; i++)
    // {
    //     column_names[i] = strsep(&header.str, ",");
    // }
    // (void *)column_names;

    tmp_file tmp = create_tmp_file(
        "loader",
        size * (MAX_INT_LENGTH + 1),
        true,
        true);

    size_t i = 0;
    while (i < size)
    {
        size_t write_location = sprintf(tmp.map, "load_parallel(%s|", header.str);
        size_t start = i;
        i += PAGE_SIZE;
        while (file[i] != '\n')
        {
            i--;
        }
        memcpy(tmp.map + write_location, file + start, i - start);
        write_location += i - start;
        tmp.map[write_location] = ')';
        write_location += 1;
        flush_load(client_socket, tmp.map, write_location);
    }
    // unmup an close temporary file
    munmap(tmp.map, tmp.size);
    close(tmp.fd);
    free(tmp.file_name);
}

void load_file(int client_socket, char *file_name)
{

    const size_t PAGE_SIZE = 4 * (size_t)sysconf(_SC_PAGESIZE);

    // Make sure the file exists
    struct stat st;
    int exists = stat(file_name, &st);
    if (exists != 0)
    {
        log_err("File does not exist \n");
        return;
    }

    // Map file into memory using mmap
    size_t size = st.st_size;
    int fd = open(file_name, O_RDONLY);
    char *file = (char *)mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);

    // get headers
    String header = read_line(file);

    // get the number of columns
    size_t num_columns = 0;
    for (size_t i = 0; i < header.len; i++)
    {
        if (header.str[i] == ',')
        {
            num_columns++;
        }
    }
    num_columns += 1;

    // extract column
    char *column_names[num_columns];
    size_t column_current[num_columns];
    column_names[0] = header.str;

    column_current[0] = 0; // keeps track of where the column name starts in the header buffer
    size_t last = 1;
    for (size_t i = 0; i < header.len; i++)
    {
        if (header.str[i] == ',')
        {
            header.str[i] = '\0';
            //  set the location of each column index to 0
            column_current[last] = 0;
            column_names[last] = header.str + i + 1;
            last++;
        }
    }

    // for each column, create a buffer of size page_size
    // and at the beginning of each buffer have the load( command
    char colums[num_columns][PAGE_SIZE];
    size_t starting_point[num_columns]; // keeps track of each columns starting point

    for (size_t i = 0; i < num_columns; i++)
    {

        size_t len = sprintf(colums[i], "load(%s,%012d,", column_names[i], 0);
        starting_point[i] = len; // for the size and the comma
        column_current[i] = starting_point[i];
    }

    // in each cyle read page_size bytes
    size_t loaded = header.len + 1;

    // read the whole file
    while (loaded < size)
    {

        // read a line
        char *line = file + loaded;

        // add ecah column data to the column array
        size_t current_col = 0;
        size_t last_read = 0;
        size_t i = 0;

        // read until the end of the line
        while (line[i] != '\n' && current_col < num_columns - 1)
        {

            if (line[i] == ',')
            {

                size_t col_len = i - last_read; // the length of the single entry

                // The flush function sends a single page at a time
                // if an entry is longer than one page, some mechanism should be
                // put in place to partition before sending
                if (col_len + 1 > PAGE_SIZE)
                {
                    log_err("Column data is too large");
                    // free(line.str);
                    return;
                }

                // if writing is going to make it bigger than the columns
                // capacity flush first
                if (column_current[current_col] + MAX_INT_LENGTH > PAGE_SIZE)
                {
                    // flush the column to database if it is full

                    char sz[MAX_INT_LENGTH + 1];
                    sprintf(sz, "%012d", (int)(column_current[current_col] - starting_point[current_col]));

                    strncpy(colums[current_col] + (starting_point[current_col] - 1 - MAX_INT_LENGTH), sz, MAX_INT_LENGTH);
                    colums[current_col][column_current[current_col]] = ')';
                    colums[current_col][column_current[current_col] + 1] = '\0';

                    flush_load(client_socket, colums[current_col], column_current[current_col] + 1);
                    column_current[current_col] = starting_point[current_col];
                }

                // copy a zeropadded string version of the number into the array
                zeropadd(line + last_read, colums[current_col] + column_current[current_col]);

                // since each entry is padded with fixed number of zeros
                column_current[current_col] += MAX_INT_LENGTH;
                colums[current_col][column_current[current_col]] = ',';
                column_current[current_col] += 1;
                current_col += 1;
                last_read = i + 1;
            }
            i += 1;
        }

        // if this isn't the case, it means the file had a line with
        // a wrong format and in that case it should be handled
        assert(current_col == num_columns - 1);

        if (column_current[current_col] + MAX_INT_LENGTH > PAGE_SIZE)
        {
            // flush the column to database if it is full
            char sz[MAX_INT_LENGTH + 1];
            sprintf(sz, "%012lu", column_current[current_col] - starting_point[current_col]);

            strncpy(colums[current_col] + (starting_point[current_col] - 1 - MAX_INT_LENGTH), sz, MAX_INT_LENGTH);
            colums[current_col][column_current[current_col]] = ')';
            colums[current_col][column_current[current_col] + 1] = '\0';

            flush_load(client_socket, colums[current_col], column_current[current_col] + 1);
            column_current[current_col] = starting_point[current_col];
        }

        zeropadd(line + last_read, colums[current_col] + column_current[current_col]);

        column_current[current_col] += MAX_INT_LENGTH;
        colums[current_col][column_current[current_col]] = ',';
        column_current[current_col] += 1;

        // find the length of the last column
        size_t last_col_len = 0;
        while (line[i] != '\n')
        {
            last_col_len++;
            i++;
        }

        loaded += last_read + last_col_len + 1; // including the new line character
    }

    // flush the remaining data to the database
    for (size_t i = 0; i < num_columns; i++)
    {

        char sz[MAX_INT_LENGTH + 1];
        sprintf(sz, "%012lu", column_current[i] - starting_point[i]);

        strncpy(colums[i] + (starting_point[i] - 1 - MAX_INT_LENGTH), sz, MAX_INT_LENGTH);
        colums[i][column_current[i]] = ')';
        colums[i][column_current[i] + 1] = '\0';

        flush_load(client_socket, colums[i], column_current[i] + 1);
        column_current[i] = starting_point[i];
    }

    munmap(file, size);
    close(fd);

    char *complete_message = catnstr(3, "load(complete,", column_names[0], ")");
    free(column_names[0]);

    communicate_server(client_socket, (message){
                                          .length = strlen(complete_message),
                                          .payload = complete_message,
                                      },
                       false);
    free(complete_message);
}

void load_file2(int client_socket, char *file_name)
{
    // read line by line and use realational_insert instead of flush_load

    struct stat st;
    int exists = stat(file_name, &st);
    if (exists != 0)
    {
        log_err("File does not exist \n");
        return;
    }

    // Map file into memory using mmap
    size_t size = st.st_size;
    int fd = open(file_name, O_RDONLY);
    char *file = (char *)mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);

    String header = read_line(file);
    size_t red = header.len + 1;

    char *db = strsep(&header.str, ".");
    char *table = strsep(&header.str, ".");

    char *table_header = catnstr(3, db, ".", table);
    free(db);

    while (red < size)
    {
        String line = read_line(file + red);
        red += line.len + 1;

        char *relationl_insert = catnstr(5, "relational_insert(", table_header, ",", line.str, ")");
        free(line.str);
        communicate_server(client_socket, (message){
                                              .length = strlen(relationl_insert),
                                              .payload = relationl_insert,
                                          },
                           false);
        free(relationl_insert);
    }
}
/**
 * Getting Started Hint:
 *      What kind of protocol or structure will you use to deliver your results from the server to the client?
 *      What kind of protocol or structure will you use to interpret results for final display to the user?
 *
 **/
int main(void)
{

    int client_socket = connect_client();
    if (client_socket < 0)
    {
        exit(1);
    }

    message send_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char *prefix = "";
    if (isatty(fileno(stdin)))
    {
        prefix = "db_client > ";
    }

    char *output_str = NULL;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;
    send_message.status = 0;

    while (printf("%s", prefix), output_str = fgets(read_buffer, DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin))
    {
        if (output_str == NULL)
        {
            log_err("fgets failed.\n");
            break;
        }

        size_t input_length = strlen(read_buffer);
        // Only process input that is greater than 1 character.
        // Convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);

        if (send_message.length > 1)
        {

            size_t i = 0;
            // check if input is the special load command
            for (; i < input_length; i++)
            {
                if (read_buffer[i] != ' ')
                {
                    break;
                }
            }

            // A special case for the load command
            if (strncmp(read_buffer + i, "load", 4) == 0)
            {

                // get the file name (`empty space + load("` )
                // this file name regex could be hacked check again
                char *file_name = read_buffer + i + 6;

                int j = 0;
                while (file_name[j] != '\"')
                {
                    j++;
                }
                file_name[j] = '\0';

                load_file(client_socket, file_name);
            }

            else
            {
                bool wait_til_complete = false;
                if (strncmp(read_buffer + i, "print", 5) == 0)
                {
                    wait_til_complete = true;
                }

                // send the message to the server
                communicate_server(client_socket, send_message, wait_til_complete);
            }
        }
    }
    close(client_socket);
    return 0;
}

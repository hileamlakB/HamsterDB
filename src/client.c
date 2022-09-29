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

#include "common.h"
#include "message.h"
#include "utils.h"

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

void communicate_server(int client_socket, message send_message)
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

    // Always wait for server response (even if it is just an OK message)
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
        exit(1);
    }
}

void flush_load(int client_socet, char *col_name, char *col_data, size_t data_size)
{

    message send_message;
    send_message.status = OK_DONE;

    size_t name_len = strlen(col_name);
    char *load_query = malloc(sizeof(char) * (name_len + data_size + 20));

    if (!load_query)
    {
        log_err("Failed to allocate memory for load query.");
        exit(1);
    }

    sprintf(load_query, "load(%s,", col_name);
    memcpy(load_query + name_len + 6, col_data, data_size);
    load_query[name_len + 6 + data_size] = ')';
    load_query[name_len + 7 + data_size] = '\0';

    send_message.length = name_len + 7 + data_size;
    send_message.payload = load_query;

    // passing the send message like this might not be a good idea
    // as the size might be big and may result in a longer copy time
    communicate_server(client_socet, send_message);
}

void load_file(int client_socket, char *file_name)
{

    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);

    // Make sure the file exists
    struct stat st;
    int exists = stat(file_name, &st);
    if (exists != 0)
    {
        log_err("File does not exist");
        exit(1);
    }

    // Map file into memory using mmap
    size_t size = st.st_size;
    char *file = (char *)mmap(NULL, size, PROT_READ, MAP_PRIVATE, open(file_name, O_RDONLY), 0);

    // get headers
    size_t line_len = line_length(file);
    char header_buffer[line_len + 1];
    strncpy(header_buffer, file, line_len);
    header_buffer[line_len] = '\0';

    // get the number of columns
    size_t num_columns = 0;
    for (size_t i = 0; i < line_len; i++)
    {
        if (header_buffer[i] == ',')
        {
            num_columns++;
        }
    }
    num_columns += 1;

    // extract column

    char *column_names[num_columns];
    size_t column_i[num_columns];
    column_names[0] = header_buffer;
    column_i[0] = 0;
    size_t last = 1;
    for (size_t i = 0; i < line_len; i++)
    {
        if (header_buffer[i] == ',')
        {
            header_buffer[i] = '\0';
            //  set the location of each column index to 0
            column_i[last] = 0;
            column_names[last] = header_buffer + i + 1;
            last++;
        }
    }

    char colums[num_columns][PAGE_SIZE];

    size_t cycle_reads = line_len + 1;
    size_t loaded = 0;
    // read the whole file
    while (loaded < size)
    {

        // read a page_size block at a time
        while (cycle_reads < PAGE_SIZE)
        {

            if (loaded + cycle_reads >= size)
            {
                break;
            }
            // read a line
            line_len = line_length(file + loaded + cycle_reads);
            if (cycle_reads + line_len > PAGE_SIZE)
            {
                break;
            }
            char line_buffer[line_len + 1];

            // you might optimize this by reading the at once instead of copying to a line before first
            strncpy(line_buffer, file + loaded + cycle_reads, line_len);
            line_buffer[line_len] = '\0';

            // add ecah column to the column array

            size_t last = 0;
            size_t str_i = 0;
            size_t j = 0;
            for (; j < line_len; j++)
            {
                if (line_buffer[j] == ',')
                {
                    line_buffer[j] = '\0';
                    size_t col_len = j - str_i;
                    if (col_len + 1 > PAGE_SIZE)
                    {
                        log_err("Column data is too large");
                        exit(1);
                    }

                    if (column_i[last] + col_len > PAGE_SIZE)
                    {
                        // flush the column to database if it is full
                        flush_load(client_socket, column_names[last], colums[last], column_i[last]);
                        column_i[last] = 0;
                    }

                    strcpy(colums[last] + column_i[last], line_buffer + str_i);
                    column_i[last] += col_len;
                    colums[last][column_i[last]] = ',';
                    column_i[last] += 1;
                    last += 1;
                    str_i = j + 1;
                }
            }
            // you can save some time by not iterating through the last colum data in
            // the loop above

            if (last != num_columns - 1)
            {
                // maybe hande errors cases instead of dying
                log_err("wrong number of colums in line ...");
                exit(1);
            }

            strcpy(colums[last] + column_i[last], line_buffer + str_i);
            column_i[last] += (j - str_i);
            colums[last][column_i[last]] = ',';
            column_i[last] += 1;

            cycle_reads += line_len + 1;
        }

        loaded += cycle_reads;
        cycle_reads = 0;
    }

    // flush the remaining data to the database
    for (size_t i = 0; i < num_columns; i++)
    {

        flush_load(client_socket, column_names[i], colums[i], column_i[i]);
    }
    munmap(file, size);
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

                // send the message to the server
                communicate_server(client_socket, send_message);
            }
        }
    }
    close(client_socket);
    return 0;
}

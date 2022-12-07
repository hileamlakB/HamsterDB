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
#include "Utils/utils.h"
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

void communicate_server(int client_socket, message send_message, bool wait, bool shutdown_immediately)
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
    bool print_newline = false;
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
                    printf("%s", payload);
                    print_newline = true;
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

            if (shutdown_immediately)
            {
                log_info("-- Server closed connection\n");
                exit(0);
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

    if (print_newline)
    {
        printf("\n");
    }
}

void load_file4(int client_socket, char *file_name)
{

    int fd = open(file_name, O_RDONLY);

    struct stat st;
    stat(file_name, &st);
    int size = st.st_size;
    char *file = (char *)mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);

    // Read the header
    String header = read_line(file);
    int num_columns = 0;
    for (int i = 0; i < (int)header.len; i++)
    {
        if (header.str[i] == ',')
        {
            num_columns++;
        }
    }
    num_columns += 1;

    char file_size_str[12];
    sprintf(file_size_str, "%d,", size);

    char *starter = catnstr(4, "load_start(", file_size_str, header.str, ")");

    communicate_server(client_socket,
                       (message){
                           .status = OK_DONE,
                           .length = header.len + 12,
                           .payload = starter},
                       false, false);

    int i = header.len + 1;
    while (i < size)
    {

        int j = i + min(LOAD_BATCH_SIZE, size - i);

        // find the last newline
        while (j > i && file[j] != '\n')
        {
            j--;
        }

        communicate_server(client_socket,
                           (message){
                               .status = OK_DONE,
                               .length = j - i,
                               .payload = file + i},
                           false, false);
        i = j + 1;
    }

    communicate_server(client_socket,
                       (message){
                           .status = OK_DONE,
                           .length = 10,
                           .payload = "load_end()"},
                       false, false);

    // unmup an close temporary file
    munmap(file, size);
    close(fd);
    free(starter);
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

                load_file4(client_socket, file_name);
            }

            else
            {
                bool wait_til_complete = false;
                bool shutdown_right_away = false;
                if (strncmp(read_buffer + i, "print", 5) == 0)
                {
                    wait_til_complete = true;
                }
                else if (strncmp(read_buffer + i, "shutdown", 8) == 0)
                {
                    shutdown_right_away = true;
                }

                // send the message to the server
                communicate_server(client_socket, send_message, wait_til_complete, shutdown_right_away);
            }
        }
    }
    close(client_socket);
    return 0;
}

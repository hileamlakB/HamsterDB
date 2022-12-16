/** server.c
 * CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#define _DEFAULT_SOURCE
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "Utils/utils.h"

#include <assert.h>
#include "tasks.h"

#include <stdatomic.h>
#include <pthread.h>

#include <Loader/load.h>
#include <Serializer/serialize.h>
#include <Parser/parse.h>
#include <Create/create.h>
#include <Engine/engine.h>

Db *current_db;

int send_message(int client_socket, message_status status, String result)
{
    message m;
    m.length = result.len;
    // char send_buffer[m.length + 1];

    // strcpy(send_buffer, result.str);
    m.payload = result.str;
    m.status = status;

    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    if (send(client_socket, &(m), sizeof(message), 0) == -1)
    {
        log_err("Failed to send message.");
        exit(1);
    }

    // 4. Send the actual response to the request
    return send(client_socket, result.str, m.length, 0);
}

void free_dbOp(DbOperator *query)
{
    if (query)
    {
        if (query->type == SELECT)
        {
            free(query->operator_fields.select_operator.handler);
            if (query->operator_fields.select_operator.low)
            {
                free(query->operator_fields.select_operator.low);
            }
            if (query->operator_fields.select_operator.high)
            {
                free(query->operator_fields.select_operator.high);
            }
        }
        if (query->type == FETCH)
        {
            free(query->operator_fields.fetch_operator.handler);
        }
        if (query->type == JOIN)
        {
            free(query->operator_fields.join_operator.handler1);
            free(query->operator_fields.join_operator.handler2);
        }

        free(query);
    }
}

void free_db_operator(DbOperator *query)
{
    if (!batch.mode)
    {
        if (batch.num_queries > 0)
        {
            for (size_t i = 0; i < batch.num_queries; i++)
            {
                free_dbOp(batch.queries[i]);
            }
            batch.num_queries = 0;
            free_dbOp(query);
        }
        else
        {
            free_dbOp(query);
        }
    }
}

void send_print_msg(int client_socket, String result, message s_message)
{
    int sent = 0;
    int send_chunk_size = 1024;
    while (sent < (int)result.len)
    {

        int to_send = min(send_chunk_size, (int)result.len - sent);

        String new_result = (String){.str = result.str + sent, .len = to_send};
        int sent_now = send_message(client_socket, s_message.status, new_result);
        if (sent_now == -1)
        {
            log_err("Failed to send message.");
            exit(1);
        }

        sent += sent_now;
    }
    send_message(client_socket, PRINT_COMPLETE, (String){.str = "", .len = 0});
    free(result.str);
}

void handle_client(int client_socket)
{
    int done = 0;
    int length = 0;

    // Create two messages, one from which to read and one from which to receive
    message recv_message, s_message;
    s_message.status = OK_WAIT_FOR_RESPONSE; // default status

    do
    {
        length = recv(client_socket, &recv_message, sizeof(message), 0);

        if (length < 0)
        {
            exit(1);
        }
        else if (length == 0)
        {
            done = 1;
        }

        if (!done)
        {

            char *recv_buffer = malloc(sizeof(char) * (recv_message.length + 1));
            length = recv(client_socket, recv_buffer, recv_message.length, 0);

            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            //    Query string is converted into a request for an database operator
            DbOperator *query = NULL;
            if (bload.mode)
            {
                // loading is a special case where we need to read the entire file
                // before parsing other commands
                String load_str = {
                    .str = recv_message.payload,
                    .len = recv_message.length};
                query = parse_load_parallel(load_str, &s_message);
            }
            else
            {
                query = parse_command(recv_message.payload, &s_message);
            }

            // 2. Handle request
            //    Corresponding database operator is executed over the query
            String result;

            if (batch.mode && query)
            {

                // batch is a spcial execution case where we need to execute all the queries
                // all at once
                assert(query);
                if (query->type == BATCH_EXECUTE)
                {
                    result = execute_DbOperator(query);
                    batch.mode = false;
                    // batch.num_queries = 0;
                }
                else
                {
                    if (query->type != BATCH_QUERY)
                    {
                        assert(query->type == SELECT || query->type == FETCH);
                        batch.queries[batch.num_queries] = query;
                        batch.num_queries++;
                    }
                }
            }
            else
            {
                result = execute_DbOperator(query);
            }

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)

            if (query && query->type == PRINT)
            {
                // print is a special case where we need to send the result in chunks
                send_print_msg(client_socket, result, s_message);
            }
            else
            {
                send_message(client_socket, s_message.status, result);
            }

            // free the db_operator
            free_db_operator(query);

            if (!bload.mode)
            {
                free(recv_buffer);
            }
        }
    } while (!done);

    // log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server()
{
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    // log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
    {
        // log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1)
    {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1)
    {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

void init_bload()
{
    // initalize the bload mutex and condition variable
    pthread_mutex_init(&bload.batch_load_mutex, NULL);
    // pthread_mutex_init(&bload.ticket_lock, NULL);
}

void init_db()
{
    init_bload();
    // init_storage();
    // init_catalog();
    // init_bload();
    // init_batch();
    // init_stats();
}

// This main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.
//
// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients?
//      Is there a maximum number of concurrent client connections you will allow?
//      What aspects of siloes or isolation are maintained in your design? (Think `what` is shared between `whom`?)
int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0)
    {
        exit(1);
    }

    // Setup the database
    init_db();

    while (true)
    {
        // log_info("Waiting for a connection %d ...\n", server_socket);

        struct sockaddr_un remote;
        socklen_t t = sizeof(remote);
        int client_socket = 0;

        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1)
        {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }

        handle_client(client_socket);
    }
    return 0;
}

void shutdown_server(DbOperator *dbo)
{
    if (current_db)
    {
        // cache everything to file
        flush_db(current_db);
    }

    // free var pool
    free_var_pool();
    free_db();
    free(dbo);
    // destroy_thread_pool();
    exit(0);
    // Think about delaying the exit after sending the ack message

    // close all open files
    // for good practice

    // make sure to only close one socket at a time one you implement multiple clients
}

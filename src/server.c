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
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 *
 * Getting started hints:
 *      What are the structural attributes of a `query`?
 *      How will you interpret different queries?
 *      How will you ensure different queries invoke different execution paths in your code?
 **/
char *execute_DbOperator(DbOperator *query)
{

    // free query before you return here
    if (!query)
    {
        return "";
        // return "Unkown query";
    }

    if (query->type == CREATE)
    {
        if (query->operator_fields.create_operator.create_type == _DB)
        {
            struct Status ret_status = create_db(query->operator_fields.create_operator.name);

            free(query);
            if (ret_status.code == OK)
            {

                return "";
            }
            else
            {
                return ret_status.error_message;
            }
        }
        else if (query->operator_fields.create_operator.create_type == _TABLE)
        {
            Status create_status;
            create_table(query->operator_fields.create_operator.db,
                         query->operator_fields.create_operator.name,
                         query->operator_fields.create_operator.col_count,
                         &create_status);
            free(query);
            if (create_status.code != OK)
            {
                cs165_log(stdout, "Adding table failed.\n");
                return "Failed";
            }
            return "";
            // return "Table created successfully";
        }
        else if (query->operator_fields.create_operator.create_type == _COLUMN)
        {
            Status create_status;
            create_column(query->operator_fields.create_operator.table,
                          query->operator_fields.create_operator.name,
                          false,
                          &create_status);
            free(query);
            if (create_status.code != OK)
            {
                cs165_log(stdout, "Adding column failed.");
                return "Failed";
            }
            return "";
        }
    }

    else if (query->type == LOAD)
    {

        Status write_load_status;

        Table *table = query->operator_fields.load_operator.address.table;
        if (query->operator_fields.load_operator.complete)
        {

            size_t loaded = table->columns[0].pending_load;
            bool can_load = true;
            // make sure all the columns have the same amount loaded
            for (size_t i = 0; i < table->col_count; i++)
            {
                if (loaded != table->columns[i].pending_load)
                {
                    can_load = false;
                }
                table->columns[i].pending_load = 0;
            }

            if (can_load == false)
            {
                update_col_end(table);
                return "Load failed: Unbalanced columns\n";
            }
            table->rows += loaded;
            update_col_end(table);
            return "";
        }

        // consider adding status
        write_load(
            table,
            query->operator_fields.load_operator.address.col,
            query->operator_fields.load_operator.data,
            query->operator_fields.load_operator.size,
            &write_load_status);
        free(query);
        if (write_load_status.code == OK)
        {
            return "";
        }
        else
        {
            return "Col write failed: retracting steps";
        }

        // return "File Loaded";
    }
    else if (query->type == SELECT)
    {
        Status select_status;
        select_col(query->operator_fields.select_operator.table,
                   query->operator_fields.select_operator.column,
                   query->operator_fields.select_operator.handler,
                   query->operator_fields.select_operator.low,
                   query->operator_fields.select_operator.high,
                   &select_status);
        free(query);
        return "";
        // return "File Loaded";
    }
    else if (query->type == FETCH)
    {
        Status fetch_status;
        fetch_col(
            query->operator_fields.select_operator.table,
            query->operator_fields.fetch_operator.column,
            query->operator_fields.fetch_operator.variable,
            query->operator_fields.fetch_operator.handler,
            &fetch_status);
        return "";
    }
    else if (query->type == PRINT)
    {

        char *result = print_tuple(query->operator_fields.print_operator);
        free(query);
        return result;
    }
    else if (query->type == AVG)
    {
        average(query->operator_fields.avg_operator.handler,
                query->operator_fields.avg_operator.variable);
        free(query);
        return "";
    }
    else if (query->type == SUM)
    {
        sum(query->operator_fields.avg_operator.handler,
            query->operator_fields.avg_operator.variable);
        free(query);
        return "";
    }
    else if (query->type == ADD)
    {
        add(query->operator_fields.math_operator.handler,
            query->operator_fields.math_operator.operand_1,
            query->operator_fields.math_operator.operand_2);
        free(query);
        return "";
    }
    else if (query->type == SUB)
    {
        sub(query->operator_fields.math_operator.handler,
            query->operator_fields.math_operator.operand_1,
            query->operator_fields.math_operator.operand_2);
        free(query);
        return "";
    }
    else if (query && query->type == SHUTDOWN)
    {
        free(query);
        shutdown_server();
        return "";
        // return "Shutting down";
    }

    free(query);
    return "";
}

char *ftos(float value)
{

    char *str = malloc(sizeof(char) * MAX_INT_LENGTH);
    sprintf(str, "%.2f", value);

    return str;
}

char *print_tuple(PrintOperator print_operator)
{
    if (print_operator.type == SINGLE_FLOAT)
    {
        return ftos(print_operator.data.value);
    }

    int width = print_operator.data.tuple.width;
    int height = print_operator.data.tuple.height;
    Variable **results = print_operator.data.tuple.data;

    // for commas and extra variables, we have + size
    char *result = malloc(sizeof(char) * (width * MAX_INT_LENGTH * height + width));
    char *result_i = result;
    for (int row = 0; row < height; row++)
    {
        for (int col = 0; col < width; col++)
        {
            Variable *current_var = results[col];
            int printed = sprintf(result_i, "%d,", current_var->result.values[row]);
            result_i += printed;
        }
        sprintf(result_i - 1, "\n");
    }
    *(result_i - 1) = '\0';

    return result;
}

int generic_sum(Variable *variable)
{
    int sum = 0;
    for (int i = 0; i < variable->result.size; i++)
    {
        sum += variable->result.values[i];
    }
    return sum;
}

void sum(char *handler, Variable *variable)
{
    add_var(handler,
            (vector){.value = generic_sum(variable), .size = 1},
            FLOAT_VALUE);
}

void average(char *handler, Variable *variable)
{
    add_var(handler,
            (vector){.value = generic_sum(variable) / variable->result.size, .size = 1},
            FLOAT_VALUE);
}

void add(char *handler, Variable *variable1, Variable *variable2)
{

    int *result = malloc(sizeof(int) * variable1->result.size);
    for (int i = 0; i < variable1->result.size; i++)
    {
        result[i] = variable1->result.values[i] + variable2->result.values[i];
    }
    add_var(handler, (vector){.values = result, .size = variable1->result.size}, VALUE_VECTOR);
}

void sub(char *handler, Variable *variable1, Variable *variable2)
{

    int *result = malloc(sizeof(int) * variable1->result.size);
    for (int i = 0; i < variable1->result.size; i++)
    {
        result[i] = variable1->result.values[i] - variable2->result.values[i];
    }
    add_var(handler, (vector){.values = result, .size = variable1->result.size}, VALUE_VECTOR);
}

void handle_client(int client_socket)
{
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext *client_context = NULL;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response to the request.
    do
    {
        length = recv(client_socket, &recv_message, sizeof(message), 0);

        if (length < 0)
        {
            log_err("Client connection closed!\n");
            exit(1);
        }
        else if (length == 0)
        {
            done = 1;
        }

        if (!done)
        {

            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length, 0);

            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            //    Query string is converted into a request for an database operator
            DbOperator *query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

            // 2. Handle request
            //    Corresponding database operator is executed over the query
            char *result = execute_DbOperator(query);

            send_message.length = strlen(result);
            char send_buffer[send_message.length + 1];
            strcpy(send_buffer, result);
            send_message.payload = send_buffer;
            send_message.status = OK_WAIT_FOR_RESPONSE;

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1)
            {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response to the request
            if (send(client_socket, result, send_message.length, 0) == -1)
            {
                log_err("Failed to send message.");
                exit(1);
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
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

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
    {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

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

// Currently this main will setup the socket and accept a single client.
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

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1)
    {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }

    handle_client(client_socket);

    return 0;
}

Status shutdown_server()
{

    // flush stuatus
    Status flush_status;
    // cache everything to file
    flush_db(current_db, &flush_status);
    exit(0);
    // Think about delaying the exit after sending the ack message

    // close all open files
    // for good practice

    // make sure to only close one socket at a time one you implement multiple clients
}
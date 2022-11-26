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
#include <sys/stat.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <fcntl.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"
#include <assert.h>
#include "tasks.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

String empty_string = {
    .str = "",
    .len = 0};
String failed_string = {
    .str = "Failed",
    .len = 6};

String execute_DbOperator(DbOperator *query)
{

    // free query before you return here
    if (!query)
    {
        return empty_string;
        // return "Unkown query";
    }

    if (query->type == CREATE)
    {
        if (query->operator_fields.create_operator.create_type == _DB)
        {
            struct Status ret_status = create_db(query->operator_fields.create_operator.name);

            if (ret_status.code == OK)
            {

                return empty_string;
            }
            else
            {
                return (String){
                    .str = ret_status.error_message,
                    .len = strlen(ret_status.error_message)};
            }
        }
        else if (query->operator_fields.create_operator.create_type == _TABLE)
        {
            Status create_status;
            create_table(query->operator_fields.create_operator.db,
                         query->operator_fields.create_operator.name,
                         query->operator_fields.create_operator.col_count,
                         &create_status);

            if (create_status.code != OK)
            {
                cs165_log(stdout, "--Adding table failed.\n");
            }
            return empty_string;
        }
        else if (query->operator_fields.create_operator.create_type == _COLUMN)
        {
            Status create_status;
            create_column(query->operator_fields.create_operator.table,
                          query->operator_fields.create_operator.name,
                          false,
                          &create_status);

            if (create_status.code != OK)
            {
                cs165_log(stdout, "-- Adding column failed.\n");
            }
            return empty_string;
        }
        else if (query->operator_fields.create_operator.create_type == _INDEX)
        {
            Status create_status;
            create_index(query->operator_fields.create_operator.table,
                         query->operator_fields.create_operator.column,
                         query->operator_fields.create_operator.index_type,
                         query->operator_fields.create_operator.cluster_type,
                         &create_status);

            if (create_status.code != OK)
            {
                cs165_log(stdout, "-- Adding index failed.\n");
            }
            return empty_string;
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
                cs165_log(stdout, "-- Unbalanced column, adding column failed.");
                return failed_string;
            }
            table->rows += loaded;
            update_col_end(table);

            // populte indexes
            for (size_t i = 0; i < table->col_count; i++)
            {
                populate_index(table, &table->columns[i]);
            }

            return empty_string;
        }

        // consider adding status
        // log_info("Loading column %s %d\n", query->operator_fields.load_operator.address.col->name, query->operator_fields.load_operator.size);
        write_load(
            table,
            query->operator_fields.load_operator.address.col,
            query->operator_fields.load_operator.data,
            query->operator_fields.load_operator.size,
            &write_load_status);
        query->operator_fields.load_operator.address.col->pending_load += (query->operator_fields.load_operator.size / (MAX_INT_LENGTH + 1));

        if (write_load_status.code == OK)
        {

            return empty_string;
        }

        // interrupt the loading so that the client will stop
        return failed_string;
    }
    else if (query->type == INSERT)
    {
        Status insert_status;
        insert(query->operator_fields.insert_operator.table,
               query->operator_fields.insert_operator.values,
               &insert_status);
    }
    else if (query->type == SELECT)
    {
        Status select_status;

        if (query->operator_fields.select_operator.type == SELECT_COL)
        {
            select_col(query->operator_fields.select_operator.table,
                       query->operator_fields.select_operator.column,
                       query->operator_fields.select_operator.handler,
                       query->operator_fields.select_operator.low,
                       query->operator_fields.select_operator.high,
                       &select_status);
        }
        else if (query->operator_fields.select_operator.type == SELECT_POS)
        {
            select_pos(
                query->operator_fields.select_operator.pos_vec,
                query->operator_fields.select_operator.val_vec,
                query->operator_fields.select_operator.handler,
                query->operator_fields.select_operator.low,
                query->operator_fields.select_operator.high,
                &select_status);
        }

        return empty_string;
        // return "File Loaded";
    }
    else if (query->type == FETCH)
    {
        Status fetch_status;
        fetch_col(
            query->operator_fields.fetch_operator.table,
            query->operator_fields.fetch_operator.column,
            query->operator_fields.fetch_operator.variable,
            query->operator_fields.fetch_operator.handler,
            &fetch_status);
        return empty_string;
    }
    else if (query->type == PRINT)
    {

        return print_tuple(query->operator_fields.print_operator);
    }
    else if (query->type == AVG)
    {
        average(query->operator_fields.avg_operator.handler,
                query->operator_fields.avg_operator.variable);

        return empty_string;
    }
    else if (query->type == SUM)
    {
        Status sum_status;
        sum(query->operator_fields.avg_operator, &sum_status);

        return empty_string;
    }
    else if (query->type == ADD)
    {
        add(query->operator_fields.math_operator.handler,
            query->operator_fields.math_operator.operand_1,
            query->operator_fields.math_operator.operand_2);

        return empty_string;
    }
    else if (query->type == SUB)
    {
        sub(query->operator_fields.math_operator.handler,
            query->operator_fields.math_operator.operand_1,
            query->operator_fields.math_operator.operand_2);

        return empty_string;
    }
    else if (query->type == MIN || query->type == MAX)
    {
        Status min_max_status;
        MinMax(query->operator_fields.min_max_operator, &min_max_status);

        return empty_string;
    }
    else if (query->type == BATCH_EXECUTE)
    {
        Status batch_status;
        String result = batch_execute(batch.queries, batch.num_queries, &batch_status);
        batch.mode = false;
        return result;
    }
    else if (query && query->type == SHUTDOWN)
    {

        shutdown_server(query);
        return empty_string;
        // return "Shutting down";
    }

    return empty_string;
}

typedef struct created_threads
{
    pthread_t *threads;
    size_t num_threads;
} created_threads;

void *execute_query(void *q_group)
{
    node *query_group = (node *)q_group;
    DbOperator *db_op = (DbOperator *)query_group->val;
    Table *table = db_op->operator_fields.select_operator.table;
    Column *column = db_op->operator_fields.select_operator.column;

    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);
    Status status;
    create_colf(table, column, &status);
    if (status.code != OK)
    {
        log_err("--Error opening file for column write");
        return NULL;
    }

    struct stat sb;
    fstat(column->fd, &sb);

    char *buffer = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, column->fd, column->meta_data_size * PAGE_SIZE);
    if (buffer == MAP_FAILED)
    {
        log_err("--Error mapping file for column read");
        return NULL;
    }

    pthread_t threads[query_group->depth];

    // thread_group *allocated_threads = allocate_threads(query_group->depth);
    size_t depth = query_group->depth;

    thread_select_args args[depth];

    //  here if the number of quries is less than one you won't need to create threads
    // you can use this thread itself for one query
    for (size_t i = 0; i < depth; i++)
    {
        DbOperator *dbs = (DbOperator *)query_group->val;

        args[i] = (thread_select_args){
            .low = dbs->operator_fields.select_operator.low,
            .high = dbs->operator_fields.select_operator.high,
            .file = buffer,
            .read_size = dbs->operator_fields.select_operator.table->rows * (MAX_INT_LENGTH + 1),
            .handle = dbs->operator_fields.select_operator.handler};

        pthread_create(&threads[i], NULL, thread_select_col, &args[i]);
        query_group = query_group->next;
    }

    // wait or all the threads to finish
    for (size_t i = 0; i < depth; i++)
    {
        pthread_join(threads[i], NULL);
        // log_info("Thread %zu finished", i);
        // add results to respctive vectors
    }

    return NULL;
}

String batch_execute(DbOperator **queries, size_t n, Status *status)
{

    if (n == 0)
    {
        status->code = OK;
        return empty_string;
    }

    grouped_tasks gtasks = query_planner(queries, n, status);

    hashtable *independent = gtasks.independent;
    if (independent->count == 0)
    {
        status->code = ERROR;
        return failed_string;
    }

    pthread_t threads[independent->size];
    size_t threads_created = 0;

    for (size_t i = 0; i < independent->size; i++)
    {
        if (independent->array[i])
        {
            node *query_group = independent->array[i];
            pthread_create(&threads[threads_created], NULL, execute_query, (void *)query_group);
            threads_created += 1;
        }
    }

    // wait for all the created threads
    for (size_t i = 0; i < threads_created; i++)
    {
        pthread_join(threads[i], NULL);
    }

    return empty_string;
}

String print_tuple(PrintOperator print_operator)
{
    if (print_operator.type == SINGLE_FLOAT)
    {
        char *str = malloc(sizeof(char) * MAX_INT_LENGTH);
        int len = sprintf(str, "%.2f", print_operator.data.fvalue);
        return (String){.str = str, len = len};
    }
    else if (print_operator.type == SINGLE_INT)
    {
        char *str = malloc(sizeof(char) * MAX_INT_LENGTH);
        int len = sprintf(str, "%d", print_operator.data.ivalue);
        return (String){.str = str, len = len};
    }

    int width = print_operator.data.tuple.width;
    int height = print_operator.data.tuple.height;
    Variable **results = print_operator.data.tuple.data;

    // for commas and extra variables, we have + 1,  MAX_INT_LENGTH + 1, +1 for newline
    size_t size = (width * height * (MAX_INT_LENGTH + 1));
    char *result = malloc(sizeof(char) * ((width * (MAX_INT_LENGTH + 1)) * height + 1) + 1);
    char *result_i = result;
    for (int row = 0; row < height; row++)
    {
        for (int col = 0; col < width; col++)
        {
            Variable *current_var = results[col];
            int printed;
            if (current_var->type == INT_VALUE)
            {
                printed = sprintf(result_i, "%d,", current_var->result.ivalue);
            }
            else if (current_var->type == FLOAT_VALUE)
            {
                printed = sprintf(result_i, "%.2f,", current_var->result.fvalue);
            }
            else
            {
                printed = sprintf(result_i, "%d,", current_var->result.values[row]);
            }
            result_i += printed;
        }

        sprintf(result_i - 1, "\n");
    }
    if (size)
    {
        *(result_i - 1) = '\n';
        *(result_i) = '\0';
    }

    free(results);
    return (String){.str = result, .len = result_i - result};
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

void sum(AvgOperator avg_operator, Status *status)
{
    char *handler = avg_operator.handler;

    if (avg_operator.type == VARIABLE_O)
    {
        Variable *variable = avg_operator.variable;

        add_var(handler,
                (vector){.ivalue = generic_sum(variable), .size = 1},
                INT_VALUE);
    }
    else if (avg_operator.type == COLUMN_O)
    {

        Table *table = avg_operator.address.table;
        Column *column = avg_operator.address.col;

        // check if sum is already calculated
        if (column->sum[0] == 1)
        {
            // handle new inserts and and new loads
            add_var(handler,
                    (vector){.ivalue = column->sum[1], .size = 1},
                    INT_VALUE);
            return;
        }

        const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);

        create_colf(table, column, status);

        if (status->code != OK)
        {
            log_err("Error opening file for column write");
            return;
        }

        struct stat sb;
        fstat(column->fd, &sb);

        // create a map for the column file
        char *buffer = mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, column->fd, 0);
        int index = column->meta_data_size * PAGE_SIZE;

        int sum = 0;
        while (buffer[index] != '\0')
        {
            int num = zerounpadd(buffer + index, ',');
            sum += num;
            index += MAX_INT_LENGTH + 1;
        }

        munmap(buffer, sb.st_size);
        column->sum[0] = 1;
        column->sum[1] = sum;
        add_var(handler,
                (vector){.ivalue = sum, .size = 1},
                INT_VALUE);
    }
}

void average(char *handler, Variable *variable)
{
    add_var(handler,
            (vector){.fvalue = (float)generic_sum(variable) / variable->result.size, .size = 1},
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

void MinMax(MinMaxOperator minmax_operator, Status *status)
{
    status->code = OK;
    char *handler = minmax_operator.handler;
    Variable *variable = minmax_operator.variable;

    int result[2];
    result[0] = variable->result.values[0];
    int index = 1;

    if (minmax_operator.operation == MIN)
    {
        for (int i = 1; i < variable->result.size; i++)
        {
            result[index] = variable->result.values[i];
            index -= variable->result.values[i] < result[0];
            result[index] = variable->result.values[i];
            index = 1;
        }
    }

    else if (minmax_operator.operation == MAX)
    {
        for (int i = 1; i < variable->result.size; i++)
        {
            result[index] = variable->result.values[i];
            index -= variable->result.values[i] > result[0];
            result[index] = variable->result.values[i];
            index = 1;
        }
    }

    add_var(handler,
            (vector){.ivalue = result[0], .size = 1},
            INT_VALUE);
}

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

void handle_client(int client_socket)
{
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message recv_message;
    message s_message;
    s_message.status = OK_WAIT_FOR_RESPONSE; // default status

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
            DbOperator *query = parse_command(recv_message.payload, &s_message, client_socket, client_context);

            // 2. Handle request
            //    Corresponding database operator is executed over the query
            String result;

            if (batch.mode)
            {
                assert(query);
                if (query->type == BATCH_EXECUTE)
                {

                    result = execute_DbOperator(query);
                    batch.mode = false;
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

            if (query && query->type == PRINT)
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
            else
            {
                send_message(client_socket, s_message.status, result);
            }

            // free query
            // if (query && query->type == PRINT)
            // {
            //     free(result.str);
            // }

            if (!batch.mode)
            {
                if (query)
                {
                    if (query->type == SELECT)
                    {
                        free(query->operator_fields.select_operator.handler);
                    }
                    if (query->type == FETCH)
                    {
                        free(query->operator_fields.fetch_operator.handler);
                    }
                }
                free(query);
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

    while (true)
    {
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
    }
    return 0;
}

Status shutdown_server(DbOperator *dbo)
{

    // flush stuatus
    Status flush_status;
    // cache everything to file
    flush_db(current_db, &flush_status);

    // free var pool
    free_var_pool();
    free_db();
    free(dbo);

    exit(0);
    // Think about delaying the exit after sending the ack message

    // close all open files
    // for good practice

    // make sure to only close one socket at a time one you implement multiple clients

    return flush_status;
}
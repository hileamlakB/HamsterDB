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

#define DEFAULT_QUERY_BUFFER_SIZE 1024

String empty_string = {
    .str = "",
    .len = 0};
String failed_string = {
    .str = "Failed",
    .len = 6};
Db *current_db;
linkedList *var_pool;
Column empty_column = {
    .fd = -1,
    .end = 0,
    .data = NULL,
};
Table empty_table;
batch_query batch = {
    .mode = false,
    .num_queries = 0

};

ParallelLoader bload = {
    .mode = false,
    .done = false,
};

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

            create_table(query->operator_fields.create_operator.db,
                         query->operator_fields.create_operator.name,
                         query->operator_fields.create_operator.col_count);

            return empty_string;
        }
        else if (query->operator_fields.create_operator.create_type == _COLUMN)
        {
            Status create_status;
            create_column(query->operator_fields.create_operator.table,
                          query->operator_fields.create_operator.name,
                          &create_status);

            if (create_status.code != OK)
            {
                cs165_log(stdout, "-- Adding column failed.\n");
            }
            return empty_string;
        }
        else if (query->operator_fields.create_operator.create_type == _INDEX)
        {

            create_index(query->operator_fields.create_operator.table,
                         query->operator_fields.create_operator.column,
                         query->operator_fields.create_operator.index_type,
                         query->operator_fields.create_operator.cluster_type);

            return empty_string;
        }
    }

    else if (query->type == INSERT)
    {
        size_t len = strlen(query->operator_fields.insert_operator.value);
        String str = {
            .str = query->operator_fields.insert_operator.value,
            .len = len};

        insert(query->operator_fields.insert_operator.table,
               str);
    }
    else if (query->type == SELECT)
    {
        Status select_status;

        if (query->operator_fields.select_operator.type == SELECT_COL)
        {
            select_args args = {
                .tbl = query->operator_fields.select_operator.table,
                .col = query->operator_fields.select_operator.column,
                .handle = query->operator_fields.select_operator.handler,
                .low = query->operator_fields.select_operator.low,
                .high = query->operator_fields.select_operator.high};
            select_col(&args);
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

        sum(query->operator_fields.avg_operator);

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
    else if (query->type == BATCH_LOAD_START)
    {
        prepare_load(query);
    }
    else if (query->type == BATCH_LOAD_END)
    {
        finish_load(query);
    }
    else if (query->type == BATCH_LOAD)
    {
        // free the linked list
        batch_load(query);
        return empty_string;
    }
    else if (query && query->type == SHUTDOWN)
    {

        shutdown_server(query);
        return empty_string;
        // return "Shutting down";
    }
    else if (query && query->type == JOIN)
    {
        join(query);
    }

    return empty_string;
}

void *execute_query(void *q_group)
{
    node *query_group = (node *)q_group;
    DbOperator *db_op = (DbOperator *)query_group->val;
    Table *table = db_op->operator_fields.select_operator.table;
    Column *column = db_op->operator_fields.select_operator.column;

    map_col(table, column, 0);
    struct stat sb;
    fstat(column->fd, &sb);

    pthread_t threads[query_group->depth];

    // thread_group *allocated_threads = allocate_threads(query_group->depth);
    size_t depth = query_group->depth;

    select_args args[depth];

    //  here if the number of quries is less than one you won't need to create threads
    // you can use this thread itself for one query
    for (size_t i = 0; i < depth; i++)
    {
        DbOperator *dbs = (DbOperator *)query_group->val;

        args[i] = (select_args){
            .low = dbs->operator_fields.select_operator.low,
            .high = dbs->operator_fields.select_operator.high,
            .read_size = dbs->operator_fields.select_operator.table->rows,
            .handle = dbs->operator_fields.select_operator.handler,
            .tbl = table,
            .col = column};
        // handle the case where there are other commands besides select_col
        pthread_create(&threads[i], NULL, select_col, &args[i]);
        query_group = query_group->next;
    }

    // wait or all the threads to finish
    for (size_t i = 0; i < depth; i++)
    {
        pthread_join(threads[i], NULL);
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

    // free the hashtable
    free_grouped_tasks(gtasks);

    return empty_string;
}

String print_tuple(PrintOperator print_operator)
{
    if (print_operator.type == SINGLE_FLOAT)
    {
        char *str = malloc(sizeof(char) * MAX_INT_LENGTH * 2);
        size_t len = sprintf(str, "%.2f", print_operator.data.fvalue);
        return (String){.str = str, .len = len};
    }
    else if (print_operator.type == SINGLE_INT)
    {
        char *str = malloc(sizeof(char) * MAX_INT_LENGTH * 2);
        size_t len = sprintf(str, "%ld", print_operator.data.ivalue);
        return (String){.str = str, .len = len};
    }

    int width = print_operator.data.tuple.width;
    int height = print_operator.data.tuple.height;
    Variable **results = print_operator.data.tuple.data;
    linkedList *lst[width];
    size_t list_indexs[width];

    for (int i = 0; i < width; i++)
    {
        if (results[i]->type == VECTOR_CHAIN)
        {
            lst[i] = results[i]->result.pos_vec_chain;
            list_indexs[i] = 0;
        }
    }

    // for commas and extra variables, we have + 1,  MAX_INT_LENGTH + 1, +1 for newline
    size_t size = (width * height * (MAX_INT_LENGTH + 1));
    char *result = malloc(sizeof(char) * ((width * (MAX_INT_LENGTH + 1)) * height + 1) + 1);
    char *result_i = result;
    for (int row = 0; row < height; row++)
    {
        for (int col = 0; col < width; col++)
        {
            Variable *current_var = results[col];
            int printed = 0;
            if (current_var->type == INT_VALUE)
            {
                printed = sprintf(result_i, "%ld,", current_var->result.ivalue);
            }
            else if (current_var->type == FLOAT_VALUE)
            {
                printed = sprintf(result_i, "%.2f,", current_var->result.fvalue);
            }
            else if (current_var->type == POSITION_VECTOR || current_var->type == VALUE_VECTOR)
            {
                printed = sprintf(result_i, "%d,", current_var->result.values.values[row]);
            }
            else if (current_var->type == VECTOR_CHAIN)
            {

                if (list_indexs[col] >= ((pos_vec *)lst[col]->data)->size)
                {
                    list_indexs[col] = 0;
                    lst[col] = lst[col]->next;
                }

                printed = sprintf(result_i, "%d,", ((pos_vec *)lst[col]->data)->values[list_indexs[col]]);
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

double generic_sum(Variable *variable)
{

    double sum = 0;
    // sum is to be used only for fetched values
    assert(variable->type == VALUE_VECTOR);

    for (size_t i = 0; i < variable->result.values.size; i++)
    {
        sum += variable->result.values.values[i];
    }

    return sum;
}

void sum(AvgOperator avg_operator)
{
    char *handler = avg_operator.handler;

    if (avg_operator.type == VARIABLE_O)
    {
        Variable *variable = avg_operator.variable;
        double sum = generic_sum(variable);
        Variable *fin_result = malloc(sizeof(Variable));
        *fin_result = (Variable){
            .type = INT_VALUE,
            .result.ivalue = sum,
            .name = strdup(handler),
            .exists = true};

        add_var(fin_result);
    }
    else if (avg_operator.type == COLUMN_O)
    {

        Table *table = avg_operator.address.table;
        Column *column = avg_operator.address.col;

        // check if sum is already calculated
        if (column->metadata->sum[0] == 1)
        {
            // handle new inserts and and new loads
            Variable *fin_result = malloc(sizeof(Variable));
            *fin_result = (Variable){
                .type = INT_VALUE,
                .result.ivalue = column->metadata->sum[1],
                .name = strdup(handler),
                .exists = true};
            add_var(fin_result);
            return;
        }

        map_col(table, column, 0);

        double sum = 0;
        size_t index = 0;
        while (index < table->rows)
        {
            sum += column->data[index];
            index++;
        }

        column->metadata->sum[0] = 1;
        column->metadata->sum[1] = sum;

        Variable *fin_result = malloc(sizeof(Variable));
        *fin_result = (Variable){
            .type = INT_VALUE,
            .result.ivalue = sum,
            .name = strdup(handler),
            .exists = true};

        add_var(fin_result);
    }
}

void average(char *handler, Variable *variable)
{

    double avg = (variable->result.values.size) ? (double)generic_sum(variable) / variable->result.values.size : 0;
    Variable *fin_result = malloc(sizeof(Variable));
    *fin_result = (Variable){
        .type = FLOAT_VALUE,
        .result.fvalue = avg,
        .name = strdup(handler),
        .exists = true};
    add_var(fin_result);
}

int add_op(int a, int b)
{
    return a + b;
}

int sub_op(int a, int b)
{
    return a - b;
}

void add_sub(char *handler, Variable *variable1, Variable *variable2, int (*op)(int a, int b))
{
    linkedList *variable1_lst;
    size_t variable1_index = 0;
    linkedList *variable2_lst;
    size_t variable2_index = 0;
    size_t result_size = 0;

    if (variable1->type == VECTOR_CHAIN)
    {
        variable1_lst = variable1->result.pos_vec_chain;
        result_size = variable1->vec_chain_size;
    }
    else
    {
        result_size = variable1->result.values.size;
    }

    if (variable2->type == VECTOR_CHAIN)
    {
        variable2_lst = variable2->result.pos_vec_chain;
        if (result_size == 0)
        {
            result_size = variable2->vec_chain_size;
        }
        assert(result_size == variable2->vec_chain_size);
    }
    else
    {
        result_size = variable2->result.values.size;
    }

    int *result = malloc(sizeof(int) * variable1->result.values.size);
    for (size_t i = 0; i < variable1->result.values.size; i++)
    {
        int valu1, value2;
        if (variable1->type == VECTOR_CHAIN)
        {
            if (variable1_index >= ((pos_vec *)variable1_lst->data)->size)
            {
                variable1_lst = variable1_lst->next;
                variable1_index = 0;
            }
            valu1 = ((pos_vec *)variable1_lst->data)->values[variable1_index];
            variable1_index++;
        }
        else
        {
            valu1 = variable1->result.values.values[i];
        }

        if (variable2->type == VECTOR_CHAIN)
        {
            if (variable2_index >= ((pos_vec *)variable2_lst->data)->size)
            {
                variable2_lst = variable2_lst->next;
                variable2_index = 0;
            }
            value2 = ((pos_vec *)variable2_lst->data)->values[variable2_index];
            variable2_index++;
        }
        else
        {
            value2 = variable2->result.values.values[i];
        }

        result[i] = op(valu1, value2);
    }

    Variable *fin_result = malloc(sizeof(Variable));
    *fin_result = (Variable){
        .type = VALUE_VECTOR,
        .result.values.values = result,
        .result.values.size = variable1->result.values.size,
        .name = strdup(handler),
        .exists = true};

    add_var(fin_result);
}

void add(char *handler, Variable *variable1, Variable *variable2)
{
    add_sub(handler, variable1, variable2, add_op);
}

void sub(char *handler, Variable *variable1, Variable *variable2)
{

    add_sub(handler, variable1, variable2, sub_op);
}

bool min_op(int a, int b)
{
    return a < b;
}

bool max_op(int a, int b)
{
    return a > b;
}

void MinMax(MinMaxOperator minmax_operator, Status *status)
{
    status->code = OK;
    char *handler = minmax_operator.handler;
    Variable *variable = minmax_operator.variable;
    bool (*op)(int a, int b) = (minmax_operator.operation == MIN) ? min_op : max_op;

    int result[2];
    result[0] = variable->result.values.values[0];
    int index = 1;

    size_t size = 0;
    size_t chain_index = 0;
    linkedList *lst;
    if (variable->type == VECTOR_CHAIN)
    {
        size = variable->vec_chain_size;
        lst = variable->result.pos_vec_chain;
    }
    else
    {
        size = variable->result.values.size;
    }

    for (size_t i = 1; i < size; i++)
    {
        int value;
        if (variable->type == VECTOR_CHAIN)
        {
            if (chain_index >= ((pos_vec *)lst->data)->size)
            {
                lst = lst->next;
                chain_index = 0;
            }
            value = ((pos_vec *)lst->data)->values[chain_index];
            chain_index++;
        }
        else
        {
            value = variable->result.values.values[i];
        }
        result[index] = value;
        index -= op(value, result[0]);
        result[index] = value;
        index = 1;
    }

    Variable *fin_result = malloc(sizeof(Variable));
    *fin_result = (Variable){
        .type = INT_VALUE,
        .result.ivalue = result[0],
        .name = strdup(handler),
        .exists = true};

    add_var(fin_result);
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

void free_db_operator(DbOperator *query)
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

            char *recv_buffer = malloc(sizeof(char) * (recv_message.length + 1));
            length = recv(client_socket, recv_buffer, recv_message.length, 0);

            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            //    Query string is converted into a request for an database operator
            DbOperator *query = NULL;
            if (bload.mode)
            {
                String load_str = {
                    .str = recv_message.payload,
                    .len = recv_message.length};
                query = parse_load_parallel(load_str, &s_message);
            }
            else
            {
                query = parse_command(recv_message.payload, &s_message, client_socket, client_context);
            }

            // 2. Handle request
            //    Corresponding database operator is executed over the query
            String result;

            if (batch.mode && query)
            {
                assert(query);
                if (query->type == BATCH_EXECUTE)
                {
                    result = execute_DbOperator(query);
                    batch.mode = false;
                    // free(query);
                }
                else
                {
                    if (query->type != BATCH_QUERY)
                    {
                        // currenlty only select and fetch are supported in batch queries
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

            if (!batch.mode)
            {
                if (batch.num_queries > 0)
                {
                    for (size_t i = 0; i < batch.num_queries; i++)
                    {
                        free_db_operator(batch.queries[i]);
                    }
                    batch.num_queries = 0;
                    free_db_operator(query);
                }
                else
                {
                    free_db_operator(query);
                }
            }

            if (!bload.mode)
            {
                free(recv_buffer);
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
    // init_buffer_pool();
    // init_lock_table();
    // init_transaction_table();
    // init_query_cache();
    // init_bload();
    // init_batch();
    // init_stats();
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

    // Setup the database
    init_db();

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

void shutdown_server(DbOperator *dbo)
{

    // cache everything to file
    flush_db(current_db);

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
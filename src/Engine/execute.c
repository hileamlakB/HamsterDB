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

String empty_string = {
    .str = "",
    .len = 0};
String failed_string = {
    .str = "Failed",
    .len = 6};

batch_query batch = {
    .mode = false,
    .num_queries = 0

};

typedef struct execute_args
{
    node *query_group;
    atomic_bool *is_done;
} execute_args;

void *execute_query(void *arg)
{
    execute_args *exec_args = (execute_args *)arg;
    node *query_group = exec_args->query_group;
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
    atomic_bool is_done[depth];

    //  here if the number of quries is less than one you won't need to create threads
    // you can use this thread itself for one query
    for (size_t i = 0; i < depth; i++)
    {
        DbOperator *dbs = (DbOperator *)query_group->val;

        is_done[i] = false;
        args[i] = (select_args){
            .low = dbs->operator_fields.select_operator.low,
            .high = dbs->operator_fields.select_operator.high,
            .read_size = dbs->operator_fields.select_operator.table->rows,
            .handle = dbs->operator_fields.select_operator.handler,
            .tbl = table,
            .col = column,
            .is_done = &is_done[i]};
        // handle the case where there are other commands besides select_col
        pthread_create(&threads[i], NULL, select_col, &args[i]);
        pthread_detach(threads[i]);
        query_group = query_group->next;
    }

    // wait or all the threads to finish
    for (size_t i = 0; i < depth; i++)
    {
        while (!is_done[i])
        {
            sched_yield();
        }
    }

    *exec_args->is_done = true;
    return NULL;
}

String batch_execute2(DbOperator **queries, size_t n)
{
    Table *tbl = queries[0]->operator_fields.select_operator.table;
    Column *col = queries[0]->operator_fields.select_operator.column;

    map_col(tbl, col, 0);

    int **lows = malloc(sizeof(int *) * n);
    int **highs = malloc(sizeof(int *) * n);
    char **handles = malloc(sizeof(char *) * n);
    for (size_t i = 0; i < n; i++)
    {
        lows[i] = queries[i]->operator_fields.select_operator.low;
        highs[i] = queries[i]->operator_fields.select_operator.high;
        handles[i] = queries[i]->operator_fields.select_operator.handler;
    }

    batch_select_args common = {
        .file = col->data,
        .n = n,
        .low = lows,
        .high = highs,
        .handle = handles,
        .read_size = tbl->rows,
        .offset = 0,
    };
    shared_scan(common);

    free(lows);
    free(highs);
    free(handles);
    return empty_string;
}

String batch_execute(DbOperator **queries, size_t n)
{

    if (n == 0)
    {
        return empty_string;
    }

    grouped_tasks gtasks = query_planner(queries, n);

    hashtable *independent = gtasks.independent;
    if (independent->count == 0)
    {
        return failed_string;
    }

    pthread_t threads[independent->size];
    size_t threads_created = 0;
    execute_args args[independent->size];

    atomic_bool is_done[independent->size];
    for (size_t i = 0; i < independent->size; i++)
    {
        if (independent->array[i])
        {
            // thread allocation becomes overwhalming when number of rows is approximatly > 10^6
            // so we can detach threads and let them run in the background
            is_done[threads_created] = false;
            args[threads_created] = (execute_args){
                .query_group = independent->array[i],
                .is_done = &is_done[threads_created]};

            pthread_create(&threads[threads_created], NULL, execute_query, &args[threads_created]);
            pthread_detach(threads[threads_created]);
            threads_created += 1;
        }
    }

    // wait for all the created threads
    for (size_t i = 0; i < threads_created; i++)
    {
        while (!is_done[i])
        {
            sched_yield();
        }
    }

    // free the hashtable
    free_grouped_tasks(gtasks);

    return empty_string;
}

String execute_DbOperator(DbOperator *query)
{

    if (!query)
    {
        return empty_string;
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
            atomic_bool is_done = false;
            select_args args = {
                .tbl = query->operator_fields.select_operator.table,
                .col = query->operator_fields.select_operator.column,
                .handle = query->operator_fields.select_operator.handler,
                .low = query->operator_fields.select_operator.low,
                .high = query->operator_fields.select_operator.high,
                .is_done = &is_done};
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

        String result = batch_execute2(batch.queries, batch.num_queries);
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
        batch_load(query);
        return empty_string;
    }
    else if (query && query->type == SHUTDOWN)
    {

        shutdown_server(query);
        return empty_string;
    }
    else if (query && query->type == JOIN)
    {
        join(query);
    }

    return empty_string;
}

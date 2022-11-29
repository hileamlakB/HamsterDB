#define _DEFAULT_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>
#include <sys/mman.h>
#include <errno.h>

#include "cs165_api.h"
#include "unistd.h"

#define initial_size 1024
// #define PAGE_SIZE ((size_t)sysconf(_SC_PAGESIZE))
// #define NUM_COMMA_LEN (MAX_INT_LENGTH + 1)
// #define NUM_TUPLE_LEN (2 * (MAX_INT_LENGTH + 1))

Variable btree_select(select_args args)
{
    return sorted_select(args);
}

Variable sorted_select(select_args args)
{
    char *starting = args.file;
    char *ending = args.file + args.tbl->rows * (MAX_INT_LENGTH + 1);

    if (*args.low)
    {
        starting = closest_search(
            args.low, args.file, args.tbl->rows,
            sizeof(char[MAX_INT_LENGTH + 1]), compare_sints);

        // make sure this is the first occurance of the value
        while (starting != args.file && compare_sints(starting, starting - sizeof(char[MAX_INT_LENGTH + 1])) == 0)
        {
            starting -= sizeof(char[MAX_INT_LENGTH + 1]);
        }
    }

    if (*args.high)
    {

        ending = closest_search(args.high, args.file, args.tbl->rows, sizeof(char[MAX_INT_LENGTH + 1]), compare_sints);
        // make sure this is the last occurance of the value
        while (ending != args.file + args.tbl->rows * (MAX_INT_LENGTH + 1) && compare_sints(ending, ending + sizeof(char[MAX_INT_LENGTH + 1])) == 0)
        {
            ending += sizeof(char[MAX_INT_LENGTH + 1]);
        }
    }

    // return a pos_vec with the positions of the values in the range
    Variable res = (Variable){
        .type = RANGE, .name = strdup(args.handle), .exists = true};
    res.result.range[0] = starting - args.file;
    res.result.range[1] = ending - args.file;
    return res;
}

// choose search algorithms between
// using sorted search, btree or normal search
Variable (*choose_algorithm(select_args args))(select_args)
{

    // pointer to a funtion that returns a pos_vec and takes an int
    if (args.col->indexed)
    {
        ColumnIndex idx = args.col->index;
        if (idx.type == SORTED)
        {
            return sorted_select;
        }
        else if (idx.type == BTREE)
        {
            // in this case you might want to choose using sorted_search even
            // if the option exists.
            return btree_select;
        }
    }

    return generic_select;
}

void *select_section(void *arg)
{
    select_args *args = (select_args *)arg;
    int *result_p = malloc(sizeof(int) * initial_size);
    size_t rcapacity = initial_size;

    int *low = args->low;
    int *high = args->high;
    size_t read_size = args->read_size;
    char *file = args->file;

    size_t offset = args->offset;

    size_t index = 0;
    size_t result_size = 0;

    while (file[index] != '\0' && index < read_size)
    {
        int num = zerounpadd(file + index, ',');

        result_p[result_size] = offset + index / (MAX_INT_LENGTH + 1);
        result_size += ((!low || num >= *low) && (!high || num < *high));

        // expand result if needed
        if (result_size + 1 >= rcapacity)
        {
            rcapacity *= 2;
            int *new_result = realloc(result_p, rcapacity * sizeof(int));
            if (!new_result)
            {
                free(result_p);
                log_err("Error allocating memory for result");

                return NULL;
            }
            result_p = new_result;
        }

        // including separating comma
        index += MAX_INT_LENGTH + 1;
    }

    if (result_size == 0)
    {
        free(result_p);
        return NULL;
    }

    int *new_result = realloc(result_p, result_size * sizeof(int));
    if (new_result)
    {
        result_p = new_result;
    }

    args->result = result_p;
    args->result_size = result_size;

    return NULL;
}

// returns position vector for all the elements that
// satisfy the condition
// right now it returns all at onces, but it should
// return in a batche dmanner
Variable generic_select(select_args args)
{

    // makes sure you cut a file into sections
    // at meaningfull locaitons
    const size_t PAGE_SIZE = 1 * ((size_t)sysconf(_SC_PAGESIZE) / (MAX_INT_LENGTH + 1)) * (MAX_INT_LENGTH + 1);
    // Variable *final_res = malloc(sizeof(Variable));

    // if file size is bigger than page size,
    // create multiple threads and later merge the result instead of
    // running it in one thread
    if (args.read_size <= PAGE_SIZE ||
        (args.low && args.high && ((size_t)*args.high - *args.low < PAGE_SIZE)))
    {

        select_section(&args);
        return (Variable){
            .type = POSITION_VECTOR,
            .name = strdup(args.handle),
            .result.values.values = args.result,
            .result.values.size = args.result_size,
            .exists = true};

        // return final_res;
    }
    else // (read_size > PAGE_SIZE )
    {
        size_t num_threads = args.read_size / PAGE_SIZE;
        if (args.read_size % PAGE_SIZE)
        {
            num_threads++;
        }
        pthread_t threads[num_threads];
        select_args targs[num_threads];

        for (size_t i = 0; i < num_threads; i++)
        {
            targs[i] = (select_args){
                .low = args.low,
                .high = args.high,
                .col = args.col,
                .tbl = args.tbl,
                .file = args.file + i * PAGE_SIZE,
                .read_size = PAGE_SIZE,
                .offset = (i * PAGE_SIZE) / (MAX_INT_LENGTH + 1)};

            pthread_create(&threads[i], NULL, select_section, &targs[i]);
        }

        linkedList head_chain = {
            .next = NULL};
        linkedList *end_chain = &head_chain;

        size_t total_size = 0;

        // wait for all threads to finish
        // and join answers
        for (size_t i = 0; i < num_threads; i++)
        {
            pthread_join(threads[i], NULL);
            if (targs[i].result_size)
            {
                linkedList *new_node = malloc(sizeof(linkedList));
                new_node->next = NULL;

                pos_vec *new_vec = malloc(sizeof(pos_vec));
                new_vec->values = targs[i].result;
                new_vec->size = targs[i].result_size;
                new_node->data = new_vec;
                end_chain->next = new_node;
                end_chain = new_node;
                total_size += targs[i].result_size;
            }
        }

        // merge results
        // V2.0
        // instead of merging them put them in a linked list
        // Variable *final_result = malloc(sizeof(Variable));
        return (Variable){
            .type = VECTOR_CHAIN,
            .name = strdup(args.handle),
            .result.pos_vec_chain = head_chain.next,
            .vec_chain_size = total_size,
            .exists = true};
        // return final_result;
    }
}

void *select_col(void *arg)
{
    select_args *args = (select_args *)arg;
    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);

    Status status;
    create_colf(args->tbl, args->col, &status);

    // mmap file for read
    if (!args->col->read_map)
    {
        char *buffer = mmap(NULL,
                            args->tbl->rows * (MAX_INT_LENGTH + 1),
                            PROT_READ, MAP_SHARED,
                            args->col->fd, args->col->meta_data_size * PAGE_SIZE);

        args->col->read_map = buffer;
        args->col->read_map_size = args->tbl->rows * (MAX_INT_LENGTH + 1);
    }
    args->file = args->col->read_map;
    args->read_size = args->col->read_map_size;

    Variable (*search_algorithm)(select_args) = choose_algorithm(*args);
    Variable *fin_result = malloc(sizeof(Variable));

    *fin_result = search_algorithm(*args);
    add_var(fin_result);
    return NULL;
}

void select_pos(Variable *posVec, Variable *valVec, char *handle, int *low, int *high, Status *status)
{

    if (!posVec->exists || !valVec->exists)
    {
        log_err("Error: Invalid variable");
        status->code = ERROR;
        return;
    }

    status->code = OK;

    size_t positions = 0;
    linkedList *pos_chain;
    linkedList *val_chain;
    size_t pos_index = 0;
    size_t val_index = 0;
    if (posVec->type == VECTOR_CHAIN)
    {
        positions = posVec->vec_chain_size;
        pos_chain = posVec->result.pos_vec_chain;
    }
    else
    {
        positions = posVec->result.values.size;
    }

    if (valVec->type == VECTOR_CHAIN)
    {
        val_chain = valVec->result.pos_vec_chain;
    }

    int *result = calloc(positions, sizeof(int));
    size_t result_size = 0;
    for (size_t i = 0; i < positions; i++)
    {
        int pos, value;
        if (posVec->type == VECTOR_CHAIN)
        {
            if (pos_index >= ((pos_vec *)pos_chain->data)->size)
            {
                pos_chain = pos_chain->next;
                pos_index = 0;
            }
            pos = ((pos_vec *)pos_chain->data)->values[pos_index];
            pos_index++;
        }
        else
        {
            pos = posVec->result.values.values[i];
        }

        if (valVec->type == VECTOR_CHAIN)
        {
            if (val_index >= ((pos_vec *)val_chain->data)->size)
            {
                val_chain = val_chain->next;
                val_index = 0;
            }
            value = ((pos_vec *)val_chain->data)->values[val_index];
            val_index++;
        }
        else
        {
            value = valVec->result.values.values[i];
        }

        result[result_size] = pos;
        result_size += ((!low || value >= *low) &&
                        (!high || value < *high));
    }

    Variable *fin_result = malloc(sizeof(Variable));
    *fin_result = (Variable){
        .type = POSITION_VECTOR,
        .name = strdup(handle),
        .result.values.values = result,
        .result.values.size = result_size,
        .exists = true};

    add_var(fin_result);
}

int generic_fetch(int *result, Column *column, pos_vec vec)
{

    int result_size = 0;

    int position = 0;
    size_t result_p = 0;

    size_t index = 0;
    while (column->read_map[index] != '\0' && result_p < vec.size)
    {

        if (position == vec.values[result_p])
        {
            result[result_size++] = zerounpadd(column->read_map + index, ',');
            result_p++;
        }

        position++;
        // including separating comma
        index += MAX_INT_LENGTH + 1;
    }
    return result_size;
}

void fetch_from_chain(Table *table, Column *column, Variable *var, char *var_name)
{
    (void)table;

    size_t max_size_result = 0;
    for (linkedList *chain = var->result.pos_vec_chain; chain; chain = chain->next)
    {
        max_size_result += ((pos_vec *)chain->data)->size;
    }

    int *result = calloc(max_size_result, sizeof(int));
    int result_size = 0;
    // iterate through the chain and execute gneric fetch on each
    for (linkedList *chain = var->result.pos_vec_chain; chain; chain = chain->next)
    {
        result_size += generic_fetch(result + result_size, column, *((pos_vec *)chain->data));
    }

    // resize result
    int *new_result = realloc(result, result_size * sizeof(int));
    if (new_result)
    {
        result = new_result;
    }

    Variable *fin_result = malloc(sizeof(Variable));
    *fin_result = (Variable){
        .type = VALUE_VECTOR,
        .name = strdup(var_name),
        .result.values.values = result,
        .result.values.size = result_size,
        .exists = true};
    add_var(fin_result);
}

void fetch_col(Table *table, Column *column, Variable *var, char *var_name, Status *status)
{
    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);

    if (!var->exists)
    {
        log_err("Error: Invalid variable");
        status->code = ERROR;
        return;
    }

    create_colf(table, column, status);

    struct stat sb;
    fstat(column->fd, &sb);

    // here instead use mmap to create an area and use that to write to file
    // which you can delete at the end
    // mmap file for read
    if (!column->read_map)
    {
        char *buffer = mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, column->fd, column->meta_data_size * PAGE_SIZE);
        if (buffer == MAP_FAILED)
        {
            log_err("Error mapping file for column read");
            return;
        }
        column->read_map = buffer;
        column->read_map_size = sb.st_size;
    }

    if (var->type == VECTOR_CHAIN)
    {
        fetch_from_chain(table, column, var, var_name);
    }
    else
    {
        int *result = calloc(var->result.values.size, sizeof(int));
        int result_size = generic_fetch(result, column, var->result.values);

        Variable *fin_result = malloc(sizeof(Variable));

        *fin_result = (Variable){
            .type = VALUE_VECTOR,
            .name = strdup(var_name),
            .result.values.values = result,
            .result.values.size = result_size,
            .exists = true};
        add_var(fin_result);
    }
    status->code = OK;
}
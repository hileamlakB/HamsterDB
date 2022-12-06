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
#include <unistd.h>
#include <Serializer/serialize.h>

Variable btree_select(select_args args)
{
    return sorted_select(args);
}

Variable sorted_select(select_args args)
{

    if (!args.col->index.read_map)
    {
        char *filename = catnstr(2, args.col->file_path, ".sorted");
        int fd = open(filename, O_RDONLY);
        args.col->index.read_map = mmap(NULL, args.tbl->rows * (MAX_INT_LENGTH + 1), PROT_READ, MAP_PRIVATE, fd, 0);
        free(filename);
    }

    char *sorted_file = args.col->index.read_map;

    // map file

    char *starting = sorted_file;
    char *ending = sorted_file + args.tbl->rows * (MAX_INT_LENGTH + 1);

    if (*args.low)
    {
        char *key_str = malloc(MAX_INT_LENGTH + 1);
        sprintf(key_str, "%012d", *args.low);
        starting = closest_search(
            key_str, sorted_file, args.tbl->rows,
            sizeof(char[MAX_INT_LENGTH + 1]), compare_sints);

        // make sure this is the first occurance of the value
        while (starting > sorted_file && compare_sints(key_str, starting - sizeof(char[MAX_INT_LENGTH + 1])) == 0)
        {
            starting -= sizeof(char[MAX_INT_LENGTH + 1]);
        }

        // if the value isn't in the file, find the insertion point
        while (compare_sints(starting, key_str) < 0)
        {
            starting += sizeof(char[MAX_INT_LENGTH + 1]);
        }
        free(key_str);
    }

    if (*args.high)
    {
        char *key_str = malloc(MAX_INT_LENGTH + 1);
        sprintf(key_str, "%012d", *args.high);
        ending = closest_search(
            key_str,
            sorted_file,
            args.tbl->rows, sizeof(char[MAX_INT_LENGTH + 1]), compare_sints);
        // make sure this isn't included  the last occurance of the value
        while (ending != sorted_file && compare_sints(key_str, ending - sizeof(char[MAX_INT_LENGTH + 1])) == 0)
        {
            ending -= sizeof(char[MAX_INT_LENGTH + 1]);
        }

        char *max_end = sorted_file + (args.tbl->rows) * (MAX_INT_LENGTH + 1);
        // if the value isn't in the file, find the insertion point
        while (compare_sints(ending, key_str) < 0 && ending < max_end)
        {
            ending += sizeof(char[MAX_INT_LENGTH + 1]);
        }

        free(key_str);
    }

    // return a pos_vec with the positions of the values in the range
    Variable res = (Variable){
        .type = RANGE,
        .name = strdup(args.handle),
        .exists = true,
        .is_sorted = true,
        .is_clustered = (args.col->index.clustered == CLUSTERED)};
    strcpy(res.sorting_column, args.col->name);
    strcpy(res.sorting_column_path, args.col->file_path);

    res.result.range[0] = (starting - sorted_file) / (MAX_INT_LENGTH + 1);
    res.result.range[1] = (ending - sorted_file) / (MAX_INT_LENGTH + 1);
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
    int *file = args->file;

    size_t offset = args->offset;

    size_t index = 0;
    size_t result_size = 0;

    while (index < read_size)
    {

        result_p[result_size] = offset + index;
        result_size += ((!low || file[index] >= *low) && (!high || file[index] < *high));

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
        index += 1;
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
    const size_t PAGE_SIZE = 4 * (size_t)sysconf(_SC_PAGESIZE); // experimental value
    // Variable *final_res = malloc(sizeof(Variable));

    // if file size is bigger than page size,
    // create multiple threads and later merge the result instead of
    // running it in one thread
    if (args.read_size * sizeof(int) <= PAGE_SIZE)
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
        size_t total_pages = args.read_size * sizeof(int);

        size_t num_threads = total_pages / PAGE_SIZE;
        if (args.read_size % PAGE_SIZE)
        {
            num_threads++;
        }
        pthread_t threads[num_threads];
        select_args targs[num_threads];

        for (size_t i = 0; i < num_threads; i++)
        {
            size_t read_size = min(args.tbl->rows * sizeof(int) - (i - 1) * PAGE_SIZE, PAGE_SIZE);

            targs[i] = (select_args){
                .low = args.low,
                .high = args.high,
                .col = args.col,
                .tbl = args.tbl,
                .file = args.file + (i * PAGE_SIZE) / sizeof(int),
                .read_size = read_size / sizeof(int),
                .offset = (i * PAGE_SIZE) / (sizeof(int))};

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

    map_col(args->tbl, args->col, 0);

    // mmap file for read

    args->file = args->col->data;
    args->read_size = args->tbl->rows;

    Variable (*search_algorithm)(select_args) = choose_algorithm(*args);
    Variable *fin_result = malloc(sizeof(Variable));

    *fin_result = search_algorithm(*args);
    add_var(fin_result);
    return NULL;
}

void select_pos_range(Variable *posVec, Variable *valVec, char *handle, int *low, int *high)
{
    int position = posVec->result.range[0];

    int *result = calloc(
        posVec->result.range[1] - posVec->result.range[0],
        sizeof(int));
    size_t result_size = 0;

    size_t i = 0;
    while (i < valVec->result.values.size)
    {
        int value = valVec->result.values.values[i];

        result[result_size] = position;
        result_size += ((!low || value >= *low) &&
                        (!high || value < *high));
        position += 1;
        i++;
    }

    Variable *fin_result = malloc(sizeof(Variable));
    *fin_result = (Variable){
        .type = POSITION_VECTOR,
        .name = strdup(handle),
        .result.values.values = result,
        .result.values.size = result_size,
        .is_sorted = posVec->is_sorted,
        .is_clustered = posVec->is_clustered,
        .exists = true};
    strcpy(fin_result->sorting_column, posVec->sorting_column);
    strcpy(fin_result->sorting_column_path, posVec->sorting_column_path);

    add_var(fin_result);
}

void select_pos(Variable *posVec, Variable *valVec, char *handle, int *low, int *high, Status *status)
{

    assert(posVec->exists && valVec->exists);

    if (posVec->type == RANGE)
    {
        select_pos_range(posVec, valVec, handle, low, high);
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
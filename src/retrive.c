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

void *select_section(void *arg)
{
    thread_select_args *args = (thread_select_args *)arg;
    int **result = args->result;
    int *low = args->low;
    int *high = args->high;
    size_t read_size = args->read_size;
    size_t rcapacity = args->result_capacity;
    char *file = args->file;

    size_t offset = args->offset;

    size_t index = 0;
    size_t result_size = 0;

    int *result_p = *result;
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

    int *new_result = realloc(result_p, result_size * sizeof(int));
    if (new_result)
    {
        result_p = new_result;
    }

    *result = result_p;
    args->result_size = result_size;

    return NULL;
}

// returns position vector for all the elements that
// satisfy the condition
// right now it returns all at onces, but it should
// return in a batche dmanner

pos_vec generic_select(int *low, int *high, char *file, int **result, size_t result_capacity, Status *status, size_t read_size)
{
    (void)status;

    // make sure you cut a file into sections
    // at meaningfull locaitons
    const size_t PAGE_SIZE = 1 * ((size_t)sysconf(_SC_PAGESIZE) / (MAX_INT_LENGTH + 1)) * (MAX_INT_LENGTH + 1);

    // if file size is bigger than page size,
    // create multiple threads and later merge the result instead of
    // running it here
    if (read_size <= PAGE_SIZE || (low && high && ((size_t)*high - *low < PAGE_SIZE)))
    {

        thread_select_args args = (thread_select_args){
            .low = low,
            .high = high,
            .file = file,
            .result = result,
            .read_size = read_size,
            .result_capacity = result_capacity};
        select_section(&args);
        return (pos_vec){.values = *result, .size = args.result_size, .ivalue = 0, .fvalue = 0.0};
    }
    else // (read_size > PAGE_SIZE )
    {
        size_t num_threads = read_size / PAGE_SIZE;
        if (read_size % PAGE_SIZE)
        {
            num_threads++;
        }
        pthread_t threads[num_threads];
        thread_select_args args[num_threads];
        int *results[num_threads];

        for (size_t i = 1; i < num_threads; i++)
        {
            results[i] = malloc(100 * sizeof(int));
            args[i] = (thread_select_args){
                .low = low,
                .high = high,
                .file = file + i * PAGE_SIZE,
                .result = &results[i],
                .read_size = PAGE_SIZE,
                .result_capacity = 100,
                .offset = (i * PAGE_SIZE) / (MAX_INT_LENGTH + 1)};

            pthread_create(&threads[i], NULL, select_section, &args[i]);
        }
        // how should I join answers here

        // read the first section
        results[0] = malloc(100 * sizeof(int));
        args[0] = (thread_select_args){
            .low = low,
            .high = high,
            .file = file,
            .result = &results[0],
            .read_size = PAGE_SIZE,
            .result_capacity = 100,
            .offset = 0};
        select_section(&args[0]);

        // wait for all threads to finish
        // and join answers
        for (size_t i = 1; i < num_threads; i++)
        {
            pthread_join(threads[i], NULL);
        }

        // merge results
        size_t result_size = args[0].result_size;
        *result = realloc(*result, result_size * sizeof(int));
        memcpy(*result, results[0], result_size * sizeof(int));
        for (size_t i = 1; i < num_threads; i++)
        {
            result_size += args[i].result_size;
            *result = realloc(*result, (result_size) * sizeof(int));
            memcpy(*result + result_size - args[i].result_size, results[i], args[i].result_size * sizeof(int));
        }
        return (pos_vec){.values = *result, .size = result_size, .ivalue = 0, .fvalue = 0.0};
    }
}

void *thread_select_col(void *args)

{
    thread_select_args *targs = (thread_select_args *)args;
    Status status = {.code = OK};

    int *results = malloc(100 * sizeof(int));
    pos_vec pos = generic_select(targs->low,
                                 targs->high, targs->file,
                                 &results, 100,
                                 &status, targs->read_size);
    if (status.code == ERROR)
    {
        log_err("Error in select");
        return NULL;
    }

    add_var(targs->handle, pos, POSITION_VECTOR);
    return args;
}

void select_col(Table *table, Column *column, char *var_name, int *low, int *high, Status *status)
{
    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);

    create_colf(table, column, status);
    if (status->code != OK)
    {
        log_err("-- Error opening file for column write");
        return;
    }

    struct stat sb;
    fstat(column->fd, &sb);

    int result_capacity = (PAGE_SIZE / sizeof(int));
    int *result = malloc(result_capacity * sizeof(int));

    // mmap file for read
    if (!column->read_map)
    {
        char *buffer = mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, column->fd, column->meta_data_size * PAGE_SIZE);
        if (buffer == MAP_FAILED)
        {
            log_err("-- Error mapping file for column read");
            return;
        }
        column->read_map = buffer;
    }

    pos_vec fin_result = generic_select(low, high,
                                        column->read_map,
                                        &result,
                                        result_capacity,
                                        status,
                                        table->rows * (MAX_INT_LENGTH + 1));

    // free select operators
    if (low)
    {
        free(low);
    }
    if (high)
    {
        free(high);
    }

    // resize result vector to minimize memory usages
    if (status->code == OK)
    {
        add_var(var_name, fin_result, POSITION_VECTOR);
    }

    // munmap(buffer, sb.st_size);
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

    int *result = calloc(posVec->result.size, sizeof(int));
    int result_size = 0;
    for (int i = 0; i < posVec->result.size; i++)
    {
        result[result_size] = posVec->result.values[i];
        result_size += ((!low || valVec->result.values[i] >= *low) && (!high || valVec->result.values[i] < *high));
    }

    if (low)
    {
        free(low);
    }
    if (high)
    {
        free(high);
    }

    add_var(handle, (pos_vec){.values = result, .size = result_size, .ivalue = 0, .fvalue = 0.0}, POSITION_VECTOR);
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

    //  chekc if the file is already mapped
    // make sure the map is at the start of the file if not create a new map
    // may be have a reading map and a writing map separately
    // to start with just do a simple read

    create_colf(table, column, status);

    if (column->fd == 0)
    {
        // do something about file fialing to write
        log_err("Error opening file for column write");
        return;
    }
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
    }

    int *result = calloc(var->result.size, sizeof(int));
    int result_size = 0;

    int position = 0;
    int result_p = 0;

    int index = 0;
    while (column->read_map[index] != '\0' && result_p < var->result.size)
    {

        if (position == var->result.values[result_p])
        {
            result[result_size++] = zerounpadd(column->read_map + index, ',');
            result_p++;
        }

        position++;
        // including separating comma
        index += MAX_INT_LENGTH + 1;
    }

    int *new_result = realloc(result, result_size * sizeof(int));
    if (new_result)
    {
        result = new_result;
    }
    add_var(var_name, (pos_vec){.size = result_size, .values = result, .ivalue = 0, .fvalue = 0.0},
            POSITION_VECTOR);
}
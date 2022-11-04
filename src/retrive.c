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

// returns position vector for all the elements that
// satisfy the condition
// right now it returns all at onces, but it should
// return in a batche dmanner
void select_col(Table *table, Column *column, char *var_name, int *low, int *high, Status *status)
{
    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);

    create_colf(table, column, status);
    if (status->code != OK)
    {
        log_err("--Error opening file for column write");
        return;
    }

    struct stat sb;
    fstat(column->fd, &sb);

    int result_capacity = (PAGE_SIZE / sizeof(int));
    int *result = malloc(result_capacity * sizeof(int));
    int result_size = 0;

    // mmap file for read
    char *buffer = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, column->fd, column->meta_data_size * PAGE_SIZE);
    if (buffer == MAP_FAILED)
    {
        log_err("--Error mapping file for column read");
        return;
    }

    int index = 0;
    while (buffer[index] != '\0')
    {
        int num = zerounpadd(buffer + index, ',');

        result[result_size] = index / (MAX_INT_LENGTH + 1);
        result_size += ((!low || num >= *low) && (!high || num < *high));

        // expand result if needed
        if (result_size + 1 == result_capacity)
        {
            result_capacity *= 2;
            int *new_result = realloc(result, result_capacity * sizeof(int));
            if (!new_result)
            {
                free(result);
                log_err("Error allocating memory for result");
                status->code = ERROR;
                return;
            }
            result = new_result;
        }

        // including separating comma
        index += MAX_INT_LENGTH + 1;
    }

    // free select operators
    if (low)
    {
        free(low);
    }
    if (high)
    {
        free(high);
    }

    // resize result vector to minimize memory usage
    int *new_result = realloc(result, result_size * sizeof(int));
    if (new_result)
    {
        result = new_result;
    }
    add_var(var_name, (pos_vec){.values = result, .size = result_size, .ivalue = 0, .fvalue = 0.0}, POSITION_VECTOR);
    munmap(buffer, sb.st_size);
}

void select_pos(Variable *posVec, Variable *valVec, char *handle, int *low, int *high, Status *status)
{

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
    char *buffer = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, column->fd, column->meta_data_size * PAGE_SIZE);

    int *result = calloc(var->result.size, sizeof(int));
    int result_size = 0;

    int position = 0;
    int result_p = 0;

    int index = 0;
    while (buffer[index] != '\0' && result_p < var->result.size)
    {

        if (position == var->result.values[result_p])
        {
            result[result_size++] = zerounpadd(buffer + index, ',');
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
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
void select_col(Column *column, char *var_name, int *low, int *high)
{
    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);

    //  chekc if the file is already mapped
    // make sure the map is at the start of the file if not create a new map
    // may be have a reading map and a writing map separately
    // to start with just do a simple read

    create_colf(column);

    if (column->fd == 0)
    {
        // do something about file fialing to write
        log_err("Error opening file for column write");
        return;
    }
    struct stat sb;
    fstat(column->fd, &sb);
    char buffer[sb.st_size];
    int *result = malloc(sizeof(int) * sb.st_size);
    int result_size = 0;
    lseek(column->fd, column->meta_data_size * PAGE_SIZE, SEEK_SET);
    read(column->fd, buffer, sb.st_size);

    int position = 0;
    int index = 0;
    while (buffer[index] != '\0')
    {
        int num = zerounpadd(buffer + index, ',');

        if ((!low || num >= *low) && (!high || num < *high))
        {
            result[result_size++] = position;
        }

        position++;
        // including separating comma
        index += MAX_INT_LENGTH + 1;
    }

    add_var(var_name, (pos_vec){.values = result, .size = result_size, .value = 0}, POSITION_VECTOR);
}

void fetch_col(Column *column, Variable *var, char *var_name)
{
    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);

    //  chekc if the file is already mapped
    // make sure the map is at the start of the file if not create a new map
    // may be have a reading map and a writing map separately
    // to start with just do a simple read

    create_colf(column);

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
    char buffer[sb.st_size];
    int *result = malloc(sizeof(int) * var->result.size);
    int result_size = 0;
    lseek(column->fd, column->meta_data_size * PAGE_SIZE, SEEK_SET);
    read(column->fd, buffer, sb.st_size);

    int position = 0;
    int result_p = 0;

    int index = 0;
    while (buffer[index] != '\0')
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

    add_var(var_name, (pos_vec){.size = result_size, .values = result, .value = 0},
            POSITION_VECTOR);
}
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

int generic_fetch(int *from, int *map, int *result, pos_vec vec)
{

    int result_size = 0;

    int position = 0;

    size_t index = 0;

    while (index < vec.size)
    {
        position = vec.values[index];
        int value;
        if (map)
        {
            int maped_index = map[position];
            value = from[maped_index];
        }
        else
        {
            value = from[position];
        }
        result[result_size++] = value;
        index++;
    }

    return result_size;
}

int fetch_from_range(int *from, int *map, int *result, int low, int high)
{
    int result_size = 0;
    int position = low;

    while (position < high)
    {
        int value;
        if (map)
        {
            int maped_index = map[position];
            value = from[maped_index];
        }
        else
        {
            value = from[position];
        }
        result[result_size++] = value;
        position += 1;
    }
    return result_size;
}

void fetch_from_chain(int *from, int *index_map, Variable *var, char *var_name)
{

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
        result_size += generic_fetch(from, index_map, result + result_size, *((pos_vec *)chain->data));
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

    assert(var->exists);

    map_col(table, column, 0);

    int *read_from = NULL, *index_map = NULL;

    if (var->is_sorted && var->is_clustered)
    {
        char *clusterd_sorted = catnstr(3, column->file_path, ".clustered.", var->sorting_column);
        int fd = open(clusterd_sorted, O_RDONLY);
        read_from = mmap(NULL, table->rows * sizeof(int), PROT_READ, MAP_PRIVATE, fd, 0);
    }
    else
    {
        map_col(table, column, 0);

        read_from = column->data;

        if (var->is_sorted && !var->is_clustered)
        {
            char *map_name = catnstr(2, var->sorting_column_path, ".map");
            int fd = open(map_name, O_RDONLY);
            index_map = mmap(NULL, table->rows * sizeof(int), PROT_READ, MAP_SHARED, fd, 0);
            free(map_name);
        }
    }

    if (var->type == VECTOR_CHAIN)
    {
        fetch_from_chain(read_from, index_map, var, var_name);
    }
    else if (var->type == POSITION_VECTOR)
    {
        int *result = calloc(var->result.values.size, sizeof(int));
        int result_size = generic_fetch(read_from, index_map, result, var->result.values);

        Variable *fin_result = malloc(sizeof(Variable));

        *fin_result = (Variable){
            .type = VALUE_VECTOR,
            .name = strdup(var_name),
            .result.values.values = result,
            .result.values.size = result_size,
            .exists = true};
        add_var(fin_result);
    }
    else if (var->type == RANGE)
    {

        int *result = calloc(var->result.range[1] - var->result.range[0], sizeof(int));
        int result_size = fetch_from_range(read_from, index_map, result, var->result.range[0], var->result.range[1]);

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

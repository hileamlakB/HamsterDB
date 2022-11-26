#ifndef __EXTERNAL_SORT_H__
#define __EXTERNAL_SORT_H__

#include <stdlib.h>
#include <stdbool.h>
#include <stdatomic.h>
#include "cs165_api.h"

#define MAX_RUNS 1000

typedef struct sort_args
{
    void *array;
    size_t size;
    int (*compare)(const void *, const void *);
    atomic_bool is_sorted;

} sort_args;

typedef struct merge_args
{
    char *array1;
    char *array2;
    size_t size1;
    size_t size2;
    atomic_bool *is_merged;

} merge_args;

extern size_t PAGE_SIZE;

int sort_col(Table *tbl, Column *col);
void propagate_sort(Table *tbl, Column *idx_column);

#endif // __EXTERNAL_SORT_H__
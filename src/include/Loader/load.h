#ifndef __LOAD_H__
#define __LOAD_H__

#include <data_structures.h>
#include <cs165_api.h>

typedef struct scheduled_write
{
    char *data;
    atomic_size_t ticket;

} scheduled_write;

void finish_load();
void batch_load();
void prepare_load(DbOperator *query);
void *preprocess_load(void *data);
int insert(Table *table, String values);
int insert_col(Table *table, Column *col, int value);

#endif // __LOAD_H__
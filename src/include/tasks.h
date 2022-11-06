#ifndef TAKS_H
#define TAKS_H

#include "thread.h"
#include "cs165_api.h"

typedef enum task_type
{
    GROUPED,
    SINGLE
} task_type;

typedef struct task
{
    task_type type;
    pthread_t *allocated_threads;
    int id;

    union
    {
        struct task *sub_tasks;
        DbOperator *db_op;
    } task;

    struct task *next;

} task;

typedef struct tasks
{
    task *task;
    size_t num_tasks;
} tasks;

typedef struct grouped_tasks
{
    hashtable *independent;
    tasks dependent;
} grouped_tasks;

grouped_tasks query_planner(DbOperator **db_ops, int num_ops, Status *status);

#endif
#ifndef THREAD_H
#define THREAD_H

#include <pthread.h>
#include <stdbool.h>
#include "data_structures.h"

#define NUM_THREADS 5

typedef Queue thread_group;

typedef enum thread_type
{
    MASTER,
    WORKER
} thread_type;

// variables that end with _s are shared and requires a mutex to
// access
typedef struct thread_pool
{
    bool setup; // is a thread pool setup

    int num_threads;
    int num_free_threads_s;

    // these should be a hash table
    Queue *free_threads_s;
    Queue *busy_threads_s;

    pthread_t threads[NUM_THREADS];
    pthread_mutex_t mutex;
    pthread_cond_t cond;

} thread_pool;

void create_threads();
thread_group *allocate_threads(size_t num_threads);
void create_job_board();
void idel_threads();

// job_board

typedef struct job_board
{
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    hashtable *jobs_s;
    bool setup;
} job_board;

extern thread_pool *main_pool;
extern job_board *main_board;

#endif
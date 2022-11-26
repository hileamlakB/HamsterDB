#ifndef __THREAD_POOL_H__
#define __THREAD_POOL_H__

#include <pthread.h>
#include <stdatomic.h>

#define NUM_THREADS 5

typedef struct job
{
    struct job *job_next;      /* linked list of jobs */
    void *(*job_func)(void *); /* function to call */
    void *job_arg;             /* its argument */
} job;

typedef struct pool_t
{
    pthread_mutex_t job_mutex; /* protects job list */
    pthread_cond_t job_cond;   /* signals arrival of new job */
    atomic_bool stop;
    atomic_size_t count; /* number of threads in the pool */
    pthread_t *threads;
    job *jobs; /* head of the job queue */
    job *last_job;

} pool_t;

extern pool_t thread_pool;

void thread_pool_init(int num_threads);
void *generic_task();
void destroy_thread_pool();
void add_job(void *(*job_func)(void *), void *job_arg);

#endif // __THREAD_POOL_H__
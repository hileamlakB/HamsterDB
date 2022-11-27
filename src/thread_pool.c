#include "thread_pool.h"
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

pool_t thread_pool;

void *generic_task()
{
    // condtionaly wait for to be awaken and when awaken
    // check if there is a job to do
    // if there is a job to do, do it
    // if there is no job to do, go back to sleep

    while (true)
    {

        pthread_mutex_lock(&thread_pool.job_mutex);
        while (thread_pool.jobs == NULL)
        {
            pthread_cond_wait(&thread_pool.job_cond, &thread_pool.job_mutex);
            if (thread_pool.stop)
            {

                pthread_mutex_unlock(&thread_pool.job_mutex);
                // thread_pool.count--;
                return NULL;
            }
        }
        job *current_job = thread_pool.jobs;
        thread_pool.jobs = thread_pool.jobs->job_next;
        if (thread_pool.jobs == NULL)
        {
            thread_pool.last_job = NULL;
        }
        pthread_mutex_unlock(&thread_pool.job_mutex);

        current_job->job_func(current_job->job_arg);
        free(current_job);
    }
}

void thread_pool_init(int num_threads)
{
    pthread_mutex_init(&thread_pool.job_mutex, NULL);
    pthread_cond_init(&thread_pool.job_cond, NULL);
    thread_pool.stop = false;
    thread_pool.count = num_threads;
    thread_pool.jobs = NULL;

    thread_pool.threads = (pthread_t *)malloc(sizeof(pthread_t) * num_threads);

    for (int i = 0; i < num_threads; i++)
    {
        pthread_create(&thread_pool.threads[i], NULL, generic_task, NULL);
    }
}

void destroy_thread_pool()
{
    // wait for the job board to be empty
    pthread_mutex_lock(&thread_pool.job_mutex);
    while (thread_pool.jobs)
    {
        pthread_mutex_unlock(&thread_pool.job_mutex);
        sleep(2);
        pthread_mutex_lock(&thread_pool.job_mutex);
    }
    thread_pool.stop = true;
    pthread_cond_broadcast(&thread_pool.job_cond);

    pthread_mutex_unlock(&thread_pool.job_mutex);

    for (size_t i = 0; i < thread_pool.count; i++)
    {
        pthread_join(thread_pool.threads[i], NULL);
    }
    free(thread_pool.threads);
    pthread_mutex_destroy(&thread_pool.job_mutex);
    pthread_cond_destroy(&thread_pool.job_cond);
}

void add_job(void *(*job_func)(void *), void *job_arg)
{
    job *new_job = (job *)malloc(sizeof(job));
    new_job->job_func = job_func;
    new_job->job_arg = job_arg;
    new_job->job_next = NULL;

    pthread_mutex_lock(&thread_pool.job_mutex);
    if (thread_pool.jobs == NULL)
    {
        thread_pool.jobs = new_job;
        thread_pool.last_job = new_job;
    }
    else
    {
        thread_pool.last_job->job_next = new_job;
        thread_pool.last_job = new_job;
    }
    pthread_mutex_unlock(&thread_pool.job_mutex);
    pthread_cond_signal(&thread_pool.job_cond);
}
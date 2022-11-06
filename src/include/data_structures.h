#ifndef DATA_STRUCTURES_H
#define DATA_STRUCTURES_H

#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>

// Queue.c
typedef union queue_element
{
    int int_val;
    char *str_val;
    void *ptr_val;
    pthread_t pthread_val;

} queue_element;

typedef struct Queue
{
    size_t front, rear, size;
    size_t capacity;
    queue_element *array;
} Queue;

Queue *create_queue(size_t capacity);
int is_queue_full(Queue *queue);
int is_queue_empty(Queue *queue);
void enqueue(Queue *queue, queue_element item);
queue_element dequeue(Queue *queue);
//

// hash_table.c

typedef void *hash_element;

typedef struct node
{
    hash_element key;
    hash_element val;
    size_t depth;
    struct node *next;
} node;

typedef struct hashtable
{
    size_t size;
    size_t count;
    size_t (*hash_function)(hash_element, size_t);
    size_t (*compare_function)(hash_element, hash_element);
    node **array;
} hashtable;

typedef struct node_array
{
    size_t node_size;
    node **array;

} node_array;

int create_ht(hashtable **ht, size_t size, size_t (*hash_function)(hash_element, size_t), size_t (*compare_function)(hash_element, hash_element));
int put_ht(hashtable *ht, hash_element key, hash_element value);
int get_ht(hashtable *ht, hash_element key, hash_element *values, int num_values, int *num_results);
int erase_ht(hashtable *ht, hash_element key);
node_array get_keys(hashtable *ht);
int deallocate_ht(hashtable *ht);

#endif
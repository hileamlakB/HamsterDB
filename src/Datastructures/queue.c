#include <stdlib.h>
#include "data_structures.h"

// creates a queue
Queue *create_queue(size_t capacity)
{

    Queue *queue = (Queue *)malloc(sizeof(Queue));
    queue->capacity = capacity;
    queue->front = queue->size = 0;
    queue->rear = capacity - 1;
    queue->array = malloc(queue->capacity * sizeof(queue_element));
    return queue;
}

// checks if queue is full
int is_queue_full(Queue *queue)
{
    return (queue->size == queue->capacity);
}

// checks if queue is empty
int is_queue_empty(Queue *queue)
{
    return (queue->size == 0);
}

// adds an item to the queue
void enqueue(Queue *queue, queue_element item)
{
    if (is_queue_full(queue))
        return;

    // the queue is ciruclar
    queue->rear = (queue->rear + 1) % queue->capacity;
    queue->array[queue->rear] = item;
    queue->size = queue->size + 1;
}

// removes an item from the queue
queue_element dequeue(Queue *queue)
{
    if (is_queue_empty(queue))
        return (queue_element){0};

    queue_element item = queue->array[queue->front];
    queue->front = (queue->front + 1) % queue->capacity;
    queue->size = queue->size - 1;
    return item;
}

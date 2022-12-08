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

typedef struct hash_elements
{
    hash_element *values;
    size_t values_size;
    size_t capacity;
} hash_elements;

typedef struct node
{
    hash_element key;
    hash_element val;
    hash_elements fat_val;
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
    bool is_fat;
} hashtable;

typedef struct node_array
{
    size_t node_size;
    node **array;

} node_array;

int create_ht(hashtable **ht, size_t size, size_t (*hash_function)(hash_element, size_t), size_t (*compare_function)(hash_element, hash_element), bool fat);
int put_ht(hashtable *ht, hash_element key, hash_element value);
hash_elements get_ht(hashtable *ht, hash_element key);
int erase_ht(hashtable *ht, hash_element key);
node_array get_keys(hashtable *ht);
int deallocate_ht(hashtable *ht, bool free_key, bool free_value); // the boolean is to determine if the key should be freed or not
void print_ht(hashtable *ht);

// btree.c
/* create a new empty tree */
#define FANOUT (255)

// we want MAX(sizeof(Btree_node)) = 4096
// = 4 + 4 + 2 * fanout * 4 + (fanout + 1) * 8
// thus fanout = 255

typedef struct key_loc_tuple
{
    int key;
    int location;
} key_loc_tuple;

typedef struct Btree_node
{
    int isLeaf;  /* is this a leaf node? */
    int numKeys; /* how many keys does this node contain? */
    key_loc_tuple keys[FANOUT];
    struct Btree_node *children[FANOUT + 1]; /* children[i] holds nodes < keys[i] */
} Btree_node;
typedef struct serialized_node
{
    int num_keys;
    int is_leaf;
    key_loc_tuple keys[FANOUT];
} serialized_node;

void bt_free(Btree_node *btree);
int bt_search(Btree_node *btree, int key);
void bt_insert(Btree_node *btree, int key, int location);
void bt_print(Btree_node *btree);

Btree_node *bt_create(void);

int retrive_location(Btree_node *btree, int key);
int binary_search(int n, key_loc_tuple keys[FANOUT], int key);

serialized_node *bt_serialize(Btree_node *btree, serialized_node *storage);
Btree_node *bt_deserialize(serialized_node **s_node);

#endif
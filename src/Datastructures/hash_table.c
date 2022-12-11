#include "data_structures.h"
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include <stdio.h>
#include <pthread.h>

// Improvment ideas
// 1. sophisticated hash function
//      instead of having one use n collosion handling function
//      use a prime number(understand why)
// 2. Store more than one key_val pair in a node (fat-node) (read up on cs 61 material)
// 3. don't allocate size amount of space ? not sure if this is an improvnment
// 4. grow hash table capacity once the the size of linked list becomes over a certain limit

//  This is a hash functions that determines the location for each key
int hash(int key, int size)
{
    return key % size;
}

size_t hash_string(hash_element str, size_t size)
{
    char *s = (char *)str;
    // using the polynomial rolling hash function
    const int p = 31;
    // size_t m = 1e9 + 7;
    size_t m = size;
    size_t hash = 0;
    long p_pow = 1;
    for (size_t i = 0; s[i] != '\0'; i++)
    {
        hash = (hash + (s[i] - 'a' + 1) * p_pow) % m;
        p_pow = (p_pow * p) % m;
    }
    return hash % size;
}

// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
int create_ht(hashtable **ht, size_t size, size_t (*hash_function)(hash_element, size_t),
              size_t (*compare_function)(hash_element, hash_element), bool fat)
{
    (void)fat;
    *ht = malloc(sizeof(hashtable));
    if (*ht == NULL)
    {
        return -1;
    }
    (*ht)->size = size;
    (*ht)->count = 0;
    (*ht)->array = calloc(size, sizeof(node *));
    if ((*ht)->array == NULL)
    {
        free(*ht);
        return -1;
    }
    (*ht)->hash_function = hash_function;
    (*ht)->compare_function = compare_function;

    pthread_mutex_t *locks = calloc(size, sizeof(pthread_mutex_t));

    // initialize the locks
    for (size_t i = 0; i < size; i++)
    {
        pthread_mutex_init(&locks[i], NULL);
    }

    (*ht)->locks = locks;

    return 0;
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is called and fails).
int put_ht(hashtable *ht, hash_element key, hash_element value)
{

    assert(ht);

    int index = ht->hash_function(key, ht->size);
    node *new_node = (node *)malloc(sizeof(node));

    if (!new_node)
    {
        return -1;
    }
    new_node->key = key;
    new_node->val = value;
    new_node->next = NULL;

    // increament the number of elements
    ht->count++;

    // get the lock for the index
    pthread_mutex_lock(&ht->locks[index]);

    // see if there was no element in its location
    if (!ht->array[index])
    {
        ht->array[index] = new_node;
        ht->array[index]->depth = 1;
        pthread_mutex_unlock(&ht->locks[index]);
        return 0;
    }

    // if there was append the linked list at the top
    new_node->depth = ht->array[index]->depth + 1;
    new_node->next = ht->array[index];
    ht->array[index] = new_node;

    pthread_mutex_unlock(&ht->locks[index]);

    return 0;
}

int fat_put_ht(hashtable *ht, hash_element key, hash_element value)
{

    assert(ht);
    if (ht->count == ht->size)
    {
        return -1;
    }

    int index = ht->hash_function(key, ht->size);
    if (ht->array[index] == NULL)
    {
        ht->array[index] = (node *)malloc(sizeof(node));
        ht->array[index]->key = key;
        ht->array[index]->fat_val = (hash_elements){.values_size = 0, .capacity = 1, .values = malloc(sizeof(hash_element))};
        ht->array[index]->fat_val.values[0] = value;
        ht->array[index]->fat_val.values_size += 1;

        ht->array[index]->depth = 1;
        ht->count++;
        return 0;
    }

    // check if the key is already in the list
    node *curr = ht->array[index];
    while (curr)
    {
        if (ht->compare_function(curr->key, key) == 0)
        {
            // if the fat_val is full
            if (curr->fat_val.values_size == curr->fat_val.capacity)
            {
                curr->fat_val.capacity *= 2;
                curr->fat_val.values = realloc(curr->fat_val.values, curr->fat_val.capacity * sizeof(hash_element));
            }
            curr->fat_val.values[curr->fat_val.values_size] = value;
            curr->fat_val.values_size += 1;
            return 0;
        }
        curr = curr->next;
    }

    // if the key is not in the list
    node *new_node = (node *)malloc(sizeof(node));
    new_node->key = key;
    new_node->fat_val = (hash_elements){.values_size = 0, .capacity = 1, .values = malloc(sizeof(hash_element))};
    new_node->fat_val.values[0] = value;
    new_node->fat_val.values_size += 1;
    new_node->depth = ht->array[index]->depth + 1;
    new_node->next = ht->array[index];
    ht->array[index] = new_node;
    ht->count++;
    return 0;
}

hash_elements fat_get_ht(hashtable *ht, hash_element key)
{
    assert(ht);
    int index = ht->hash_function(key, ht->size);
    node *curr = ht->array[index];
    while (curr)
    {
        if (ht->compare_function(curr->key, key) == 0)
        {

            return curr->fat_val;
        }
        curr = curr->next;
    }
    return (hash_elements){
        .values_size = 0,
        .capacity = 0,
        .values = NULL,
    };
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int fat_deallocate_ht(hashtable *ht, bool free_key, bool free_value)
{

    for (size_t i = 0; i < ht->size; i++)
    {
        node *curr = ht->array[i];
        while (curr)
        {
            node *to_free = curr;
            curr = curr->next;
            if (free_key)
            {
                free(to_free->key);
            }
            if (free_value)
            {
                for (size_t i = 0; i < to_free->fat_val.values_size; i++)
                {
                    free(to_free->fat_val.values[i]);
                }
            }
            free(to_free->fat_val.values);
            free(to_free);
        }
    }
    free(ht->array);
    free(ht->locks);
    free(ht);
    return 0;
}

// This method retrieves entries with a matching key and stores the corresponding values in the
// values array. The size of the values array is given by the parameter
// num_values. If there are more matching entries than num_values, they are not
// stored in the values array to avoid a buffer overflow. The function returns
// the number of matching entries using the num_results pointer. If the value of num_results is greater than
// num_values, the caller can invoke this function again (with a larger buffer)
// to get values that it missed during the first call.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
hash_elements get_ht(hashtable *ht, hash_element key)
{
    int index = ht->hash_function(key, ht->size);
    hash_elements res = {
        .values_size = 0,
        .values = NULL,
        .capacity = 0,
    };

    // if there is no element in the location
    if (!ht->array[index])
    {
        return res;
    }

    size_t found = 0;
    node *curr = ht->array[index];
    while (curr)
    {
        if (ht->compare_function(curr->key, key) == 0)
        {
            found++;
            if (res.capacity <= found)
            {
                res.capacity = found * 2;
                res.values = realloc(res.values, res.capacity * sizeof(hash_element));
            }
            res.values[res.values_size++] = curr->val;
        }
        curr = curr->next;
    }

    return res;
}

node *get_raw_ht(hashtable *ht, hash_element key)
{
    int index = ht->hash_function(key, ht->size);

    // if there is no element in the location
    if (!ht->array[index])
    {
        return NULL;
    }

    node *curr = ht->array[index];
    while (curr)
    {
        if (ht->compare_function(curr->key, key) == 0)
        {
            return curr;
        }
        curr = curr->next;
    }

    return NULL;
}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int erase_ht(hashtable *ht, hash_element key)
{
    int index = ht->hash_function(key, ht->size);

    node *curr = ht->array[index];
    node *prev = NULL;

    if (!curr)
    {
        return 0;
    }

    while (curr)
    {
        if (ht->compare_function(curr->key, key))
        {
            if (prev)
            {
                prev->next = curr->next;
            }
            else
            {
                ht->array[index] = curr->next;
            }

            node *to_free = curr;
            curr = curr->next;
            free(to_free);
            ht->count--;
        }
        else
        {
            prev = curr;
            curr = curr->next;
        }
    }
    return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate_ht(hashtable *ht, bool free_key, bool free_value)
{

    for (size_t i = 0; i < ht->size; i++)
    {
        node *curr = ht->array[i];
        while (curr)
        {
            node *to_free = curr;
            curr = curr->next;
            if (free_key)
            {
                free(to_free->key);
            }
            if (free_value)
            {
                free(to_free->val);
            }
            free(to_free);
        }
    }
    free(ht->array);
    free(ht->locks);
    free(ht);
    return 0;
}

// This method prints the contents of the hash table.
void print_ht(hashtable *ht)
{
    for (size_t i = 0; i < ht->size; i++)
    {
        node *curr = ht->array[i];
        while (curr)
        {
            printf("key: %d, val: %d, depth: %ld->", *((int *)curr->key), *((int *)curr->val), curr->depth);
            curr = curr->next;
        }
        printf("\n");
    }
}
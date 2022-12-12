#define _DEFAULT_SOURCE
#include "cs165_api.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include "Utils/utils.h"
#include <data_structures.h>

#include <assert.h>

// find variable in pool
// add value to pool

hashtable *var_pool = NULL;
pthread_mutex_t var_pool_lock;

#define var_pool_size 127

// // djb hash function
// size_t hash_string(hash_element string, size_t size)
// {
//     char *str = (char *)string;
//     unsigned long hash = 5381;
//     int c;

//     while ((c = *str++))
//         hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

//     return hash % size;
// }

// string compare
size_t string_cmp(hash_element a, hash_element b)
{
    return strcmp((char *)a, (char *)b);
}

void add_var(Variable *var)
{

    pthread_mutex_lock(&var_pool_lock);
    if (var_pool == NULL)
    {
        create_ht(&var_pool, var_pool_size, hash_string, string_cmp, false, free, free_var);
    }

    // remove variable if it already exits
    erase_ht(var_pool, var->name);
    put_ht(var_pool, strdup(var->name), var);
    pthread_mutex_unlock(&var_pool_lock);
}

Variable *find_var(char *name)
{
    pthread_mutex_lock(&var_pool_lock);
    hash_elements results = get_ht(var_pool, name);
    if (results.values_size == 0)
    {
        pthread_mutex_unlock(&var_pool_lock);
        return NULL;
    }

    Variable *var = (Variable *)results.values[0]; // get the most recent version
    free(results.values);
    pthread_mutex_unlock(&var_pool_lock);
    return var;
}

void free_linked_list(linkedList *node)
{

    while (node != NULL)
    {
        linkedList *next = node->next;
        pos_vec *pos = (pos_vec *)node->data;
        free(pos->values);
        free(node->data);
        free(node);
        node = next;
    }
}

void free_var(void *vars)
{
    Variable *var = (Variable *)vars;
    // free var
    if (var->type == VECTOR_CHAIN)
    {
        free_linked_list(var->result.pos_vec_chain);
    }
    if (var->type == POSITION_VECTOR || var->type == VALUE_VECTOR)
    {
        if (var->result.values.values)
        {
            free(var->result.values.values);
        }
    }

    if (var->name)
    {
        free(var->name);
    }
    free(var);
}

void free_var_pool()
{
    if (!var_pool)
    {
        return;
    }
    deallocate_ht(var_pool, true, true);
}
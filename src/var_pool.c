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
#include "utils.h"
#include "client_context.h"
#include <assert.h>

// find variable in pool
// add value to pool

pthread_mutex_t var_pool_lock;

void add_var(Variable *var)
{

    linkedList *node = malloc(sizeof(linkedList));

    node->data = (void *)var;

    pthread_mutex_lock(&var_pool_lock);
    node->next = var_pool;
    var_pool = node;
    pthread_mutex_unlock(&var_pool_lock);
}

Variable *find_var(char *name)
{
    pthread_mutex_lock(&var_pool_lock);
    linkedList *node = var_pool;
    while (node != NULL)
    {
        Variable *var = (Variable *)node->data;
        if (strcmp(var->name, name) == 0)
        {
            pthread_mutex_unlock(&var_pool_lock);
            return var;
        }
        node = node->next;
    }
    pthread_mutex_unlock(&var_pool_lock);
    return NULL;
}

void free_linked_list(linkedList *node)
{

    while (node != NULL)
    {
        linkedList *next = node->next;
        free(node->data);
        free(node);
        node = next;
    }
}

void free_var_pool()
{
    linkedList *node = var_pool;
    while (node != NULL)
    {
        linkedList *next = node->next;
        Variable *var = (Variable *)node->data;
        if (var->type == VECTOR_CHAIN)
        {
            free_linked_list(var->result.pos_vec_chain);
        }
        if (var->type == POSITION_VECTOR)
        {
            free(var->result.values.values);
        }

        free(var->name);
        free(var);
        free(node);
        node = next;
    }
}
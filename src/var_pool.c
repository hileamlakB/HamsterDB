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

void add_var(char *name, vector result, variable_type var_type)
{

    linkedList *node = malloc(sizeof(linkedList));

    Variable *var = malloc(sizeof(Variable));
    var->name = strdup(name);
    var->result = result;
    var->type = var_type;
    var->exists = true;

    node->data = (void *)var;
    node->next = var_pool;
    var_pool = node;
}

Variable *find_var(char *name)
{
    linkedList *node = var_pool;
    while (node != NULL)
    {
        Variable *var = (Variable *)node->data;
        if (strcmp(var->name, name) == 0)
        {
            return var;
        }
        node = node->next;
    }
    return NULL;
}

void free_var_pool()
{
    linkedList *node = var_pool;
    while (node != NULL)
    {
        linkedList *next = node->next;
        Variable *var = (Variable *)node->data;
        free(var->name);
        if (var->result.values)
        {
            free(var->result.values);
        }
        free(var);
        free(node);
        node = next;
    }
}
#define _DEFAULT_SOURCE
#include "tasks.h"
#include "cs165_api.h"
#include <string.h>

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

size_t hash_cmp_str(hash_element str1, hash_element str2)
{
    return strcmp((char *)str1, (char *)str2);
}

grouped_tasks query_planner(DbOperator **db_ops, int num_ops, Status *status)
{
    hashtable *independent;
    int result = create_ht(
        &independent,
        num_ops,
        &hash_string,
        &hash_cmp_str);

    if (result == -1)
    {
        status->code = ERROR;
        return (grouped_tasks){
            .independent = NULL};
    };

    tasks dependent;

    dependent.task = malloc(sizeof(task) * num_ops);

    // make sure this doesn't fail
    dependent.num_tasks = 0;

    if (dependent.task == NULL)
    {
        printf("Error: malloc failed in query_planner");
        // before you crush exit do crash dumps and
        // backups
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < num_ops; i++)
    {
        DbOperator *current_op = db_ops[i];
        if (current_op->type == SELECT)
        {
            if (current_op->operator_fields.select_operator.type == SELECT_COL)
            {
                put_ht(independent, strdup(current_op->operator_fields.select_operator.column->name), current_op);
            }
            else if (current_op->operator_fields.select_operator.type == SELECT_POS)
            {

                if (!current_op->operator_fields.select_operator.val_vec->exists)
                {
                    // see if it was satisified this round
                    Variable *pos_vec = find_var(current_op->operator_fields.select_operator.val_vec->name);
                    if (pos_vec)
                    {
                        // don't forge to free everything not just the main vector
                        free(current_op->operator_fields.select_operator.val_vec->name);
                        free(current_op->operator_fields.select_operator.val_vec);
                        current_op->operator_fields.select_operator.val_vec = pos_vec;
                    }
                }

                if (!current_op->operator_fields.select_operator.pos_vec->exists)
                {
                    Variable *pos_vec = find_var(current_op->operator_fields.select_operator.pos_vec->name);
                    if (!pos_vec)
                    {
                        free(current_op->operator_fields.select_operator.pos_vec->name);
                        free(current_op->operator_fields.select_operator.pos_vec);
                        current_op->operator_fields.select_operator.pos_vec = pos_vec;
                    }
                }

                if (current_op->operator_fields.select_operator.val_vec->exists && current_op->operator_fields.select_operator.pos_vec->exists)
                {
                    put_ht(independent, (void *)strdup(current_op->operator_fields.select_operator.column->name), current_op);
                }
                else
                {
                    dependent.task[dependent.num_tasks].type = SINGLE;
                    dependent.task[dependent.num_tasks].task.db_op = current_op;
                    dependent.num_tasks++;
                }
            }
        }

        else if (current_op->type == FETCH)
        {
            if (!current_op->operator_fields.fetch_operator.variable->exists)
            {
                Variable *pos_vec = find_var(current_op->operator_fields.fetch_operator.variable->name);
                if (pos_vec)
                {
                    free(current_op->operator_fields.fetch_operator.variable->name);
                    free(current_op->operator_fields.fetch_operator.variable);
                    current_op->operator_fields.fetch_operator.variable = pos_vec;
                }
            }

            if (current_op->operator_fields.fetch_operator.variable->exists)
            {
                put_ht(independent, current_op->operator_fields.fetch_operator.column->name, current_op);
            }
            else
            {
                dependent.task[dependent.num_tasks].type = SINGLE;
                dependent.task[dependent.num_tasks].task.db_op = current_op;
                dependent.num_tasks++;
            }
        }
    }

    grouped_tasks grouped;
    grouped.independent = independent;
    grouped.dependent = dependent;

    return grouped;
}

void free_grouped_tasks(grouped_tasks grouped_tasks)
{
    deallocate_ht(grouped_tasks.independent, true, false);
    free(grouped_tasks.dependent.task);
}
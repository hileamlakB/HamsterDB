
#define _DEFAULT_SOURCE
#include <string.h>
#include "cs165_api.h"
#include <assert.h>
#include <unistd.h>

size_t get_size(Variable *var)
{
    size_t size = 0;
    if (var->type == INT_VALUE || var->type == FLOAT_VALUE)
    {
        size = 1;
    }
    else if (var->type == POSITION_VECTOR || var->type == VALUE_VECTOR)
    {
        size = var->result.values.size;
    }
    else if (var->type == RANGE)
    {
        size = var->result.range[1] - var->result.range[0];
    }
    else if (var->type == VECTOR_CHAIN)
    {
        size = var->vec_chain_size;
    }

    return size;
}

void nested_loop_join(DbOperator *query)
{
    Variable *outer_result = malloc(sizeof(Variable));
    Variable *inner_result = malloc(sizeof(Variable));

    Variable *val1 = query->operator_fields.join_operator.val1;
    Variable *val2 = query->operator_fields.join_operator.val2;

    Variable *pos1 = query->operator_fields.join_operator.pos1;
    Variable *pos2 = query->operator_fields.join_operator.pos2;

    Variable *inner, *outer, *inner_pos, *outer_pos;
    char *inner_handle, *outer_handle;
    linkedList *inner_chain, *outer_chain;
    size_t inner_chain_index = 0, outer_chain_index = 0;

    // choose the smaller table to be the inner loop
    if (get_size(val1) < get_size(val2))
    {
        inner = val1;
        inner_pos = pos1;
        inner_handle = query->operator_fields.join_operator.handler1;
        outer = val2;
        outer_pos = pos2;
        outer_handle = query->operator_fields.join_operator.handler2;
    }
    else
    {
        inner = val2;
        inner_pos = pos2;
        inner_handle = query->operator_fields.join_operator.handler2;
        outer = val1;
        outer_pos = pos1;
        outer_handle = query->operator_fields.join_operator.handler1;
    }

    if (outer_pos->type == VECTOR_CHAIN)
    {
        outer_chain = outer_pos->result.pos_vec_chain;
        outer_chain_index = 0;
    }

    int result_size = 0;
    int *oresult = malloc(sizeof(int) * get_size(outer) * get_size(inner));
    int *iresult = malloc(sizeof(int) * get_size(inner) * get_size(outer));

    // nested loop join
    for (size_t i = 0; i < get_size(outer); i++)
    {

        if (inner_pos->type == VECTOR_CHAIN)
        {
            inner_chain = inner_pos->result.pos_vec_chain;
            inner_chain_index = 0;
        }
        int outer_value = outer->result.values.values[i];
        int outer_position;

        if (outer_pos->type == POSITION_VECTOR)
        {
            outer_position = outer_pos->result.values.values[i];
        }
        else if (outer_pos->type == RANGE)
        {
            outer_position = outer_pos->result.range[0] + i;
        }
        else
        {
            assert(outer_pos->type == VECTOR_CHAIN);
            if (outer_chain_index >= ((pos_vec *)outer_chain->data)->size)
            {
                outer_chain_index = 0;
                outer_chain = outer_chain->next;
            }
            outer_position = ((pos_vec *)outer_chain->data)->values[outer_chain_index];
            outer_chain_index += 1;
        }
        for (size_t j = 0; j < get_size(inner); j++)
        {

            int inner_value = inner->result.values.values[j];

            int inner_position;
            if (inner_pos->type == POSITION_VECTOR)
            {
                inner_position = inner_pos->result.values.values[j];
            }
            else if (inner_pos->type == RANGE)
            {
                inner_position = inner_pos->result.range[0] + j;
            }
            else
            {
                assert(inner_pos->type == VECTOR_CHAIN);
                if (inner_chain_index >= ((pos_vec *)inner_chain->data)->size)
                {
                    inner_chain_index = 0;
                    inner_chain = inner_chain->next;
                }
                inner_position = ((pos_vec *)inner_chain->data)->values[inner_chain_index];
                inner_chain_index++;
            }

            if (outer_value == inner_value)
            {
                oresult[result_size] = outer_position;
                iresult[result_size] = inner_position;
                result_size++;
            }
        }
    }

    *outer_result = (Variable){
        .type = POSITION_VECTOR,
        .result = {
            .values = {
                .values = oresult,
                .size = result_size,
            },
        },
        .exists = true,
        .name = strdup(outer_handle)};

    *inner_result = (Variable){
        .type = POSITION_VECTOR,
        .result = {
            .values = {
                .values = iresult,
                .size = result_size,
            },
        },
        .exists = true,
        .name = strdup(inner_handle)};

    add_var(outer_result);
    add_var(inner_result);
}

void hash_join(DbOperator *query)
{
    (void)query;
}

void join(DbOperator *query)
{
    if (query->operator_fields.join_operator.type == NESTED_LOOP)
    {
        nested_loop_join(query);
    }
    else
    {
        hash_join(query);
    }
}
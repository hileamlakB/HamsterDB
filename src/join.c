
#define _DEFAULT_SOURCE
#include <string.h>
#include "cs165_api.h"
#include <assert.h>
#include <unistd.h>
#include "data_structures.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

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

long robert_junkins_hash(long a)
{
    a = (a + 0x7ed55d16) + (a << 12);
    a = (a ^ 0xc761c23c) ^ (a >> 19);
    a = (a + 0x165667b1) + (a << 5);
    a = (a + 0xd3a2646c) ^ (a << 9);
    a = (a + 0xfd7046c5) + (a << 3);
    a = (a ^ 0xb55a4f09) ^ (a >> 16);
    return a;
}

size_t int_hash(hash_element element, size_t size)
{
    return robert_junkins_hash(*((int *)element)) % size;
}

size_t int_compare(hash_element a, hash_element b)
{
    if (*((int *)a) > *((int *)b))
    {
        return 1;
    }
    else if (*((int *)a) < *((int *)b))
    {
        return -1;
    }

    return 0;
}

void hash_join(DbOperator *query)
{
    Variable *left_result = malloc(sizeof(Variable));
    Variable *right_result = malloc(sizeof(Variable));

    Variable *val1 = query->operator_fields.join_operator.val1;
    Variable *val2 = query->operator_fields.join_operator.val2;

    Variable *pos1 = query->operator_fields.join_operator.pos1;
    Variable *pos2 = query->operator_fields.join_operator.pos2;

    hashtable *ht;
    size_t pos1_size = get_size(pos1);
    size_t pos2_size = get_size(pos2);
    int size = max(pos1_size, pos2_size);
    long int hash_table_size = find_closet_prime(size);
    create_ht(&ht, hash_table_size, int_hash, int_compare);

    // iterate throught the values and positions of the first variable and insert
    // them into the hastable

    linkedList *left_chain, *right_chain;
    size_t left_chain_index = 0, right_chain_index = 0;

    if (pos1->type == VECTOR_CHAIN)
    {
        left_chain = pos1->result.pos_vec_chain;
        left_chain_index = 0;
    }

    if (pos2->type == VECTOR_CHAIN)
    {
        right_chain = pos2->result.pos_vec_chain;
        right_chain_index = 0;
    }

    for (size_t i = 0; i < pos1_size; i++)
    {
        int value = val1->result.values.values[i];
        int position;
        if (pos1->type == POSITION_VECTOR)
        {
            position = pos1->result.values.values[i];
        }
        else if (pos1->type == RANGE)
        {
            position = pos1->result.range[0] + i;
        }
        else
        {
            assert(pos1->type == VECTOR_CHAIN);
            if (left_chain_index >= ((pos_vec *)left_chain->data)->size)
            {
                left_chain_index = 0;
                left_chain = left_chain->next;
            }
            position = ((pos_vec *)left_chain->data)->values[left_chain_index];
            left_chain_index += 1;
        }

        int *key = malloc(sizeof(int));
        *key = value;
        int *value_ptr = malloc(sizeof(int));
        *value_ptr = position;

        put_ht(ht, key, value_ptr);
    }

    // print_ht(ht);

    int *lresult = malloc(sizeof(int) * size * size);
    int *rresult = malloc(sizeof(int) * size * size);
    size_t l, r = 0;

    // go through the second variable and check if the values are in the hashtable, if it is add it to results
    for (size_t i = 0; i < pos2_size; i++)
    {
        int value = val2->result.values.values[i];
        int position;
        if (pos2->type == POSITION_VECTOR)
        {
            position = pos2->result.values.values[i];
        }
        else if (pos2->type == RANGE)
        {
            position = pos2->result.range[0] + i;
        }
        else
        {
            assert(pos2->type == VECTOR_CHAIN);
            if (right_chain_index >= ((pos_vec *)right_chain->data)->size)
            {
                right_chain_index = 0;
                right_chain = right_chain->next;
            }
            position = ((pos_vec *)right_chain->data)->values[right_chain_index];
            right_chain_index += 1;
        }

        int *tmp_positions[size];
        int local_result = 0;

        get_ht(ht, &value, (void **)&tmp_positions, size, &local_result);
        while (local_result)
        {
            lresult[l] = *tmp_positions[local_result - 1];
            rresult[r] = position;
            l++;
            r++;
            local_result--;
        }
    }

    *left_result = (Variable){
        .type = POSITION_VECTOR,
        .result = {
            .values = {
                .values = lresult,
                .size = l,
            },
        },
        .exists = true,
        .name = strdup(query->operator_fields.join_operator.handler1)};

    *right_result = (Variable){
        .type = POSITION_VECTOR,
        .result = {
            .values = {
                .values = rresult,
                .size = r,
            },
        },
        .exists = true,
        .name = strdup(query->operator_fields.join_operator.handler2)};

    add_var(left_result);
    add_var(right_result);
    deallocate_ht(ht, true, true);
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

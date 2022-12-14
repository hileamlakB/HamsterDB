
#define _DEFAULT_SOURCE
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "Utils/utils.h"

#include <assert.h>
#include "tasks.h"

#include <stdatomic.h>
#include <pthread.h>

#include <Loader/load.h>
#include <Serializer/serialize.h>
#include <Parser/parse.h>
#include <Create/create.h>
#include <Engine/engine.h>

double generic_sum(Variable *variable)
{
    assert(variable->type == VALUE_VECTOR);

    double sum = 0;
    for (size_t i = 0; i < variable->result.values.size; i++)
    {
        sum += variable->result.values.values[i];
    }
    return sum;
}

void sum(AvgOperator avg_operator)
{
    char *handler = avg_operator.handler;

    if (avg_operator.type == VARIABLE_O)
    {
        Variable *variable = avg_operator.variable;
        double sum = generic_sum(variable);
        Variable *fin_result = malloc(sizeof(Variable));
        *fin_result = (Variable){
            .type = INT_VALUE,
            .result.ivalue = sum,
            .name = strdup(handler),
            .exists = true};

        add_var(fin_result);
    }
    else if (avg_operator.type == COLUMN_O)
    {

        // Table *table = avg_operator.address.table;
        Column *column = avg_operator.address.col;

        // check if sum is already calculated
        // if (column->metadata->sum[0] == 1)
        // {
        // handle new inserts and and new loads
        Variable *fin_result = malloc(sizeof(Variable));
        *fin_result = (Variable){
            .type = INT_VALUE,
            .result.ivalue = column->metadata->sum,
            .name = strdup(handler),
            .exists = true};
        add_var(fin_result);
        return;
        // }

        // map_col(table, column, 0);

        // double sum = 0;
        // size_t index = 0;
        // while (index < table->rows)
        // {
        //     sum += column->data[index];
        //     index++;
        // }

        // column->metadata->sum[0] = 1;
        // column->metadata->sum[1] = sum;

        // Variable *fin_result = malloc(sizeof(Variable));
        // *fin_result = (Variable){
        //     .type = INT_VALUE,
        //     .result.ivalue = sum,
        //     .name = strdup(handler),
        //     .exists = true};

        // add_var(fin_result);
    }
}

void average(char *handler, Variable *variable)
{

    double avg = (variable->result.values.size) ? generic_sum(variable) / variable->result.values.size : 0;
    Variable *fin_result = malloc(sizeof(Variable));
    *fin_result = (Variable){
        .type = FLOAT_VALUE,
        .result.fvalue = avg,
        .name = strdup(handler),
        .exists = true};
    add_var(fin_result);
}

void add_sub(char *handler, Variable *variable1, Variable *variable2, int (*op)(int a, int b))
{
    linkedList *variable1_lst;
    size_t variable1_index = 0;
    linkedList *variable2_lst;
    size_t variable2_index = 0;
    size_t result_size = 0;

    if (variable1->type == VECTOR_CHAIN)
    {
        variable1_lst = variable1->result.pos_vec_chain;
        result_size = variable1->vec_chain_size;
    }
    else
    {
        result_size = variable1->result.values.size;
    }

    if (variable2->type == VECTOR_CHAIN)
    {
        variable2_lst = variable2->result.pos_vec_chain;
        if (result_size == 0)
        {
            result_size = variable2->vec_chain_size;
        }
        assert(result_size == variable2->vec_chain_size);
    }
    else
    {
        result_size = variable2->result.values.size;
    }

    int *result = malloc(sizeof(int) * variable1->result.values.size);
    for (size_t i = 0; i < variable1->result.values.size; i++)
    {
        int valu1, value2;
        if (variable1->type == VECTOR_CHAIN)
        {
            if (variable1_index >= ((pos_vec *)variable1_lst->data)->size)
            {
                variable1_lst = variable1_lst->next;
                variable1_index = 0;
            }
            valu1 = ((pos_vec *)variable1_lst->data)->values[variable1_index];
            variable1_index++;
        }
        else
        {
            valu1 = variable1->result.values.values[i];
        }

        if (variable2->type == VECTOR_CHAIN)
        {
            if (variable2_index >= ((pos_vec *)variable2_lst->data)->size)
            {
                variable2_lst = variable2_lst->next;
                variable2_index = 0;
            }
            value2 = ((pos_vec *)variable2_lst->data)->values[variable2_index];
            variable2_index++;
        }
        else
        {
            value2 = variable2->result.values.values[i];
        }

        result[i] = op(valu1, value2);
    }

    Variable *fin_result = malloc(sizeof(Variable));
    *fin_result = (Variable){
        .type = VALUE_VECTOR,
        .result.values.values = result,
        .result.values.size = variable1->result.values.size,
        .name = strdup(handler),
        .exists = true};

    add_var(fin_result);
}

void add(char *handler, Variable *variable1, Variable *variable2)
{
    add_sub(handler, variable1, variable2, add_op);
}

void sub(char *handler, Variable *variable1, Variable *variable2)
{

    add_sub(handler, variable1, variable2, sub_op);
}

void MinMax(MinMaxOperator minmax_operator, Status *status)
{
    status->code = OK;
    char *handler = minmax_operator.handler;
    Variable *variable = minmax_operator.variable;
    bool (*op)(int a, int b) = (minmax_operator.operation == MIN) ? min_op : max_op;

    int result[2];
    result[0] = variable->result.values.values[0];
    int index = 1;

    size_t size = 0;
    size_t chain_index = 0;
    linkedList *lst;
    if (variable->type == VECTOR_CHAIN)
    {
        size = variable->vec_chain_size;
        lst = variable->result.pos_vec_chain;
    }
    else
    {
        size = variable->result.values.size;
    }

    for (size_t i = 1; i < size; i++)
    {
        int value;
        if (variable->type == VECTOR_CHAIN)
        {
            if (chain_index >= ((pos_vec *)lst->data)->size)
            {
                lst = lst->next;
                chain_index = 0;
            }
            value = ((pos_vec *)lst->data)->values[chain_index];
            chain_index++;
        }
        else
        {
            value = variable->result.values.values[i];
        }
        result[index] = value;
        index -= op(value, result[0]);
        result[index] = value;
        index = 1;
    }

    Variable *fin_result = malloc(sizeof(Variable));
    *fin_result = (Variable){
        .type = INT_VALUE,
        .result.ivalue = result[0],
        .name = strdup(handler),
        .exists = true};

    add_var(fin_result);
}

#define _DEFAULT_SOURCE
#include <string.h>
#include <string.h>

#include <cs165_api.h>

String print_tuple(PrintOperator print_operator)
{
    if (print_operator.type == SINGLE_FLOAT)
    {
        char *str = malloc(sizeof(char) * MAX_INT_LENGTH * 2);
        size_t len = sprintf(str, "%.2f", print_operator.data.fvalue);
        return (String){.str = str, .len = len};
    }
    else if (print_operator.type == SINGLE_INT)
    {
        char *str = malloc(sizeof(char) * MAX_INT_LENGTH * 2);
        size_t len = sprintf(str, "%ld", print_operator.data.ivalue);
        return (String){.str = str, .len = len};
    }

    int width = print_operator.data.tuple.width;
    int height = print_operator.data.tuple.height;
    Variable **results = print_operator.data.tuple.data;
    linkedList *lst[width];
    size_t list_indexs[width];

    for (int i = 0; i < width; i++)
    {
        if (results[i]->type == VECTOR_CHAIN)
        {
            lst[i] = results[i]->result.pos_vec_chain;
            list_indexs[i] = 0;
        }
    }

    // for commas and extra variables, we have + 1,  MAX_INT_LENGTH + 1, +1 for newline
    size_t size = (width * height * (MAX_INT_LENGTH + 1));
    char *result = malloc(sizeof(char) * ((width * (MAX_INT_LENGTH + 1)) * height + 1) + 1);
    char *result_i = result;
    for (int row = 0; row < height; row++)
    {
        for (int col = 0; col < width; col++)
        {
            Variable *current_var = results[col];
            int printed = 0;
            if (current_var->type == INT_VALUE)
            {
                printed = sprintf(result_i, "%ld,", current_var->result.ivalue);
            }
            else if (current_var->type == FLOAT_VALUE)
            {
                printed = sprintf(result_i, "%.2f,", current_var->result.fvalue);
            }
            else if (current_var->type == POSITION_VECTOR || current_var->type == VALUE_VECTOR)
            {
                printed = sprintf(result_i, "%d,", current_var->result.values.values[row]);
            }
            else if (current_var->type == VECTOR_CHAIN)
            {

                if (list_indexs[col] >= ((pos_vec *)lst[col]->data)->size)
                {
                    list_indexs[col] = 0;
                    lst[col] = lst[col]->next;
                }

                printed = sprintf(result_i, "%d,", ((pos_vec *)lst[col]->data)->values[list_indexs[col]]);
                list_indexs[col] += 1;
            }
            result_i += printed;
        }

        sprintf(result_i - 1, "\n");
    }
    if (size)
    {
        *(result_i - 1) = '\n';
        *(result_i) = '\0';
    }

    free(results);
    return (String){.str = result, .len = result_i - result};
}

#include <Parser/parse.h>

DbOperator *parse_print(char *print_argument)
{
    char *tokenizer = print_argument;
    message_status status = OK_DONE;

    // remove parenthesis
    tokenizer++;

    size_t number_of_commas = 0;
    for (int i = 0; tokenizer[i]; i++)
    {
        if (tokenizer[i] == ',')
        {
            number_of_commas++;
        }
    }

    // remove trailing parenthesis
    tokenizer[strlen(tokenizer) - 1] = '\0';

    Variable **result_tupls = malloc(sizeof(Variable *) * (number_of_commas + 1));

    size_t i = 0;
    size_t max_row = 0;
    while (i <= number_of_commas)
    {
        char *var = next_token(&tokenizer, &status);
        Variable *var_location = find_var(var);
        if (var_location == NULL)
        {
            // free(result_tupls);
            cs165_log(stdout, "Query unsupported. Bad variable name\n");
            return NULL;
        }
        result_tupls[i] = var_location;

        size_t var_size = 0;
        if (var_location->type == POSITION_VECTOR || var_location->type == VALUE_VECTOR)
        {
            var_size = var_location->result.values.size;
        }
        else if (var_location->type == INT_VALUE || var_location->type == FLOAT_VALUE)
        {
            var_size = 1;
        }
        else
        {
            var_size = var_location->vec_chain_size;
        }

        max_row = max(max_row, var_size);
        i++;
    }

    DbOperator *print_op = malloc(sizeof(DbOperator));
    print_op->type = PRINT;
    if (number_of_commas == 0 && result_tupls[0]->type == FLOAT_VALUE)
    {

        print_op->operator_fields.print_operator.type = SINGLE_FLOAT;
        print_op->operator_fields.print_operator.data.fvalue = result_tupls[0]->result.fvalue;
        free(result_tupls);
    }
    else if (number_of_commas == 0 && result_tupls[0]->type == INT_VALUE)
    {
        print_op->operator_fields.print_operator.type = SINGLE_INT;
        print_op->operator_fields.print_operator.data.ivalue = result_tupls[0]->result.ivalue;
        free(result_tupls);
    }

    else
    {
        print_op->operator_fields.print_operator.type = TUPLE;
        print_op->operator_fields.print_operator.data.tuple = (PrintTuple){
            .data = result_tupls,
            .width = number_of_commas + 1,
            .height = max_row,
        };
    }

    return print_op;
}
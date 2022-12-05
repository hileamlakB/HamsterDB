#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>
#include <cs165_api.h>
#include <client_context.h>
#include <Parser/parse.h>

DbOperator *parse_sum(char *handle, char *sum_arg)
{
    char *tokenizer = sum_arg;
    // message_status status = OK_DONE;

    // remove parenthesis
    tokenizer++;

    int number_of_dots = 0;
    // check what kind of input we have
    for (size_t i = 0; i < strlen(tokenizer); i++)
    {
        if (tokenizer[i] == '.')
        {
            number_of_dots += 1;
        }
    }

    tokenizer[strlen(tokenizer) - 1] = '\0';
    if (number_of_dots > 1)
    {
        // we have a column

        char *db_name = strsep(&tokenizer, ".");
        char *tbl_name = strsep(&tokenizer, ".");
        char *col_name = strsep(&tokenizer, ".");

        if (!current_db || strcmp(current_db->name, db_name) != 0)
        {
            // should this check be done during parsing?
            // isn't that a bit unclean
            cs165_log(stdout, "Query unsupported. Bad db name\n");
            return NULL; // QUERY_UNSUPPORTED
        }

        // make sure table exists
        Table *table = lookup_table(current_db, tbl_name);
        if (!table)
        {
            cs165_log(stdout, "Query unsupported. Bad table name\n");
            return NULL;
        }

        // make sure column exists
        Column *column = lookup_column(table, col_name);
        if (!column)
        {
            cs165_log(stdout, "Query unsupported. Bad column name\n");
            return NULL;
        }

        // create the operator
        DbOperator *sum_op = malloc(sizeof(DbOperator));
        sum_op->type = SUM;
        sum_op->operator_fields.avg_operator.handler = handle;
        sum_op->operator_fields.avg_operator.type = COLUMN_O;
        sum_op->operator_fields.avg_operator.address = (EntityAddress){
            .db = current_db,
            .table = table,
            .col = column};

        return sum_op;
    }

    // check if variable exists
    Variable *result = find_var(tokenizer);
    if (!result)
    {
        cs165_log(stdout, "Query unsupported. Bad variable name\n");
        return NULL;
    }

    // create the operator
    DbOperator *sum_op = malloc(sizeof(DbOperator));
    sum_op->type = SUM;
    sum_op->operator_fields.avg_operator.handler = handle;
    sum_op->operator_fields.avg_operator.variable = result;
    sum_op->operator_fields.avg_operator.type = VARIABLE_O;

    return sum_op;
}

DbOperator *parse_add(char *handle, char *add_arg)
{
    char *tokenizer = add_arg;
    message_status status = OK_DONE;

    // remove parenthesis
    tokenizer++;

    char *var1 = next_token(&tokenizer, &status);
    char *var2 = next_token(&tokenizer, &status);
    var2[strlen(var2) - 1] = '\0';

    // may be check the case where you have more than one comma
    Variable *var_1 = find_var(var1);
    Variable *var_2 = find_var(var2);

    if (var_1 == NULL || var_2 == NULL)
    {
        cs165_log(stdout, "Query unsupported. Bad variable name\n");
        return NULL;
    }

    // create the operator
    DbOperator *math_operator = malloc(sizeof(DbOperator));
    math_operator->type = ADD;
    math_operator->operator_fields.math_operator.handler = handle;
    math_operator->operator_fields.math_operator.operand_1 = var_1;
    math_operator->operator_fields.math_operator.operand_2 = var_2;
    math_operator->operator_fields.math_operator.operation = ADD;

    return math_operator;
}

DbOperator *parse_sub(char *handle, char *add_arg)
{
    char *tokenizer = add_arg;
    message_status status = OK_DONE;

    // remove parenthesis
    tokenizer++;

    char *var1 = next_token(&tokenizer, &status);
    char *var2 = next_token(&tokenizer, &status);
    var2[strlen(var2) - 1] = '\0';

    // may be check the case where you have more than one comma
    Variable *var_1 = find_var(var1);
    Variable *var_2 = find_var(var2);

    if (var_1 == NULL || var_2 == NULL)
    {
        cs165_log(stdout, "Query unsupported. Bad variable name\n");
        return NULL;
    }

    // create the operator
    DbOperator *math_operator = malloc(sizeof(DbOperator));
    math_operator->type = SUB;
    math_operator->operator_fields.math_operator.handler = handle;
    math_operator->operator_fields.math_operator.operand_1 = var_1;
    math_operator->operator_fields.math_operator.operand_2 = var_2;
    math_operator->operator_fields.math_operator.operation = SUB;

    return math_operator;
}

DbOperator *parse_avg(char *handle, char *avg_arg)
{
    char *tokenizer = avg_arg;
    // message_status status = OK_DONE;

    // remove parenthesis
    tokenizer++;
    tokenizer[strlen(tokenizer) - 1] = '\0';

    // check if variable exists
    Variable *result = find_var(tokenizer);
    if (!result)
    {
        cs165_log(stdout, "Query unsupported. Bad variable name\n");
        return NULL;
    }

    // create the operator
    DbOperator *average_op = malloc(sizeof(DbOperator));
    average_op->type = AVG;
    average_op->operator_fields.avg_operator.handler = handle;
    average_op->operator_fields.avg_operator.variable = result;

    return average_op;
}

DbOperator *parse_min_max(char *handler, char *arguments, OperatorType type)
{

    char *token = arguments;
    token++;                         // remove the first '('
    token[strlen(token) - 1] = '\0'; // remove the last ')'

    Variable *var = find_var(token);
    if (var == NULL)
    {
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = type;
    dbo->operator_fields.min_max_operator.variable = var;
    dbo->operator_fields.min_max_operator.handler = handler;
    dbo->operator_fields.min_max_operator.operation = type;

    return dbo;
}

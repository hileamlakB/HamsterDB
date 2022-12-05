#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>
#include <Parser/parse.h>
#include <client_context.h>

// <vec_pos1_out>,<vec_pos2_out>=join(<vec_val1>,<vec_pos1>,<vec_val2>,<vec_pos2>, [hash,nested-loop,...])
DbOperator *parse_join(char *handle, char *join_args)
{
    char *handle_1 = strsep(&handle, ",");
    char *handle_2 = handle;

    // remove parenthesis
    join_args++;

    char *vec_val_1 = strsep(&join_args, ",");
    char *vec_pos_1 = strsep(&join_args, ",");

    char *vec_val_2 = strsep(&join_args, ",");
    char *vec_pos_2 = strsep(&join_args, ",");

    // remove parenthesis
    char *j_type = strsep(&join_args, ")");

    Variable *val1 = find_var(vec_val_1),
             *val2 = find_var(vec_val_2),
             *pos1 = find_var(vec_pos_1),
             *pos2 = find_var(vec_pos_2);

    join_type type = NESTED_LOOP;
    if (strcmp(j_type, "hash") == 0)
    {
        type = HASH_JOIN;
    }

    if (!val1 || !val2 || !pos1 || !pos2)
    {
        cs165_log(stdout, "-- Query unsupported. Bad variable name\n");
        return NULL;
    }

    DbOperator *join_op = malloc(sizeof(DbOperator));
    join_op->type = JOIN;

    join_op->operator_fields.join_operator.type = type;
    join_op->operator_fields.join_operator.handler1 = strdup(handle_1);
    join_op->operator_fields.join_operator.handler2 = strdup(handle_2);

    join_op->operator_fields.join_operator.val1 = val1;
    join_op->operator_fields.join_operator.val2 = val2;

    join_op->operator_fields.join_operator.pos1 = pos1;
    join_op->operator_fields.join_operator.pos2 = pos2;

    return join_op;
}


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

// <vec_pos>=select(<posn_vec>,<val_vec>,<low>,<high>)
// uses preselected position
DbOperator *parse_select_pos(char *handle, char *select_argument)
{
    char *tokenizer = select_argument;
    message_status status = OK_DONE;
    char *pos_vec_name = next_token(&tokenizer, &status);
    char *val_vec_name = next_token(&tokenizer, &status);
    char *low = next_token(&tokenizer, &status);
    char *high = next_token(&tokenizer, &status);

    if (status != OK_DONE)
    {
        return NULL;
    }

    Variable *pos_vec = find_var(pos_vec_name);
    Variable *val_vec = find_var(val_vec_name);

    if (!pos_vec)
    {
        pos_vec = malloc(sizeof(Variable));
        pos_vec->name = strdup(pos_vec_name);
        pos_vec->exists = false;
    }

    if (!val_vec)
    {
        val_vec = malloc(sizeof(Variable));
        // make sure val_vec doesn't fail
        val_vec->name = strdup(val_vec_name);
        val_vec->exists = false;
    }

    // if ((pos_vec == NULL || val_vec == NULL) && !batch.mode)
    // {
    //     cs165_log(stdout, "Variable not found");
    //     return NULL;
    // }

    // create the operator
    DbOperator *select_op = malloc(sizeof(DbOperator));

    select_op->type = SELECT;
    select_op->operator_fields.select_operator.handler = strdup(handle);

    if (strncmp("null", low, 4) == 0)
    {
        select_op->operator_fields.select_operator.low = NULL;
    }
    else
    {
        select_op->operator_fields.select_operator.low = malloc(sizeof(int));
        *select_op->operator_fields.select_operator.low = atoi(low);
    }

    if (strncmp("null", high, 4) == 0)
    {
        select_op->operator_fields.select_operator.high = NULL;
    }
    else
    {
        select_op->operator_fields.select_operator.high = malloc(sizeof(int));
        *select_op->operator_fields.select_operator.high = atoi(high);
    }

    select_op->operator_fields.select_operator.type = SELECT_POS;
    select_op->operator_fields.select_operator.pos_vec = pos_vec;
    select_op->operator_fields.select_operator.val_vec = val_vec;

    return select_op;
}

/**
 * Takes a pointer to a string.
 * returns the equivalent select opertor
 * takes an input of the format
 * select(db1.tbl1.col1,null,20)
 **/
DbOperator *parse_select(char *handle, char *select_argument)
{

    message_status status = OK_DONE;

    char *tokenizer = select_argument;
    // remove parenthesis
    tokenizer++;

    // determine which version of select is being used
    int number_of_commas = 0;
    for (int i = 0; select_argument[i]; i++)
    {
        if (tokenizer[i] == ',')
        {
            number_of_commas++;
        }
        if (number_of_commas > 2)
        {
            break;
        }
    }

    if (number_of_commas > 2)
    {
        return parse_select_pos(handle, tokenizer);
    }

    char *name = next_token(&tokenizer, &status);
    char *db_name = strsep(&name, ".");
    char *tbl_name = strsep(&name, ".");
    char *col_name = strsep(&name, ".");
    char *low = next_token(&tokenizer, &status);
    char *high = next_token(&tokenizer, &status);
    if (status != OK_DONE)
    {
        return NULL;
    }

    // check that the database argument is the current active database
    if (!current_db)
    {
        // load active database
        load_db(db_name);
    }

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
    DbOperator *select_op = malloc(sizeof(DbOperator));

    select_op->type = SELECT;
    select_op->operator_fields.select_operator.handler = strdup(handle);

    if (strncmp("null", low, 4) == 0)
    {
        select_op->operator_fields.select_operator.low = NULL;
    }
    else
    {
        select_op->operator_fields.select_operator.low = malloc(sizeof(int));
        *select_op->operator_fields.select_operator.low = atoi(low);
    }

    if (strncmp("null", high, 4) == 0)
    {
        select_op->operator_fields.select_operator.high = NULL;
    }
    else
    {
        select_op->operator_fields.select_operator.high = malloc(sizeof(int));
        *select_op->operator_fields.select_operator.high = atoi(high);
    }

    select_op->operator_fields.select_operator.type = SELECT_COL;
    select_op->operator_fields.select_operator.table = table;
    select_op->operator_fields.select_operator.column = column;

    return select_op;
}

// <vec_val>=fetch(<col_var>,<vec_pos>)
DbOperator *parse_fetch(char *handle, char *fetch_arguments)
{
    char *tokenizer = fetch_arguments;
    message_status status = OK_DONE;

    // remove parenthesis
    tokenizer++;
    char *name = next_token(&tokenizer, &status);
    char *db_name = strsep(&name, ".");
    char *tbl_name = strsep(&name, ".");
    char *col_name = strsep(&name, ".");
    char *pos_name = next_token(&tokenizer, &status);

    // remove parenthesis
    pos_name[strlen(pos_name) - 1] = '\0';

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

    // check if variable exists
    Variable *result = find_var(pos_name);
    if (!result)
    {
        result = malloc(sizeof(Variable));
        // handle the case this fails
        result->name = strdup(pos_name);
        result->exists = false;
    }

    // create the operator
    DbOperator *fetch_op = malloc(sizeof(DbOperator));
    fetch_op->type = FETCH;
    fetch_op->operator_fields.fetch_operator.handler = strdup(handle);
    fetch_op->operator_fields.fetch_operator.table = table;
    fetch_op->operator_fields.fetch_operator.column = column;
    fetch_op->operator_fields.fetch_operator.variable = result;

    return fetch_op;
}

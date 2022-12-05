#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>
#include <cs165_api.h>

#include <Parser/parse.h>

DbOperator *parse_create_col(char *create_arguments)
{
    message_status status = OK_DONE;
    char **create_arguments_index = &create_arguments;
    char *col_name = next_token(create_arguments_index, &status);
    char *db_table_name = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT)
    {
        return NULL;
    }
    // Get the table name free of quotation marks
    col_name = trim_quotes(col_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(db_table_name) - 1;
    if (db_table_name[last_char] != ')')
    {
        return NULL;
    }
    // replace the ')' with a null terminating character.
    db_table_name[last_char] = '\0';

    // split the db_name and the table_name
    char *db_name = strsep(&db_table_name, ".");
    char *table_name = db_table_name;

    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0)
    {
        // should this check be done during parsing?
        // isn't that a bit unclean
        cs165_log(stdout, "Query unsupported. Bad db name\n");
        return NULL; // QUERY_UNSUPPORTED
    }

    // make sure table exists
    Table *table = lookup_table(current_db, table_name);
    if (!table)
    {
        cs165_log(stdout, "Query unsupported. Bad table name\n");
        return NULL;
    }

    // make create dbo for table
    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _COLUMN;
    strcpy(dbo->operator_fields.create_operator.name, col_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.table = table;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/

DbOperator *parse_create_db(char *create_arguments)
{
    char *token = strsep(&create_arguments, ",");

    // not enough arguments if token is NULL
    if (token == NULL)
    {
        return NULL;
    }
    else
    {
        // create the database with given name
        char *db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')')
        {
            return NULL;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL)
        {
            return NULL;
        }
        // make create operator.
        DbOperator *dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
        dbo->operator_fields.create_operator.create_type = _DB;
        strcpy(dbo->operator_fields.create_operator.name, db_name);
        return dbo;
    }
}

DbOperator *parse_create_idx(char *create_arguments)
{
    // extract db, table, and column names
    char *db_name = strsep(&create_arguments, ".");
    char *table_name = strsep(&create_arguments, ".");
    char *col_name = strsep(&create_arguments, ",");

    char *idx_type = strsep(&create_arguments, ",");
    char *cluster_type = strsep(&create_arguments, ",");

    if (!current_db && strcmp(current_db->name, db_name) != 0)
    {
        return NULL;
    }
    // look up the table and column
    Table *table = lookup_table(current_db, table_name);
    Column *col = lookup_column(table, col_name);

    if (!table || !col)
    {
        return NULL;
    }

    // make create operator.
    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _INDEX;
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.table = table;
    dbo->operator_fields.create_operator.column = col;

    if (strcmp(idx_type, "sorted") == 0)
    {
        dbo->operator_fields.create_operator.index_type = SORTED;
    }
    else if (strcmp(idx_type, "btree") == 0)
    {
        dbo->operator_fields.create_operator.index_type = BTREE;
    }
    else
    {
        dbo->operator_fields.create_operator.index_type = NO_INDEX;
    }

    if (strncmp(cluster_type, "clustered", 9) == 0)
    {
        dbo->operator_fields.create_operator.cluster_type = CLUSTERED;
    }
    else if (strncmp(cluster_type, "unclustered", 11) == 0)
    {
        dbo->operator_fields.create_operator.cluster_type = UNCLUSTERED;
    }
    else
    {
        dbo->operator_fields.create_operator.cluster_type = NO_CLUSTER;
    }

    return dbo;
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator *parse_create(char *create_arguments)
{
    message_status mes_status = INITIAL;
    DbOperator *dbo = NULL;
    char *token = create_arguments;

    // remove the first '('
    token++;

    char *type = next_token(&token, &mes_status);

    // pass off to next parse function.
    if (strcmp(type, "db") == 0)
    {
        dbo = parse_create_db(token);
    }
    else if (strcmp(type, "tbl") == 0)
    {
        dbo = parse_create_tbl(token);
    }
    else if (strcmp(type, "col") == 0)
    {
        dbo = parse_create_col(token);
    }
    else if (strcmp(type, "idx") == 0)
    {
        dbo = parse_create_idx(token);
    }
    else
    {
        mes_status = UNKNOWN_COMMAND;
    }

    return dbo;
}

DbOperator *parse_create_tbl(char *create_arguments)
{
    message_status status = OK_DONE;
    char **create_arguments_index = &create_arguments;
    char *table_name = next_token(create_arguments_index, &status);
    char *db_name = next_token(create_arguments_index, &status);
    char *col_cnt = next_token(create_arguments_index, &status);

    if (!current_db)
    {
        load_db(db_name);
    }

    // not enough arguments
    if (status == INCORRECT_FORMAT)
    {
        return NULL;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')')
    {
        return NULL;
    }
    // replace the ')' with a null terminating character.
    col_cnt[last_char] = '\0';
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0)
    {
        cs165_log(stdout, "-- Query unsupported. Bad db name\n");
        return NULL; // QUERY_UNSUPPORTED
    }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1)
    {
        return NULL;
    }
    // make create dbo for table
    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _TABLE;
    strcpy(dbo->operator_fields.create_operator.name, table_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.col_count = column_cnt;
    return dbo;
}

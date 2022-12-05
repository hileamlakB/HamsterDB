#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>
#include <Parser/parse.h>
#include <client_context.h>

DbOperator *parse_load_start(char *token, message *send_message)
{

    // remove parenthesis
    token++;
    token[strlen(token) - 1] = '\0';

    // only the table name matters assuing the columns are in the same order
    size_t incoming_size = _atoi(strsep(&token, ","));
    char *name = strsep(&token, ",");
    Status status;
    EntityAddress address = parse_column_name(name, &status);

    send_message->status = OK_DONE;

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = BATCH_LOAD_START;
    dbo->operator_fields.parellel_load.load_size = incoming_size;
    bload.mode = true;
    bload.table = address.table;

    return dbo;
}

DbOperator *parse_load_parallel(String query_command, message *send_message)
{
    if (strncmp(query_command.str, "load_end", 8) == 0)
    {
        DbOperator *dbo = malloc(sizeof(DbOperator));
        dbo->type = BATCH_LOAD_END;
        bload.mode = false;

        send_message->status = OK_DONE;
        return dbo;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = BATCH_LOAD;
    dbo->operator_fields.parellel_load.data = query_command;

    send_message->status = OK_DONE;

    return dbo;
}

DbOperator *parse_load(char *query_command, message *send_message)
{
    char *token = strsep(&query_command, ",");
    Status parse_status;
    // remove the first bracket
    token += 1;

    if (!current_db)
    {
        send_message->status = DATABASE_NOT_SELECTED;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = LOAD;

    if (strncmp("complete", token, 8) == 0)
    {
        dbo->operator_fields.load_operator.complete = true;
        token += 9;

        dbo->operator_fields.load_operator.address = parse_column_name(token, &parse_status);
        return dbo;
    }

    EntityAddress address = parse_column_name(token, &parse_status);

    if (parse_status.code != OK)
    {
        send_message->status = INCORRECT_FILE_FORMAT;
        free(dbo);
        return NULL;
    }

    dbo->operator_fields.load_operator.complete = false;
    dbo->operator_fields.load_operator.address = address;
    dbo->operator_fields.load_operator.size = atoi(strsep(&query_command, ","));
    dbo->operator_fields.load_operator.data = query_command;

    return dbo;
}

/**
 * parse_insert reads in the arguments for a create statement and
 * then passes these arguments to a database function to insert a row.
 **/
DbOperator *parse_insert(char *query_command, message *send_message)
{

    // remove leading '(')
    query_command++;

    // parse table input
    char *db = strsep(&query_command, ".");
    if (db == NULL || !current_db || strcmp(current_db->name, db) != 0)
    {
        send_message->status = INCORRECT_FILE_FORMAT;
        return NULL;
    }

    char *table_name = strsep(&query_command, ",");
    // lookup the table and make sure it exists.
    Table *insert_table = lookup_table(current_db, table_name);
    if (insert_table == NULL)
    {
        send_message->status = OBJECT_NOT_FOUND;
        return NULL;
    }
    // make insert operator.
    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = INSERT;
    dbo->operator_fields.insert_operator.table = insert_table;
    dbo->operator_fields.insert_operator.value = query_command;
    return dbo;
}

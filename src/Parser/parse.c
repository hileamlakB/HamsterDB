/*
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>

#include "cs165_api.h"
#include "parse.h"
#include "Utils/utils.h"
#include "Loader/load.h"
#include <cs165_api.h>
#include <Parser/parse.h>

EntityAddress parse_column_name(char *token, Status *status)
{
    EntityAddress address = {.db = NULL, .table = NULL, .col = NULL};
    status->code = OK;
    char *db_name = strsep(&token, ".");
    char *table_name = strsep(&token, ".");
    char *column_name = strsep(&token, ".");

    // Get the table name free of quotation marks
    db_name = trim_quotes(db_name);
    table_name = trim_quotes(table_name);
    column_name = trim_quotes(column_name);

    // check what the end value of a column_name is

    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0)
    {
        load_db(db_name);
    }
    address.db = current_db;

    // make sure table exists
    Table *table = lookup_table(current_db, table_name);
    if (!table)
    {
        status->code = ERROR;
        status->error_message = "Query unsupported. Bad table name\n";
        return address;
    }
    address.table = table;

    // make sure column exists
    Column *column = lookup_column(table, column_name);
    if (!column)
    {
        status->code = ERROR;
        status->error_message = "Query unsupported. Bad column name\n";
        return address;
    }

    address.col = column;

    return address;
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
DbOperator *parse_command(char *query_command, message *send_message)
{

    DbOperator *dbo = NULL;

    if (strncmp(query_command, "--", 2) == 0)
    {
        send_message->status = OK_DONE;
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL)
    {
        // handle exists, store here.
        *equals_pointer = '\0';
        query_command = ++equals_pointer;
    }
    else
    {
        handle = NULL;
    }

    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);
    // check what command is given.
    if (strncmp(query_command, "create", 6) == 0)
    {
        query_command += 6;
        dbo = parse_create(query_command);
        if (dbo == NULL)
        {
            send_message->status = INCORRECT_FORMAT;
        }
        else
        {
            send_message->status = OK_DONE;
        }
    }
    else if (strncmp(query_command, "relational_insert", 17) == 0)
    {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    }
    else if (strncmp(query_command, "load_start", 10) == 0)
    {
        query_command += 10;
        dbo = parse_load_start(query_command, send_message);
        free(query_command - 10);
    }
    else if (strncmp(query_command, "load", 4) == 0)
    {
        query_command += 4;
        dbo = parse_load(query_command, send_message);
    }
    else if (strncmp(query_command, "use", 3) == 0)
    {
    }
    else if (strncmp(query_command, "fetch", 5) == 0)
    {
        query_command += 5;
        dbo = parse_fetch(handle, query_command);
    }
    else if (strncmp(query_command, "shutdown", 8) == 0)
    {

        dbo = malloc(sizeof(DbOperator));
        dbo->type = SHUTDOWN;
        free(query_command);
    }
    else if (strncmp(query_command, "select", 6) == 0)
    {
        query_command += 6;
        dbo = parse_select(handle, query_command);
    }
    else if (strncmp(query_command, "add", 3) == 0)
    {
        query_command += 3;
        dbo = parse_add(handle, query_command);
    }
    else if (strncmp(query_command, "sub", 3) == 0)
    {
        query_command += 3;
        dbo = parse_sub(handle, query_command);
    }
    else if (strncmp(query_command, "avg", 3) == 0)
    {
        query_command += 3;
        dbo = parse_avg(handle, query_command);
    }
    else if (strncmp(query_command, "sum", 3) == 0)
    {
        query_command += 3;
        dbo = parse_sum(handle, query_command);
    }
    else if (strncmp(query_command, "min", 3) == 0)
    {
        query_command += 3;
        dbo = parse_min_max(handle, query_command, MIN);
    }
    else if (strncmp(query_command, "max", 3) == 0)
    {
        query_command += 3;
        dbo = parse_min_max(handle, query_command, MAX);
    }
    else if (strncmp(query_command, "print", 5) == 0)
    {
        query_command += 5;
        dbo = parse_print(query_command);
    }
    else if (strncmp(query_command, "batch_queries", 13) == 0)
    {
        query_command += 13;
        dbo = parse_batch_query(query_command, send_message);
    }
    else if (strncmp(query_command, "batch_execute", 13) == 0)
    {
        query_command += 13;
        dbo = parse_batch_execute(query_command, send_message);
    }
    else if (strncmp(query_command, "join", 4) == 0)
    {
        query_command += 4;
        dbo = parse_join(handle, query_command);
    }
    else if (strncmp(query_command, "timer", 5) == 0)
    {
        query_command += 5;
        dbo = parse_timer(query_command);
    }

    return dbo;
}

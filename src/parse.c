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
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char *next_token(char **tokenizer, message_status *status)
{
    char *token = strsep(tokenizer, ",");
    if (token == NULL)
    {
        *status = INCORRECT_FORMAT;
    }
    return token;
}

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

    if (pos_vec == NULL || val_vec == NULL)
    {
        cs165_log(stdout, "Variable not found");
        return NULL;
    }

    // create the operator
    DbOperator *select_op = malloc(sizeof(DbOperator));

    select_op->type = SELECT;
    select_op->operator_fields.select_operator.handler = handle;

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
        return parse_select_pos(handle, select_argument);
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
    else if (strcmp(current_db->name, db_name) != 0)
    {
        // should this check be done during parsing?
        // isn't that a bit unclean
        cs165_log(stdout, "Query unsupported. Bad db name\n\n");
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
    select_op->operator_fields.select_operator.handler = handle;

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

DbOperator *parse_print(char *print_argument)
{
    char *tokenizer = print_argument;
    message_status status = OK_DONE;

    // remove parenthesis
    tokenizer++;

    int number_of_commas = 0;
    for (int i = 0; tokenizer[i]; i++)
    {
        if (tokenizer[i] == ',')
        {
            number_of_commas++;
        }
    }

    // rremove trailing parenthesis
    tokenizer[strlen(tokenizer) - 1] = '\0';

    Variable **result_tupls = malloc(sizeof(int *) * (number_of_commas + 1));

    int i = 0;
    int max_row = 0;
    while (i <= number_of_commas)
    {
        char *var = next_token(&tokenizer, &status);
        Variable *var_location = find_var(var);
        if (var_location == NULL)
        {
            cs165_log(stdout, "Query unsupported. Bad variable name");
            return NULL;
        }
        result_tupls[i] = var_location;
        max_row = var_location->result.size > max_row ? var_location->result.size : max_row;
        i++;
    }

    DbOperator *print_op = malloc(sizeof(DbOperator));
    print_op->type = PRINT;
    if (number_of_commas == 0 && result_tupls[0]->type == FLOAT_VALUE)
    {

        print_op->operator_fields.print_operator.type = SINGLE_FLOAT;
        print_op->operator_fields.print_operator.data.value = result_tupls[0]->result.value;
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
    for (int i = 0; pos_name[i]; i++)
    {
        if (pos_name[i] == ')')
        {
            pos_name[i] = '\0';
        }
    }

    if (strcmp(current_db->name, db_name) != 0)
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
        cs165_log(stdout, "Query unsupported. Bad variable name");
        return NULL;
    }

    // create the operator
    DbOperator *fetch_op = malloc(sizeof(DbOperator));
    fetch_op->type = FETCH;
    fetch_op->operator_fields.fetch_operator.handler = handle;
    fetch_op->operator_fields.fetch_operator.table = table;
    fetch_op->operator_fields.fetch_operator.column = column;
    fetch_op->operator_fields.fetch_operator.variable = result;

    return fetch_op;
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
        cs165_log(stdout, "Query unsupported. Bad variable name");
        return NULL;
    }

    // create the operator
    DbOperator *average_op = malloc(sizeof(DbOperator));
    average_op->type = AVG;
    average_op->operator_fields.avg_operator.handler = handle;
    average_op->operator_fields.avg_operator.variable = result;

    return average_op;
}

DbOperator *parse_sum(char *handle, char *sum_arg)
{
    char *tokenizer = sum_arg;
    // message_status status = OK_DONE;

    // remove parenthesis
    tokenizer++;
    tokenizer[strlen(tokenizer) - 1] = '\0';

    // check if variable exists
    Variable *result = find_var(tokenizer);
    if (!result)
    {
        cs165_log(stdout, "Query unsupported. Bad variable name");
        return NULL;
    }

    // create the operator
    DbOperator *average_op = malloc(sizeof(DbOperator));
    average_op->type = SUM;
    average_op->operator_fields.avg_operator.handler = handle;
    average_op->operator_fields.avg_operator.variable = result;

    return average_op;
}

DbOperator *parse_add(char *handle, char *add_arg)
{
    char *tokenizer = add_arg;
    message_status status = OK_DONE;

    // remove parenthesis
    tokenizer++;

    char *var1 = next_token(&tokenizer, &status);
    char *var2 = next_token(&tokenizer, &status);

    // may be check the case where you have more than one comma
    Variable *var_1 = find_var(var1);
    Variable *var_2 = find_var(var2);

    if (var_1 == NULL || var_2 == NULL)
    {
        cs165_log(stdout, "Query unsupported. Bad variable name");
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

    // may be check the case where you have more than one comma
    Variable *var_1 = find_var(var1);
    Variable *var_2 = find_var(var2);

    if (var_1 == NULL || var_2 == NULL)
    {
        cs165_log(stdout, "Query unsupported. Bad variable name");
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

// /**
//  * Takes a pointer to a string.
//  * returns the equivalent insert opertor
//  * takes an input of the format
//  * insert(db1.tbl1.col1,10)
//  **/
// DbOperator *parse_insert(char *create_arguments)
// {
//     message_status status = OK_DONE;
//     char *tokenizer = create_arguments;
//     char *db_name = next_token(&tokenizer, &status);
//     char *tbl_name = next_token(&tokenizer, &status);
//     char *col_name = next_token(&tokenizer, &status);
//     char *value = next_token(&tokenizer, &status);
//     if (status != OK_DONE)
//     {
//         return NULL;
//     }
// }

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/

DbOperator *parse_create_tbl(char *create_arguments)
{
    message_status status = OK_DONE;
    char **create_arguments_index = &create_arguments;
    char *table_name = next_token(create_arguments_index, &status);
    char *db_name = next_token(create_arguments_index, &status);
    char *col_cnt = next_token(create_arguments_index, &status);

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
        cs165_log(stdout, "Query unsupported. Bad db name\n");
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
    char *token;
    token = strsep(&create_arguments, ",");
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

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator *parse_create(char *create_arguments)
{
    message_status mes_status = INITIAL;
    DbOperator *dbo = NULL;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input.
    tokenizer_copy = to_free = malloc((strlen(create_arguments) + 1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create.
    if (strncmp(tokenizer_copy, "(", 1) == 0)
    {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT)
        {
            return NULL;
        }
        else
        {
            // pass off to next parse function.
            if (strcmp(token, "db") == 0)
            {
                dbo = parse_create_db(tokenizer_copy);
            }
            else if (strcmp(token, "tbl") == 0)
            {
                dbo = parse_create_tbl(tokenizer_copy);
            }
            else if (strcmp(token, "col") == 0)
            {
                dbo = parse_create_col(tokenizer_copy);
            }
            else
            {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    }
    else
    {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return dbo;
}

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

        status->code = ERROR;
        status->error_message = "Query unsupported. Bad db name\n";
        return address;
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

DbOperator *parse_load(char *query_command, message *send_message)
{
    char *token = strsep(&query_command, ",");
    Status parse_status;
    // remove the first bracket
    token += 1;

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
    unsigned int columns_inserted = 0;
    char *token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0)
    {
        query_command++;
        char **command_index = &query_command;
        // parse table input
        char *table_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT)
        {
            return NULL;
        }
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
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        // parse inputs until we reach the end. Turn each given string into an integer.
        while ((token = strsep(command_index, ",")) != NULL)
        {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count)
        {
            send_message->status = INCORRECT_FORMAT;
            free(dbo);
            return NULL;
        }
        return dbo;
    }
    else
    {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 *
 * Getting Started Hint:
 *      What commands are currently supported for parsing in the starter code distribution?
 *      How would you add a new command type to parse?
 *      What if such command requires multiple arguments?
 **/
DbOperator *parse_command(char *query_command, message *send_message, int client_socket, ClientContext *context)
{
    // a second option is to malloc the dbo here (instead of inside the parse commands). Either way, you should track the dbo
    // and free it when the variable is no longer needed.
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator));

    if (strncmp(query_command, "--", 2) == 0)
    {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL)
    {
        // handle exists, store here.
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    }
    else
    {
        handle = NULL;
    }

    // by default, set the status to acknowledge receipt of command,
    //   indication to client to now wait for the response from the server.
    //   Note, some commands might want to relay a different status back to the client.
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
    }
    else if (strncmp(query_command, "max", 3) == 0)
    {
    }
    else if (strncmp(query_command, "print", 5) == 0)
    {
        query_command += 5;
        dbo = parse_print(query_command);
        dbo->type = PRINT;
    }

    // I suppose this is a place to support more commands.

    if (dbo == NULL)
    {
        return dbo;
    }

    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}

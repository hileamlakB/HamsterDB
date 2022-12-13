#define _DEFAULT_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>
#include <sys/mman.h>
#include <errno.h>

#include "cs165_api.h"
#include <unistd.h>
#include <Serializer/serialize.h>
#include <Create/create.h>

Table empty_table;
/*
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table *create_table(Db *db, const char *name, size_t num_columns)
{

    // default status to error and improve it to
    // success if everything goes well

    if (db == NULL)
    {
        log_info("-- DB not found");
        return NULL;
    }

    if (lookup_table(db, (char *)name))
    {
        log_info("-- Table already exists");
        return NULL;
    }

    Table table = empty_table;

    memcpy(table.name, name, MAX_SIZE_NAME);
    char *file_path = catnstr(4, "dbdir/", db->name, ".", name);

    strcpy(table.file_path, file_path);
    table.col_count = num_columns;
    table.table_length = 0;

    // you can support flexible number of columns later
    // the same way you did for tables
    table.columns = calloc(num_columns, sizeof(Column));

    if (db->tables_capacity == db->tables_size)
    {
        // double the size of the array
        db->tables_capacity += 1;
        db->tables_capacity *= 2;
        Table *new_tables = realloc(db->tables, db->tables_capacity * sizeof(Table));
        db->tables = new_tables;
    }

    db->tables[db->tables_size] = table;
    db->tables_size++;

    // success
    free(file_path);

    return &db->tables[db->tables_size - 1];
}

/*
 * Similarly, this method is meant to create a database.
 */
Status create_db(const char *db_name)
{

    mkdir("dbdir", 0777);

    char *file_path = catnstr(2, "dbdir/", db_name);
    // makesure db doesn't already exist
    if (access(file_path, F_OK) == 0)
    {
        // load if it already exists
        load_db(db_name);
    }

    Db *active_db = (Db *)malloc(sizeof(Db));

    memcpy(active_db->name, db_name, MAX_SIZE_NAME);
    strcpy(active_db->file_path, file_path);

    active_db->tables = NULL;
    active_db->tables_size = 0;
    active_db->tables_capacity = 0;

    // right now there is only one active database
    // you might expand this in the furture with your old code
    assert(current_db == NULL);
    current_db = active_db;

    free(file_path);

    return (Status){.code = OK};
}

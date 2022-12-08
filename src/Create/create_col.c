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

Column *create_column(Table *table, char *name, Status *ret_status)
{
    ret_status->code = ERROR;

    if (!table)
    {
        ret_status->error_message = "Table doesn't exist";
        return NULL;
    }
    if (lookup_column(table, name))
    {
        ret_status->error_message = "Column already exists";
        return NULL;
    }

    Column column = empty_column;

    strcpy(column.name, name);
    char *file_path = catnstr(6, "dbdir/", current_db->name, ".", table->name, ".", name);
    strcpy(column.file_path, file_path);

    if (table->col_count <= table->table_length)
    {
        free(file_path);
        ret_status->error_message = "Table is full";
        return NULL;
    }

    // number of columns is fixed
    table->columns[table->table_length] = column;
    table->table_length++;

    // success
    ret_status->code = OK;

    free(file_path);
    return &table->columns[table->table_length - 1];
}

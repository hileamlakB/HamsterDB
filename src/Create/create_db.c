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

int load_db(const char *db_name)
{
    assert(current_db == NULL);

    // make sure the db folder exists
    // if does load db, table, and column

    char *file_name = catnstr(2, "dbdir/", db_name);

    if (access(file_name, F_OK) == -1)
    {
        free(file_name);
        log_info("-- DB not found");
        return -1;
    }

    current_db = malloc(sizeof(Db));
    *current_db = deserialize_db(file_name);

    free(file_name);
    return 0;
}

void free_db()
{
    for (size_t i = 0; i < current_db->tables_size; i++)
    {
        free(current_db->tables[i].columns);
    }
    free(current_db->tables);
    free(current_db);
}
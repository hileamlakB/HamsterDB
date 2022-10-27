#include "cs165_api.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

// opens the file asscoiated with the table, table name in the db
// database and returns a pointer to the table
int load_table(Db *db, char *table_name)
{
    char *file_name = catnstr(4, "dbdir/", db->name, ".", table_name);
    return open(file_name, O_RDONLY);
}

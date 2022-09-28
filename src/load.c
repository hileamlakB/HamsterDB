#include "cs165_api.h"

// opens the file asscoiated with the table, table name in the db
// database and returns a pointer to the table
FILE *load_table(Db *db, char *table_name)
{
    char *file_name = catnstr(3, db->name, ".", table_name);
    return fopen(file_name, "r");
}

// loads a column from a column file
// and returns a pointer to the column
FILE *load_column(Db *db, Table *table, char *column_name)
{
    char *file_name = catnstr(4, db->name, ".", table->name, ".", column_name);
    return fopen(file_name, "r");
}

int write_db();
int write_table();
int write_column();

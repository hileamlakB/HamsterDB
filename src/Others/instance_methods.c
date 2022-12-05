#include <cs165_api.h>
#include <Serializer/serialize.h>

size_t writing_space(Column *column)
{
    return column->file_size - (column->end + sizeof(ColumnMetaData));
}

#include <cs165_api.h>
#include <string.h>

Table *lookup_table(Db *db, char *name)
{

    Table *tbls = db->tables;
    //  consider using some hash tables to store table names here
    for (size_t i = 0; i < db->tables_size; i++)
    {
        if (strcmp(tbls[i].name, name) == 0)
        {
            return &tbls[i];
        }
    }
    return NULL;
}

Column *lookup_column(Table *table, char *name)
{

    Column *columns = table->columns;
    //  consider using some hash tables to store table names here
    for (size_t i = 0; i < table->col_count; i++)
    {
        if (strcmp(columns[i].name, name) == 0)
        {
            return &columns[i];
        }
    }
    return NULL;
}
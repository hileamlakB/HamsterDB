#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>

#include <unistd.h>
#include <assert.h>
#include "utils.h"
#include "cs165_api.h"

// common serilizers

// generic_serializer, takes in a copy function and a source,
// and returns a serialized version of the source using the copy function
char *generic_serializer(int (*cpfunc)(char *, size_t, unsigned char[4], void *), void *source)
{

    // size of metadata
    union
    {
        unsigned int integer;
        unsigned char byte[4]; // string representtion of metadata size
    } intbyte;

    int max_size = sysconf(_SC_PAGESIZE);

    // change metadata to string
    char *metadata = (char *)calloc(sizeof(char), max_size);
    if (!metadata)
    {
        // consider having state variable here to indicate error
        return NULL;
    }

    // The format of the meta data is stirng is as follows:
    // "number_metada_pages.name.count.min_set.min.max_set.max.sum_set.sum"
    intbyte.integer = 1;
    int printed = (*cpfunc)(metadata, max_size, intbyte.byte, source);

    if (printed > max_size)
    {

        int npages = (printed / max_size) + 1;
        intbyte.integer = npages;

        max_size = npages * sysconf(_SC_PAGESIZE);
        char *new_meta_data = (char *)realloc(metadata, sizeof(char) * max_size);
        if (!new_meta_data)
        {
            free(metadata);
            // consider having state variable here to indicate error
            return NULL;
        }
        metadata = new_meta_data;
        printed = cpfunc(metadata, max_size, intbyte.byte, source);
    }

    // pad the remaining space with spaces
    if (printed < max_size)
    {
        for (int i = printed - 1; i < max_size - 1; i++)
        {
            metadata[i] = ' ';
        }
        metadata[max_size - 1] = '\0';
        // consider having state variable here to indicate error
    }
    return metadata;
}

// generic_deserializer, takes in a File pointer and destination and the copy the file into
// the appropriate data time through the copy function
void generic_deserializer(void (*cpfunc)(void *, char *, Status *), FILE *file, void *dest, Status *status)
{
    // keep track of important activies incase of failure
    retrack_props props = {
        .to_free = NULL,
        .outside = NULL,
        .to_remove = NULL,
        .to_close = NULL,
    };

    int initial_offset = ftell(file);
    fseek(file, 0, SEEK_SET);

    // get the number of pages in the metadata
    union
    {
        unsigned int integer;
        unsigned char byte[4];
    } intbyte;

    // handle every possible failure of fread
    size_t read = fread(intbyte.byte, sizeof(unsigned char), 4, file);
    if (read != 4)
    {
        fseek(file, initial_offset, SEEK_SET);
        status->code = ERROR;
        return;
    }

    int meta_size = intbyte.integer * sysconf(_SC_PAGESIZE);

    // get the metadata
    char *metadata = (char *)calloc(sizeof(char), meta_size);
    if (!metadata || prepend(&props.to_free, metadata) != 0)
    {
        status->code = ERROR;
        retrack(props, "Deserialization failed");
        fseek(file, initial_offset, SEEK_SET);
        // consider having state variable here to indicate error
        return;
    }

    fread(metadata, sizeof(char), meta_size, file);

    (*cpfunc)(dest, metadata, status);

    fseek(file, initial_offset, SEEK_SET);
    clean_up(props.to_free);
}

// column serializer

// cpcol - copies  a column data into a string and returns the number of bytes copied
// the copy format is
// "pages_used.name.count.min_set.min.max_set.max.sum_set.sum"
// this function is a helper function fo the serialize column function
int cpcol(char *dst, size_t mx_size, unsigned char npages[4], void *source)
{

    Column *column = (Column *)source;

    return snprintf(dst, mx_size, "%u%u%u%u.%s.%d.%d.%d.%d.%d.%d.%d",
                    npages[0], npages[1], npages[2], npages[3],
                    column->name,
                    column->count,
                    column->min[0],
                    column->min[1],
                    column->max[0],
                    column->max[1],
                    column->sum[0],
                    column->sum[1]);
}

// cp2col - does the opposit of cpcol
void cp2col(void *dest, char *metadata, Status *status)
{
    (void)status;

    Column *column = (Column *)dest;
    assert(column != NULL);

    // if there is any error in any of these files
    // it is a result of an outisde editing of the db file
    // this isn't handled here
    // get the name
    strcpy(column->name, strsep(&metadata, "."));

    column->count = atoi(strsep(&metadata, "."));
    column->min[0] = atoi(strsep(&metadata, "."));
    column->min[1] = atoi(strsep(&metadata, "."));
    column->max[0] = atoi(strsep(&metadata, "."));
    column->max[1] = atoi(strsep(&metadata, "."));
    column->sum[0] = atoi(strsep(&metadata, "."));
    column->sum[1] = atoi(strsep(&metadata, "."));
}

// serialize_column - uses the format
// <npages>.<column_name>.<count>.<min>.<max>.<sum>
// to serialize a column data into a string
// returns null if there is an error
char *serialize_column(Column *column)
{
    return generic_serializer(&cpcol, (void *)column);
}

// deserialize_column - reverse of serialize_column,
// takes in a file pointer and deserialize the string into
// a column data
Column deserialize_column(FILE *column_file, Status *status)
{
    // column to be returned
    Column column;
    generic_deserializer(&cp2col, (void *)&column, column_file, status);
    return column;
}

// table serializer

// cptable - copies  a table data into a string and returns the number of bytes copied
// the copy format is
// meta_data_size.name.id.number_of_columns.col1_name.col2_name.....
int cptable(char *dst, size_t mx_size, unsigned char npages[4], void *source)
{
    Table *table = (Table *)source;

    size_t printed = snprintf(dst, mx_size, "%u%u%u%u.%u.%s.%zu.",
                              npages[0], npages[1], npages[2], npages[3],
                              table->last_id,
                              table->name,
                              table->col_count);

    dst += printed;
    for (size_t i = 0; i < table->col_count; i++)
    {

        for (int k = 0; table->columns[i].name[k] != '\0'; k++)
        {
            if (printed + 1 >= mx_size)
            {
                return printed + 1;
            }
            *dst = table->columns[i].name[k];
            dst++;
            printed++;
        }
        if (printed + 1 >= mx_size)
        {
            return printed + 1;
        }
        *dst = '.';
        dst++;
        printed++;
    }
    return printed;
}

// cp2table - does the opposit of cptable
void cp2table(void *dest, char *metadata, Status *status)
{

    Table *table = (Table *)dest;
    assert(table != NULL);

    retrack_props props = {
        .to_free = NULL,
        .outside = NULL,
        .to_remove = NULL,
        .to_close = NULL,
    };

    // if there is any error in any of these files
    // it is a result of an outisde editing of the db file
    // this isn't handled here
    // get the name
    strcpy(table->name, strsep(&metadata, "."));
    table->col_count = atoi(strsep(&metadata, "."));

    table->columns = (Column *)calloc(sizeof(Column), table->col_count);
    if (!table->columns || prepend(&props.to_free, table->columns) != 0)
    {

        *status = retrack(props, "Copying table meta datainto memory failed");
        // consider having state variable here to indicate error
        return;
    }

    table->table_length = table->col_count;

    for (size_t i = 0; i < table->col_count; i++)
    {
        char *col_name = strsep(&metadata, ".");
        FILE *col_file = load_column(current_db, table, col_name);
        if (!col_file || prepend(&props.to_close, col_file) != 0)
        {
            *status = retrack(props, "Copying table meta datainto memory failed");
            // consider having state variable here to indicate error
            return;
        }

        table->columns[i] = deserialize_column(col_file, status);
    }
}

// serialize_table - uses the format
// <npages>.<table_name>.<col_count>.<col1>.<col2>...<coln>
// to serialize a table data into a string
char *serialize_table(Table *table)
{
    return generic_serializer(&cptable, (void *)table);
}

Table deserialize_table(FILE *table_file, Status *status)
{
    Table table;
    generic_deserializer(&cp2table, (void *)&table, table_file, status);
    return table;
}

// db serializer

// cpdb - copies  a db data into a string and returns the number of bytes copied
// the copy format is
// meta_data_size.name.number_of_tables.table1_name.table2_name.....
int cpdb(char *dst, size_t mx_size, unsigned char npages[4], void *source)
{
    Db *db = (Db *)source;

    size_t printed = snprintf(dst, mx_size, "%u%u%u%u.%s.%ld.",
                              npages[0], npages[1], npages[2], npages[3],
                              db->name,
                              db->tables_size);

    dst += printed;
    for (size_t i = 0; i < db->tables_size; i++)
    {

        // cpy table name, while making sure that writing string is under dst
        for (int k = 0; db->tables[i].name[k] != '\0'; k++)
        {
            if (printed + 1 >= mx_size)
            {
                return printed + 1;
            }
            *dst = db->tables[i].name[k];
            dst++;
            printed++;
        }
        if (printed + 1 >= mx_size)
        {
            return printed + 1;
        }
        *dst = '.';
        dst++;
        printed++;
    }
    return printed;
}

// cp2db - does the opposit of cpdb
void cp2db(void *dest, char *metadata, Status *status)
{

    Db *db = (Db *)dest;
    assert(db != NULL);

    retrack_props props = {
        .to_free = NULL,
        .outside = NULL,
        .to_remove = NULL,
        .to_close = NULL,
    };

    // if there is any error in any of these files
    // it is a result of an outisde editing of the db file
    // this isn't handled here
    // get the name
    strcpy(db->name, strsep(&metadata, "."));
    db->tables_size = atoi(strsep(&metadata, "."));

    db->tables = (Table *)calloc(sizeof(Table), db->tables_size);
    if (!db->tables || prepend(&props.to_free, db->tables) != 0)
    {
        *status = retrack(props, "Copying table meta datainto memory failed");
        // consider having state variable here to indicate error
        return;
    }

    for (size_t i = 0; i < db->tables_size; i++)
    {
        char *table_name = strsep(&metadata, ".");
        FILE *table_file = load_table(current_db, table_name);
        if (!table_file || prepend(&props.to_close, table_file) != 0)
        {
            *status = retrack(props, "Copying table meta datainto memory failed");
            // consider having state variable here to indicate error
            return;
        }

        db->tables[i] = deserialize_table(table_file, status);
    };
}

// serialize_db - uses the format
// <npages>.<db_name>.<table_count>.<table1>.<table2>...<tablen>
char *serialize_db(Db *db)
{
    return generic_serializer(&cpdb, (void *)db);
}

// deserialize_db - reverse of serialize_db, copies string from
// file into a db struct
Db deserialize_db(FILE *db_file, Status *status)
{
    Db db;
    generic_deserializer(&cp2db, (void *)&db, db_file, status);
    return db;
}

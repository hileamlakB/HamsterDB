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
    char num_pages[MAX_INT_LENGTH];

    // handle every possible failure of fread
    size_t read = fread(num_pages, sizeof(unsigned char), MAX_INT_LENGTH, file);
    if (read == 4)
    {
        fseek(file, initial_offset, SEEK_SET);
        status->code = ERROR;
        return;
    }
    size_t npages = atoi(strsep((char **)&num_pages, "."));

    int meta_size = npages * sysconf(_SC_PAGESIZE);

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
// "pages_used.name.count.min_set.min.max_set.max.sum_set.sum."
// this function is a helper function fo the serialize column function
serialize_data serialize_column(Column *column)
{

    // 8 is the number ints that are being stored as a string
    size_t page_size = MAX_INT_LENGTH * 8 + strlen(column->name);
    size_t n_pages = (page_size / sysconf(_SC_PAGESIZE)) + 1;

    page_size = n_pages * sysconf(_SC_PAGESIZE);

    char *meta_data = malloc(page_size);

    size_t printed = snprintf(meta_data, page_size, "%zu.%s.%zu.%zu.%zu.%zu.%zu.%zu.%zu.",
                              n_pages,
                              column->name,
                              column->count,
                              column->min[0],
                              column->min[1],
                              column->max[0],
                              column->max[1],
                              column->sum[0],
                              column->sum[1]);

    // check the size of printed

    return (serialize_data){
        .data = meta_data,
        .size = printed,
    };
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
// last_id.name.number_of_columns.col1_name.col2_name.....
serialize_data serialize_table(Table *table)
{

    // 8 is the number ints that are being stored as a string
    size_t page_size = MAX_INT_LENGTH * 2 + strlen(table->name);

    char *meta_data = malloc(page_size);
    size_t printed = snprintf(meta_data, page_size, "%zu.%s.%zu.",
                              table->last_id,
                              table->name,
                              table->col_count);

    for (size_t i = 0; i < table->col_count; i++)
    {

        for (int k = 0; table->columns[i].name[k] != '\0'; k++)
        {
            if (printed + MAX_SIZE_NAME >= page_size)
            {
                page_size += sysconf(_SC_PAGESIZE);
                char *new_meta_data = realloc(meta_data, page_size);
                if (!new_meta_data)
                {
                    free(meta_data);
                    return (serialize_data){
                        .data = NULL,
                        .size = 0,
                    };
                }
                meta_data = new_meta_data;
            }
            meta_data[printed] = table->columns[i].name[k];
            printed++;
        }

        meta_data[printed] = '.';
        printed++;
    }

    return (serialize_data){
        .data = meta_data,
        .size = printed,
    };
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
    size_t id = atoi(strsep(&metadata, "."));
    table->last_id = id;
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

Table deserialize_table(FILE *table_file, Status *status)
{
    Table table;
    generic_deserializer(&cp2table, (void *)&table, table_file, status);
    return table;
}

// db serializer

// cpdb - copies  a db data into a string and returns the number of bytes copied
// the copy format is
// name.number_of_tables.table1_name.table2_name.....
serialize_data serialize_db(Db *db)
{

    size_t page_size = MAX_INT_LENGTH * 1 + strlen(db->name);
    char *meta_data = malloc(page_size + strlen(db->name));

    size_t printed = snprintf(meta_data, page_size, "%s.%zu.",
                              db->name,
                              db->tables_size);

    for (size_t i = 0; i < db->tables_size; i++)
    {

        // cpy table name, while making sure that writing string is under meta_data
        for (int k = 0; db->tables[i].name[k] != '\0'; k++)
        {
            if (printed + MAX_SIZE_NAME >= page_size)
            {
                page_size += sysconf(_SC_PAGESIZE);
                char *new_meta_data = realloc(meta_data, page_size);
                if (!new_meta_data)
                {
                    free(meta_data);
                    return (serialize_data){
                        .data = NULL,
                        .size = 0,
                    };
                }
                meta_data = new_meta_data;
            }
            meta_data[printed] = db->tables[i].name[k];

            printed++;
        }
        meta_data[printed] = '.';
        printed++;
    }
    return (serialize_data){
        .data = meta_data,
        .size = printed,
    };
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

// deserialize_db - reverse of serialize_db, copies string from
// file into a db struct
Db deserialize_db(FILE *db_file, Status *status)
{
    Db db;
    generic_deserializer(&cp2db, (void *)&db, db_file, status);
    return db;
}

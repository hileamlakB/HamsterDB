#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>

#include <unistd.h>
#include <assert.h>
#include "utils.h"
#include "cs165_api.h"

// common serilizers

// column serializer

// cpcol - copies  a column data into a string and returns the number of bytes copied
// the copy format is
// "pages_used.name.count.min_set.min.max_set.max.sum_set.sum.has_index.index_type.cluster_type"
// this function is a helper function fo the serialize column function
serialize_data serialize_column(Column *column)
{

    // 8 is the number ints that are being stored as a string
    size_t page_size = MAX_INT_LENGTH * 8 + strlen(column->name);
    size_t n_pages = (page_size / sysconf(_SC_PAGESIZE)) + 1;

    page_size = n_pages * sysconf(_SC_PAGESIZE);

    char *meta_data = malloc(page_size);
    column->meta_data_size = n_pages;
    size_t printed = snprintf(meta_data, page_size, "%zu.%s.%zu.%zu.%zu.%zu.%zu.%zu.%zu.%u.%u.%u.",
                              n_pages,
                              column->name,
                              column->count,
                              column->min[0],
                              column->min[1],
                              column->max[0],
                              column->max[1],
                              column->sum[0],
                              column->sum[1],
                              column->indexed,
                              column->index.type,
                              column->index.clustered);
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

    column->meta_data_size = atoi(strsep(&metadata, "."));
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
    column->indexed = atoi(strsep(&metadata, "."));
    column->index.type = atoi(strsep(&metadata, "."));
    column->index.clustered = atoi(strsep(&metadata, "."));

    status->code = OK;
}

// deserialize_column - reverse of serialize_column,
// takes in a file pointer and deserialize the string into
// a column data
Column deserialize_column(char *file_path, Status *status)
{

    // open the file
    int fd = open(file_path, O_RDWR, 0666);
    if (fd == -1)
    {
        status->code = ERROR;
        status->error_message = "Error opening file";
        return (Column){.count = 0};
    }

    // column to be returned
    Column column = empty_column;
    strcpy(column.file_path, file_path);
    column.fd = fd;
    // get the number of pages in the metadata
    char num_pages[MAX_INT_LENGTH + 1];

    read(fd, num_pages, MAX_INT_LENGTH);

    int i = 0;
    while (i < MAX_INT_LENGTH && num_pages[i] != '.')
    {
        i++;
    }
    num_pages[i] = '\0';

    size_t npages = atoi(num_pages);

    int meta_size = npages * sysconf(_SC_PAGESIZE);
    char meta_data[meta_size];

    lseek(fd, 0, SEEK_SET);
    read(fd, meta_data, meta_size);
    cp2col(&column, meta_data, status);

    return column;
}

// table serializer

// cptable - copies  a table data into a string and returns the number of bytes copied
// the copy format is
// rows.name.number_of_columns.col1_name.col2_name.....
serialize_data serialize_table(Table *table)
{

    // 8 is the number ints that are being stored as a string
    size_t page_size = MAX_INT_LENGTH * 2 + strlen(table->name);

    char *meta_data = malloc(page_size);
    size_t printed = snprintf(meta_data, page_size, "%zu.%s.%zu.",
                              table->rows,
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
void cp2table(char *path_name, void *dest, char *metadata, Status *status)
{

    Table *table = (Table *)dest;
    // struct stat sb;
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

    // get the rows
    strcpy(table->file_path, path_name);
    table->rows = atoi(strsep(&metadata, "."));
    strcpy(table->name, strsep(&metadata, "."));
    table->col_count = atoi(strsep(&metadata, "."));

    table->columns = (Column *)calloc(sizeof(Column), table->col_count);
    if (!table->columns)
    {

        *status = retrack(props, "Copying table meta datainto memory failed");
        // consider having state variable here to indicate error
        return;
    }

    table->table_length = table->col_count;

    for (size_t i = 0; i < table->col_count; i++)
    {
        char *col_name = strsep(&metadata, ".");
        char *file_path = catnstr(3, path_name, ".", col_name);

        if (!file_path || prepend(&props.to_free, file_path) != 0)
        {
            *status = retrack(props, "Copying table meta datainto memory failed");
            // consider having state variable here to indicate error
            return;
        }

        table->columns[i] = deserialize_column(file_path, status);
        if (status->code != OK)
        {
            *status = retrack(props, "Copying table meta datainto memory failed");
            // consider having state variable here to indicate error
            return;
        }
    }
    clean_up(props.to_free);
}

Table deserialize_table(char *table_path, Status *status)
{
    Table table = empty_table;
    int table_file = open(table_path, O_RDWR, 0666);

    int sz = lseek(table_file, 0, SEEK_END);
    char metadata[sz];
    lseek(table_file, 0L, SEEK_SET);

    read(table_file, metadata, sz);
    cp2table(table_path, (void *)&table, metadata, status);

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
    db->tables_capacity = db->tables_size;

    db->tables = (Table *)calloc(sizeof(Table), db->tables_size);
    if (!db->tables)
    {
        *status = retrack(props, "Copying table meta datainto memory failed");
        // consider having state variable here to indicate error
        return;
    }

    for (size_t i = 0; i < db->tables_size; i++)
    {
        char *table_name = strsep(&metadata, ".");
        char *file_path = catnstr(4, "dbdir/", db->name, ".", table_name);
        if (!file_path || prepend(&props.to_free, file_path) != 0)
        {
            *status = retrack(props, "Copying table meta datainto memory failed");
            // consider having state variable here to indicate error
            return;
        }

        db->tables[i] = deserialize_table(file_path, status);
    };

    clean_up(props.to_free);
}

// deserialize_db - reverse of serialize_db, copies string from
// file into a db struct
Db deserialize_db(char *db_path, Status *status)
{
    Db db;

    strcpy(db.file_path, db_path);
    int db_file = open(db_path, O_RDWR, 0666);

    int sz = lseek(db_file, 0, SEEK_END);
    char metadata[sz];
    lseek(db_file, 0L, SEEK_SET);

    read(db_file, metadata, sz);
    cp2db((void *)&db, metadata, status);

    return db;
}

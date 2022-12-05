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
#include <Utils/utils.h>
#include <cs165_api.h>
#include <Serializer/serialize.h>
#include <sys/mman.h>

// cp2table - does the opposit of cptable
void cp2table(char *path_name, void *dest, char *metadata)
{

    Table *table = (Table *)dest;

    // get the rows
    strcpy(table->file_path, path_name);
    table->rows = _atoi(strsep(&metadata, "."));
    table->col_count = _atoi(strsep(&metadata, "."));

    table->columns = (Column *)calloc(sizeof(Column), table->col_count);
    table->table_length = table->col_count;

    for (size_t i = 0; i < table->col_count; i++)
    {
        char *col_name = strsep(&metadata, ".");
        char *file_path = catnstr(3, path_name, ".", col_name);

        Column col = empty_column;
        strcpy(col.name, col_name);
        strcpy(col.file_path, file_path);
        map_col(table, &col, 0);

        table->columns[i] = col;
        free(file_path);
    }
}

Table deserialize_table(char *table_path)
{
    Table table = empty_table;
    int table_file = open(table_path, O_RDWR, 0666);

    int sz = lseek(table_file, 0, SEEK_END);
    char metadata[sz];
    lseek(table_file, 0L, SEEK_SET);

    read(table_file, metadata, sz);
    cp2table(table_path, (void *)&table, metadata);

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
void cp2db(void *dest, char *metadata)
{

    Db *db = (Db *)dest;

    // if there is any error in any of these files
    // it is a result of an outisde editing of the db file
    // this isn't handled here
    // get the name
    strcpy(db->name, strsep(&metadata, "."));
    db->tables_size = _atoi(strsep(&metadata, "."));
    db->tables_capacity = db->tables_size;

    db->tables = (Table *)calloc(sizeof(Table), db->tables_size);

    for (size_t i = 0; i < db->tables_size; i++)
    {
        char *table_name = strsep(&metadata, ".");
        char *file_path = catnstr(4, "dbdir/", db->name, ".", table_name);
        db->tables[i] = deserialize_table(file_path);
        strcpy(db->tables[i].name, table_name);
        free(file_path);
    };
}

// deserialize_db - reverse of serialize_db, copies string from
// file into a db struct
Db deserialize_db(char *db_path)
{
    Db db;
    const size_t PAGE_SIZE = sysconf(_SC_PAGESIZE);

    strcpy(db.file_path, db_path);
    int db_file = open(db_path, O_RDWR, 0666);

    char metadata[PAGE_SIZE];
    lseek(db_file, 0L, SEEK_SET);

    read(db_file, metadata, PAGE_SIZE);
    cp2db((void *)&db, metadata);

    return db;
}

// copy table catalog to disk
void flush_table(Table *table)
{

    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);
    int file = open(table->file_path, O_CREAT | O_RDWR, 0666);
    lseek(file, PAGE_SIZE, SEEK_SET);
    write(file, " ", 1);

    // mmap table file
    char *file_map = mmap(NULL, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, file, 0);

    size_t printed = sprintf(file_map, "%zu.%zu.",
                             table->rows,
                             table->col_count);

    for (size_t i = 0; i < table->col_count; i++)
    {
        for (int k = 0; table->columns[i].name[k] != '\0'; k++)
        {
            file_map[printed] = table->columns[i].name[k];
            printed++;
        }
        file_map[printed] = '.';
        printed++;
        flush_col(table, table->columns + i);
    }

    msync(file_map, PAGE_SIZE, MS_SYNC);
    munmap(file_map, PAGE_SIZE);
    close(file);
}

void flush_db(Db *db)
{

    if (!db)
    {
        return;
    }

    int file = open(db->file_path, O_CREAT | O_RDWR, 0666);
    lseek(file, 0, SEEK_SET);

    serialize_data db_data = serialize_db(db);
    write(file, db_data.data, db_data.size);
    for (size_t i = 0; i < db->tables_size; i++)
    {
        flush_table(db->tables + i);
    }
    free(db_data.data);
}
#define _DEFAULT_SOURCE
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>
#include <sys/mman.h>

#include "cs165_api.h"
#include "unistd.h"

#include "Serializer/serialize.h"

// // creates a map to the column file

// // flushes data from column to mapped file
// // using the write2map function
void flush_col(Table *table, Column *column)
{
    map_col(table, column, 0);
    unmap_col(column, true);
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
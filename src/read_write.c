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

// creats and opens a file cor a column
void create_colf(Column *col)
{
    if (col->fd != 0)
    {
        return;
    }

    // file name should be set for this function to work
    char *file_name = catnstr(2, "dbdir/", col->file_name);

    int fd = open(file_name, O_CREAT | O_RDWR, 0666);
    if (fd == -1)
    {
        // do something about file fialing to write
        log_err("Error opening file for column write");
        return;
    }

    // write metadata to file

    col->fd = fd;
    serialize_data meta_data = serialize_column(col);
    write(fd, meta_data.data, meta_data.size);

    free(meta_data.data);
}

// creates a map to the equivalent column map
void map_col(Column *column)
{

    // experiment with this number, the overhead mx_size
    const size_t map_size = 2 * sysconf(_SC_PAGE_SIZE);

    if (column->fd == 0)
    {
        create_colf(column);
    }

    if (column->fd == 0)
    {
        // do something about file fialing to write
        log_err("Error opening file for column write");
        return;
    }

    // see if file is already mapped
    if (column->file != NULL)
    {
        return;
    }

    // get the last ofset
    // handle the case this fails
    size_t offset = lseek(column->fd, 0, SEEK_END);

    // page aligned offset
    size_t pa_offset = offset & ~(sysconf(_SC_PAGE_SIZE) - 1);
    // Map file into memory using mmap

    char *file = (char *)mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, column->fd, pa_offset);
    if (file == MAP_FAILED)
    {
        // do something about file fialing to write
        log_err("Error mapping file for column write");
        return;
    }
    column->file = file;
    column->location = offset - pa_offset;
    column->map_size = map_size;

    // expland the file to meet map size
    lseek(column->fd, offset + map_size, SEEK_SET);
    write(column->fd, "", map_size);
    // ftruncate(column->fd, offset + map_size);
}

// flushes data from column to mapped file
// using the write2map function
void flush_col(Column *column)
{
    if (column->fd == 0)
    {
        // This will automatically write the metadat
        create_colf(column);
        return;
    }

    if (column->fd == 0)
    {
        // do something about file fialing to write
        log_err("Error opening file for column write");
        return;
    }
    lseek(column->fd, 0, SEEK_SET);

    serialize_data column_data = serialize_column(column);
    write(column->fd, column_data.data, column_data.size);
}

// write size data to column from data
void write_col(Column *column, char *data, size_t size)
{

    if (!data || size == 0)
    {
        return;
    }

    if (!column->file)
    {
        map_col(column);
    }

    if (column->location >= column->map_size)
    {
        // remap
        munmap(column->file, column->map_size);
        column->file = NULL;
        map_col(column);
    }

    size_t write_size = size < column->map_size - column->location ? size : column->map_size - column->location;

    // write to the mmaped file

    memcpy(column->file + column->location, data, write_size);
    column->location += write_size;

    write_col(column, data + write_size, size - write_size);
}

// creats and opens a file cor a column
void flush_table(Table *table)
{
    int file = open(table->file_name, O_CREAT | O_RDWR, 0666);
    lseek(file, 0, SEEK_SET);

    serialize_data table_data = serialize_table(table);
    write(file, table_data.data, table_data.size);
    for (size_t i = 0; i < table->col_count; i++)
    {
        flush_col(table->columns + i);
    }
}

void flush_db(Db *db)
{
    chdir("dbdir");
    int file = open(db->name, O_CREAT | O_RDWR, 0666);
    lseek(file, 0, SEEK_SET);

    serialize_data db_data = serialize_db(db);
    write(file, db_data.data, db_data.size);
    for (size_t i = 0; i < db->tables_size; i++)
    {
        flush_table(db->tables + i);
    }
    chdir("..");
}
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
    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);
    struct stat sb;
    int stat = fstat(col->fd, &sb);
    if (col->fd != 0 && stat != -1)
    {
        return;
    }

    // file path should be set before this function is called
    int fd = open(col->file_path, O_CREAT | O_RDWR, 0666);
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
    col->end = ((meta_data.size / PAGE_SIZE) + 1) * PAGE_SIZE;
    col->file_size = ((meta_data.size / PAGE_SIZE) + 1) * PAGE_SIZE;
    log_info("meta deta size %d", col->file_size);

    free(meta_data.data);
}

// creates a map to the equivalent column map
void map_col(Column *column)
{

    const size_t page_size = sysconf(_SC_PAGESIZE);
    // experiment with this number, the overhead mx_size
    const size_t map_size = 2 * page_size;

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

    // page aligned offset
    // size_t pa_offset = offset & ~(page_size - 1);
    // Map file into memory using mmap

    // expland the file to meet map size
    lseek(column->fd, column->end + map_size, SEEK_SET);
    write(column->fd, "", map_size);
    column->file_size = column->end + map_size;

    char *file = (char *)mmap(NULL, column->file_size, PROT_READ | PROT_WRITE, MAP_SHARED, column->fd, 0);
    if (file == MAP_FAILED)
    {
        // do something about file fialing to write
        log_err("Error mapping file for column write");
        return;
    }
    column->file = file;
    column->map_size = map_size;

    // ftruncate(column->fd, offset + map_size);
}

// flushes data from column to mapped file
// using the write2map function
void flush_col(Column *column)
{
    if (column->fd == 0)
    {
        // This will automatically write the metadata
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

    if (column->file != NULL)
    {
        msync(column->file, column->map_size, MS_SYNC);
        munmap(column->file, column->map_size);
        column->file = NULL;
    }

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

    if (column->end >= column->file_size)
    {
        // remap
        msync(column->file, column->file_size, MS_SYNC);
        munmap(column->file, column->file_size);
        column->file = NULL;
        map_col(column);
    }

    size_t write_size = size < column->file_size - column->end ? size : column->file_size - column->end;

    // write to the mmaped file

    memcpy(column->file + column->end, data, write_size);
    column->end += write_size;

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
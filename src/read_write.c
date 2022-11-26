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

// creats and opens a file for a column
void create_colf(Table *table, Column *col, Status *status)
{
    status->code = OK;

    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);
    struct stat sb;
    // check if file is already open
    int stat = fstat(col->fd, &sb);
    if (col->fd != 0 && stat != -1)
    {
        return;
    }

    // file path should be set before this function is called
    int fd = open(col->file_path, O_CREAT | O_RDWR, 0666); // create file if it didn't exist
    if (fd == -1)
    {
        // do something about file fialing to write
        status->error_message = "Error opening file for column write";
        status->code = ERROR;
        return;
    }

    // Update metadata on file
    col->fd = fd;
    serialize_data meta_data = serialize_column(col);
    write(fd, meta_data.data, meta_data.size);
    col->end = (table->rows + ((meta_data.size / PAGE_SIZE) + 1)) * PAGE_SIZE;
    col->file_size = ((meta_data.size / PAGE_SIZE) + 1) * PAGE_SIZE;
    free(meta_data.data);
}

// creates a map to the column file
void map_col(Table *table, Column *column, Status *status)
{

    status->code = OK;
    const size_t page_size = sysconf(_SC_PAGESIZE);
    // experiment with this number, the overhead mx_size
    const size_t map_size = 4 * page_size;

    // check if the file is open if not open it
    create_colf(table, column, status);
    if (status->code != OK)
    {
        return;
    }

    // see if file is already mapped
    if (column->file != NULL)
    {
        return;
    }

    // expand the file by map size for some writing room
    lseek(column->fd, column->end + map_size, SEEK_SET);
    write(column->fd, "", map_size);

    column->file_size = column->end + map_size;
    char *file = (char *)mmap(NULL, column->file_size, PROT_READ | PROT_WRITE, MAP_SHARED, column->fd, 0);
    if (file == MAP_FAILED)
    {
        status->code = ERROR;
        status->error_message = "Error mapping file for column write";
        return;
    }
    column->file = file;
    column->map_size = map_size;
}

// flushes data from column to mapped file
// using the write2map function
void flush_col(Table *table, Column *column, Status *status)
{
    status->code = OK;
    create_colf(table, column, status);

    if (status->code != OK)
    {
        return;
    }

    lseek(column->fd, 0, SEEK_SET);
    if (column->file != NULL)
    {
        // sync mapped file
        msync(column->file, column->map_size, MS_SYNC);
        munmap(column->file, column->map_size);
        column->file = NULL;
    }

    serialize_data column_data = serialize_column(column);
    write(column->fd, column_data.data, column_data.size);
    free(column_data.data);
}

// write size data to column from data
void write_load(Table *table, Column *column, char *data, size_t size, Status *status)
{

    if (!data || size == 0)
    {
        return;
    }

    status->code = OK;
    map_col(table, column, status);

    if (status->code != OK)
    {
        return;
    }

    if (column->end + MAX_INT_LENGTH + 1 >= column->file_size)
    {
        // remap
        msync(column->file, column->file_size, MS_SYNC);
        munmap(column->file, column->file_size);
        column->file = NULL;
        map_col(table, column, status);
        if (status->code != OK)
        {
            return;
        }
    }

    size_t write_size = size < column->file_size - column->end ? size : column->file_size - column->end;
    memcpy(column->file + column->end, data, write_size);
    column->end += write_size;
    // pending load might not write full cols sometimes as max write_size might cut off
    // some porition
    // column->pending_load += (write_size / (MAX_INT_LENGTH + 1));

    write_load(table, column, data + write_size, size - write_size, status);
}

bool insert_col(Table *table, Column *col, char *value, Status *status)
{
    status->code = OK;
    map_col(table, col, status);

    if (status->code != OK)
    {
        return false;
    }

    if (col->end + MAX_INT_LENGTH + 1 >= col->file_size)
    {
        // remap
        msync(col->file, col->file_size, MS_SYNC);
        munmap(col->file, col->file_size);
        col->file = NULL;
        map_col(table, col, status);
        if (status->code != OK)
        {
            return false;
        }
    }

    char *zero_padded_str = zeropadd(value, NULL);
    memcpy(col->file + col->end, zero_padded_str, MAX_INT_LENGTH);
    memcpy(col->file + col->end + MAX_INT_LENGTH, ",", 1);
    col->end += MAX_INT_LENGTH + 1;
    free(zero_padded_str);
    return true;
}
// insert to database
void insert(Table *table, char **values, Status *status)
{
    bool success = true;
    for (size_t i = 0; i < table->col_count; i++)
    {

        if (!insert_col(table, &table->columns[i], values[i], status))
        {
            success = false;
            break;
        }
    }
    if (success)
    {
        table->rows += 1;
        status->code = OK;
    }
    update_col_end(table);
    free(values);
}

void update_col_end(Table *table)
{

    const size_t page_size = sysconf(_SC_PAGESIZE);
    for (size_t i = 0; i < table->col_count; i++)
    {

        table->columns[i].end = table->columns[i].meta_data_size * page_size + table->rows * (MAX_INT_LENGTH + 1);
    }
}

// creats and opens a file cor a column
void flush_table(Table *table, Status *status)
{
    int file = open(table->file_path, O_CREAT | O_RDWR, 0666);
    lseek(file, 0, SEEK_SET);

    serialize_data table_data = serialize_table(table);
    write(file, table_data.data, table_data.size);
    for (size_t i = 0; i < table->col_count; i++)
    {
        flush_col(table, table->columns + i, status);
    }
    free(table_data.data);
}

void flush_db(Db *db, Status *status)
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
        flush_table(db->tables + i, status);
    }
    free(db_data.data);
}
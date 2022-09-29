#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "cs165_api.h"
#include "unistd.h"

size_t write2map(size_t fd, char *file, size_t index, size_t map_size, char *data, size_t size)
{

    assert(fd != 0);
    assert(file);
    assert(index + size <= map_size);

    // write to the mmaped file
    memcpy(file + index, data, size);
    return size;
}
void create_colf(Column *col)
{
    if (col->fd != 0)
    {
        return;
    }

    // handle possible failures of catnstr
    char *file_name = catnstr(4, col->db->name, ".", col->table->name, ".", col->name);
    size_t fd = open(file_name, O_CREAT | O_RDWR, 0666);
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
    free(file_name);
}

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

    char *file = (char *)mmap(NULL, map_size, PROT_READ | PROT_READ, MAP_PRIVATE, column->fd, pa_offset);
    if (file == MAP_FAILED)
    {
        // do something about file fialing to write
        log_err("Error mapping file for column write");
        return;
    }
    column->file = file;
    column->location = offset - pa_offset;
    column->map_size = map_size;
}
void flush_col(Column *column)
{

    // check if there is a map
    // check if the map has enough space for write
    // remap
    // write to the map

    if (!column->file)
    {
        map_col(column);
    }

    // handle the case there is no map even after mapping

    // check if the map has enough space for write
    if (column->location + column->pending_i_t > column->map_size)
    {
        // remap
        munmap(column->file, column->map_size);
        column->file = NULL;
        map_col(column);
    }

    // data might be bigger even after remapping, this isonly possible
    // if we are writing from a very large file, which could only be done
    // through the load function, thus the load function should put a limit on this

    column->location += write2map(
        column->fd,
        column->file,
        column->location,
        column->map_size,
        column->pending_i,
        column->pending_i_t);
}

void write_col(Column *column, char *data, size_t size)
{

    int max_size = sysconf(_SC_PAGESIZE);

    // check if a writing could happen to a pending write
    if (column->pending_i_t + size < max_size)
    {
        memcpy(column->pending_i + column->pending_i_t, data, size);
        column->pending_i_t += size;
        return;
    }

    // caching might not be the best idea in the world in this case
    flush_col(column);

    // write the data into cache
    if (column->pending_i_t + size < max_size)
    {
        memcpy(column->pending_i + column->pending_i_t, data, size);
        column->pending_i_t += size;
        return;
    }

    // if even after caching the data is too big, write it directly
    // if data is bigger than map size, the write2map function will graciously fails
    column->location += write2map(
        column->fd,
        column->file,
        column->location,
        column->map_size,
        data,
        size);
}

void write_table(Table *table, char *data)
{
}

#include <cs165_api.h>
#include <Serializer/serialize.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <data_structures.h>

int map_col(Table *tbl, Column *col, size_t writing_space)
{

    const size_t page_size = sysconf(_SC_PAGESIZE);
    size_t map_size = 4 * page_size; // to be experimented with

    if (writing_space * sizeof(int) > 4 * page_size)
    {
        map_size = writing_space * sizeof(int);
    }

    if (!tbl || !col)
    {
        log_err("-- Trying to map a non existing column/table");
        return -1;
    }

    if (col->fd == -1)
    {
        col->fd = open(col->file_path, O_RDWR | O_CREAT, 0666);
        if (col->fd == -1)
        {
            return -1;
        }
    }

    // get the size of the file and map it to memory
    struct stat sb;
    if (fstat(col->fd, &sb) == -1)
    {
        return -1;
    }

    // expand file by n pages for writing

    lseek(col->fd, sb.st_size + map_size, SEEK_SET);
    write(col->fd, " ", 1);

    // map the file to memory
    col->file = mmap(NULL, sb.st_size + map_size, PROT_READ | PROT_WRITE, MAP_SHARED, col->fd, 0);
    if (col->file == MAP_FAILED)
    {
        return -1;
    }

    // write meta data to the file
    col->metadata = (ColumnMetaData *)(col->file);
    col->data = (int *)(col->metadata + 1);
    col->end = tbl->rows;
    col->file_size = sb.st_size + map_size;

    return 0;
}

int unmap_col(Column *col, bool should_close)
{
    munmap(col->file, col->file_size);
    col->file = NULL;
    col->metadata = NULL;
    col->data = NULL;
    col->end = 0;
    col->file_size = 0;

    if (should_close)
    {
        close(col->fd);
    }
    return 0;
}

int remap_col(Table *tbl, Column *col, size_t new_size)
{
    if (!col)
    {
        log_err("-- Trying to remap a non existing column");
        return -1;
    }
    if (!col->file)
    {
        return map_col(tbl, col, new_size);
    }

    // check size and if it needs to be remapped
    // remap
    if (writing_space(col) < new_size)
    {
        // unmap
        unmap_col(col, false);
        return map_col(tbl, col, new_size);
    }
    return 0;
}

void flush_btree(Column *col)
{
    // create and open file to flush btree to
    char *file_path = catnstr(3, col->file_path, ".", "btree");
    int fd = open(file_path, O_RDWR | O_CREAT, 0666);

    // expand and mmap file
    lseek(fd, sizeof(serialized_node) * 1000, SEEK_SET);
    write(fd, " ", 1);
    serialized_node *file = mmap(NULL, sizeof(serialized_node) * 1000, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

    // serialize btree
    bt_serialize(col->index.btree, file);

    // unmap and close file
    munmap(file, sizeof(serialized_node *) * 1000);
    close(fd);
    return;
}

void flush_col(Table *table, Column *column)
{
    map_col(table, column, 0); // creates a file if it wasn't created

    // if there is a btree associated with the column, flush it
    if (column->indexed && column->index.type == BTREE)
    {
        flush_btree(column);
    }

    unmap_col(column, true); // closes the file
}

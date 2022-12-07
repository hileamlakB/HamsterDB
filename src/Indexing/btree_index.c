#include <cs165_api.h>
#include "data_structures.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sys/mman.h>
#include <unistd.h>
#include <stdio.h>

#include <assert.h>

int map_sorted(Table *tbl, Column *col)
{

    assert(col->indexed);
    if (col->index.sorted_file)
    {
        return 0;
    }

    char *sorted_filename = catnstr(2, col->file_path, ".sorted");
    int fd_sorted = open(sorted_filename, O_RDONLY, 0666);
    free(sorted_filename);

    col->index.sorted_file = mmap(NULL, tbl->rows * sizeof(int), PROT_READ, MAP_PRIVATE, fd_sorted, 0);
    return 0;
}

// Function: create_btree - creats a btree for a sorted column
void create_btree(Table *tbl, Column *col)
{
    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);
    map_sorted(tbl, col);

    // create btree
    Btree_node *btree = bt_create();

    int last_num = col->index.sorted_file[PAGE_SIZE / sizeof(int) - 1];
    bt_insert(btree, last_num, PAGE_SIZE / sizeof(int) - 1);

    // insert all the values into the btree
    for (size_t i = 0; i < tbl->rows; i += PAGE_SIZE / sizeof(int) - 1)
    {
        int num = col->index.sorted_file[i];
        if (num != last_num)
        {
            bt_insert(btree, num, i);
            last_num = num;
        }
    }

    col->index.btree = btree;
}

void load_btree(Column *col)
{

    assert(col->indexed && col->index.type == BTREE);

    char *btree_filename = catnstr(2, col->file_path, ".btree");
    int fd_btree = open(btree_filename, O_RDONLY, 0666);
    free(btree_filename);

    // a thousand is an arbitrarly choosen number
    serialized_node **nodes = mmap(NULL, sizeof(serialized_node *) * 1000, PROT_READ, MAP_PRIVATE, fd_btree, 0);

    col->index.btree = bt_deserialize(nodes);
}

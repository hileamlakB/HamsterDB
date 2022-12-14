#define _DEFAULT_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>
#include <sys/mman.h>
#include <errno.h>

#include "cs165_api.h"
#include <unistd.h>
#include <Serializer/serialize.h>
#include <sort.h>
#include <Create/create.h>
#include <Indexing/index.h>

void populate_index(Table *tbl, Column *col)
{

    if (!col->indexed)
    {
        return;
    }

    sort_col(tbl, col);

    ColumnIndex idx = col->index;

    if (idx.clustered == CLUSTERED)
    {
        // use the map to organize the the rest of the columns
        propagate_sort(tbl, col);
    }

    if (idx.type == BTREE)
    {
        // create btree index
        create_btree(tbl, col);
    }
}

ColumnIndex create_index(
    Table *tbl, Column *col, IndexType index_type, ClusterType cluster_type)
{
    ColumnIndex idx = {
        .type = NO_INDEX,
        .clustered = NO_CLUSTER,
    };

    if (index_type == SORTED)
    {
        idx = create_sorted_index(tbl, col, cluster_type);
    }
    else if (index_type == BTREE)
    {
        idx = create_btree_index(tbl, col, cluster_type);
    }

    col->indexed = true;
    col->index = idx;

    return idx;
}

ColumnIndex create_sorted_index(Table *tbl, Column *col, ClusterType cluster_type)
{
    if (col->indexed)
    {
        return col->index;
    }

    map_col(tbl, col, 0);

    ColumnIndex idx = (ColumnIndex){
        .type = SORTED,
        .clustered = cluster_type,
    };

    char *idx_name = catnstr(2, col->name, "_sorted");
    memcpy(idx.name, idx_name, strlen(idx_name));
    free(idx_name);
    col->index = idx;
    return idx;
}

ColumnIndex create_btree_index(Table *tbl, Column *col, ClusterType cluster_type)
{
    create_sorted_index(tbl, col, cluster_type);
    col->index.type = BTREE;
    return col->index;
}

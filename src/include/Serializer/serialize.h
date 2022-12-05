#ifndef __SERIALIZE__
#define __SERIALIZE__

#include <cs165_api.h>

// opens and creats a map of a column if it
// didn't previously exist
int map_col(Table *tbl, Column *col, size_t size);

// unmaps a column with or without closing the related file
int unmap_col(Column *col, bool should_close);

int remap_col(Table *tbl, Column *col, size_t size);

// column_methods
size_t writing_space(Column *column);

#endif
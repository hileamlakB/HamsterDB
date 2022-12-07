#ifndef __INDEX_H
#define __INDEX_H

#include <cs165_api.h>
#include <Utils/utils.h>

void create_btree(Table *tbl, Column *col);
void load_btree(Column *col);

#endif // __INDEX_H
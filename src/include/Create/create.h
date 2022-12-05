#ifndef __CREATE_H__
#define __CREATE_H__

#include <cs165_api.h>

Column *create_column(Table *table, char *name, Status *ret_status);
int load_db(const char *db_name);
void free_db();
void populate_index(Table *tbl, Column *col);
ColumnIndex create_index(
    Table *tbl, Column *col, IndexType index_type, ClusterType cluster_type);
ColumnIndex create_sorted_index(Table *tbl, Column *col, ClusterType cluster_type);
ColumnIndex create_btree_index(Table *tbl, Column *col, ClusterType cluster_type);
void load_btree(Table *tble, Column *col);
Table *create_table(Db *db, const char *name, size_t num_columns);
Status create_db(const char *db_name);
Table *create_table(Db *db, const char *name, size_t num_columns);

#endif
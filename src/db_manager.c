#include "cs165_api.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include "utils.h"
#include "client_context.h"
#include <assert.h>

// In this class, there will always be only one active database at a time
Db *current_db;
linkedList *var_pool;
Column empty_column;
Table empty_table;
batch_query batch = {
	.mode = false,
	.num_queries = 0

};

Column *create_column(Table *table, char *name, bool sorted, Status *ret_status)
{
	// this is going to be used with indexing
	(void)sorted;

	// default status to error and improve it to
	// success if everything goes well
	ret_status->code = ERROR;

	if (lookup_column(table, name))
	{
		ret_status->error_message = "Column already exists";
		return NULL;
	}

	Column column = empty_column;

	strcpy(column.name, name);
	char *file_path = catnstr(6, "dbdir/", current_db->name, ".", table->name, ".", name); // hanle possible failure

	strcpy(column.file_path, file_path);

	if (table->col_count <= table->table_length)
	{
		free(file_path);
		ret_status->error_message = "Table is full";
		return NULL;
	}

	// number of columns is fixed
	table->columns[table->table_length] = column;
	table->table_length++;

	// success
	ret_status->code = OK;

	free(file_path);
	return &table->columns[table->table_length - 1];
}

ColumnIndex *create_sorted_index(Table *tbl, Column *col, ClusterType ClusterType, Status *ret_status)
{
	(void)ClusterType;
	(void)tbl;
	(void)col;
	(void)ret_status;
	return NULL;
}

ColumnIndex *create_btree_index(Table *tbl, Column *col, ClusterType ClusterType, Status *ret_status)
{
	(void)ClusterType;
	(void)tbl;
	(void)col;
	(void)ret_status;
	return NULL;
}

ColumnIndex *create_index(
	Table *tbl, Column *col, IndexType index_type, ClusterType cluster_type, Status *ret_status)
{
	if (index_type == SORTED)
	{
		return create_sorted_index(tbl, col, cluster_type, ret_status);
	}
	else if (index_type == BTREE)
	{
		return create_btree_index(tbl, col, cluster_type, ret_status);
	}

	return NULL;
}

/*
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table *create_table(Db *db, const char *name, size_t num_columns, Status *ret_status)
{

	// keep track of important activies incase of failure
	retrack_props props = {
		.to_free = NULL,
		.outside = NULL,
		.to_remove = NULL,
		.to_close = NULL,
	};

	// default status to error and improve it to
	// success if everything goes well
	ret_status->code = ERROR;

	if (lookup_table(db, (char *)name))
	{
		ret_status->error_message = "Table already exists";
		return NULL;
	}

	Table table = empty_table;

	strncpy(table.name, name, MAX_SIZE_NAME);
	char *file_path = catnstr(4, "dbdir/", db->name, ".", name);
	if (!file_path || prepend(&props.to_free, file_path) != 0)
	{
		*ret_status = retrack(props, "System Failure: error allocating memory for internal use");
		return NULL;
	}

	strcpy(table.file_path, file_path);
	table.col_count = num_columns;
	table.table_length = 0;

	// you can support flexible number of columns later
	// the same way you did for tables
	table.columns = calloc(num_columns, sizeof(Column));
	if (!table.columns)
	{
		*ret_status = retrack(props, "System Failure: error allocating memory for columns");
		return NULL;
	}

	if (db->tables_capacity == db->tables_size)
	{
		// double the size of the array
		db->tables_capacity += 1;
		db->tables_capacity *= 2;
		Table *new_tables = realloc(db->tables, db->tables_capacity * sizeof(Table));
		if (!new_tables)
		{
			db->tables_capacity /= 2;
			db->tables_capacity -= 1;
			retrack(props, "System Failure: error allocating memory for tables");
			return NULL;
		}
		db->tables = new_tables;
	}

	db->tables[db->tables_size] = table;
	db->tables_size++;

	// success
	ret_status->code = OK;
	clean_up(props.to_free);
	close_files(props.to_close);

	return &db->tables[db->tables_size - 1];
}

/*
 * Similarly, this method is meant to create a database.
 */
Status create_db(const char *db_name)
{

	// keep track of important activies incase of failure
	retrack_props props = {
		.to_free = NULL,
		.outside = NULL,
		.to_remove = NULL,
		.to_close = NULL,
	};

	// create a dir for the db
	mkdir("dbdir", 0777);

	// makesure db doesn't already exist
	char *file_path = catnstr(2, "dbdir/", db_name);
	if (!file_path || prepend(&props.to_free, file_path) != 0)
	{
		return retrack(props, "System Failure: error allocating memory for internal use");
	}

	if (access(file_path, F_OK) == 0)
	{
		// DON"T KNOW WHAT THE EXCPECTED BEHAVIOUR IS
		load_db(db_name);
		return retrack(props, "--Database already exists");
	}

	Db *active_db = (Db *)malloc(sizeof(Db));
	if (!active_db)
	{
		return retrack(props, "System Failure: error allocating memory for active_db");
	}

	strncpy(active_db->name, db_name, MAX_SIZE_NAME);
	strcpy(active_db->file_path, file_path);

	active_db->tables = NULL;
	active_db->tables_size = 0;
	active_db->tables_capacity = 0;

	// right now there is only one active database
	// you might expand this in the furture with your old code
	assert(current_db == NULL);
	current_db = active_db;

	clean_up(props.to_free);
	close_files(props.to_close);

	return (Status){.code = OK};
}

Status load_db(const char *db_name)
{
	assert(current_db == NULL);

	// make sure the db folder exists
	// if does load db, table, and column

	char *file_name = catnstr(2, "dbdir/", db_name);

	if (access(file_name, F_OK) == -1)
	{
		free(file_name);
		return (Status){.code = ERROR, .error_message = "Database doesn't exist"};
	}

	Status status;
	current_db = malloc(sizeof(Db));
	*current_db = deserialize_db(file_name, &status);

	free(file_name);
	return status;
}

void free_db()
{

	for (size_t i = 0; i < current_db->tables_size; i++)
	{
		// free each table

		free(current_db->tables[i].columns);
	}
	free(current_db->tables);
	free(current_db);
}
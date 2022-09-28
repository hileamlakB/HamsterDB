#include "cs165_api.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include "utils.h"
#include "client_context.h"

// In this class, there will always be only one active database at a time
Db *current_db;

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

	Column column;
	strcpy(column.name, name);

	if (table->col_count <= table->table_length)
	{
		ret_status->error_message = "Table is full";
		return NULL;
	}

	// number of columns is fixed
	table->columns[table->table_length] = column;
	table->table_length++;

	// success
	ret_status->code = OK;

	return &table->columns[table->table_length - 1];
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

	Table table;

	strcpy(table.name, name);
	table.col_count = num_columns;
	table.table_length = 0;
	// you can support flexible number of columns later
	// the same way you did for tables
	table.columns = malloc(num_columns * sizeof(Column));
	if (table.columns == NULL || prepend(&props.to_free, table.columns) != 0)
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

	// makesure name of db isn't too long and doesn't already exist
	if (strlen(db_name) > MAX_SIZE_NAME)
	{
		return retrack(props, "Database name too long");
	}

	Db *active_db = (Db *)malloc(sizeof(Db));
	if (!active_db || prepend(&props.outside, active_db) != 0)
	{
		return retrack(props, "System Failure: error allocating memory for active_db");
	}

	strcpy(active_db->name, db_name);
	//  this values I am not sure about yet
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

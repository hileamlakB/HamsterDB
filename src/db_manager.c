#include "cs165_api.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include "utils.h"
#include "client_context.h"
#include <assert.h>
#include <math.h>
#include "sort.h"

// In this class, there will always be only one active database at a time
Db *current_db;
linkedList *var_pool;
Column empty_column;
Table empty_table;
batch_query batch = {
	.mode = false,
	.num_queries = 0

};

batch_load bload = {
	.mode = false,
	.left_over = NULL,

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

ColumnIndex create_sorted_index(Table *tbl, Column *col, ClusterType cluster_type, Status *ret_status)
{
	if (col->indexed)
	{
		return col->index;
	}

	create_colf(tbl, col, ret_status);

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

// unimplemented
ColumnIndex create_btree_index(Table *tbl, Column *col, ClusterType cluster_type, Status *ret_status)
{
	create_sorted_index(tbl, col, cluster_type, ret_status);
	col->index.type = BTREE;
	return col->index;
}

#define fanout 1024 // 4096 /sizeof(int)

typedef struct BTREE_META
{
	int level;
	int total_nodes;
	int levels_below[10];
	char file_path[MAX_PATH_NAME];
} BTREE_META;

void create_btree(Table *tbl, Column *col)
{
	const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);
	// laod the sorted column
	// create the btree
	// save the btree
	char *sorted_col = catnstr(2, col->file_path, ".sorted");
	int fd = open(sorted_col, O_RDONLY);
	free(sorted_col);
	// mmap sorted file
	char *sorted = mmap(NULL, tbl->rows * (MAX_INT_LENGTH + 1), PROT_READ, MAP_PRIVATE, fd, 0);

	char *btree_file = catnstr(2, col->file_path, ".btree");
	int btree_fd = open(btree_file, O_RDWR | O_CREAT, 0666);

	// prepare the btree file

	size_t levels = 0;
	size_t num_nodes = 0;
	size_t nodes_per_level = tbl->rows / fanout;
	size_t nodes_below[10]; // assming max 10 levels

	while (nodes_per_level > 0)
	{
		nodes_below[levels] = num_nodes;
		num_nodes += nodes_per_level;
		levels++;
		nodes_per_level /= fanout;
		if (nodes_per_level == 0)
		{
			nodes_below[levels] = num_nodes;
			num_nodes++;
			levels++;
		}
	}

	size_t btree_size = num_nodes * fanout * sizeof(int);

	lseek(btree_fd, btree_size, SEEK_SET);
	write(btree_fd, " ", 1);
	BTREE_META *btree_ = mmap(NULL, btree_size + sizeof(BTREE), PROT_READ | PROT_WRITE, MAP_SHARED, btree_fd, 0);
	int *btree = (int *)(btree_ + 1);

	// create the lowest level of the btree
	int lowes_level_index = (num_nodes - nodes_below[levels - 1]) * PAGE_SIZE;
	for (size_t i = 0; i < tbl->rows / PAGE_SIZE; i += 1)
	{
		size_t last_page_element = i * PAGE_SIZE + ((PAGE_SIZE / (MAX_INT_LENGTH + 1)) - 1) * (MAX_INT_LENGTH + 1);
		int last_element = atoi(sorted + last_page_element);
		btree[lowes_level_index + i] = last_element;
	}

	// create the rest of the levels
	for (size_t i = 1; i <= levels; i++)
	{
		int level_index = (num_nodes - nodes_below[levels - i]) * PAGE_SIZE;
		int next_level_index = (num_nodes - nodes_below[levels - i - 1]) * PAGE_SIZE;
		int next_level_size = nodes_below[levels - i] - nodes_below[levels - i - 1];
		for (int j = 0; j < next_level_size; j++)
		{
			int last_element = btree[next_level_index + j * fanout + fanout - 1];
			btree[level_index + j] = last_element;
		}
	}

	// create the root node
	int next_node_start = (num_nodes - nodes_below[levels - 1]) * PAGE_SIZE;
	int num_internal_nodes = nodes_below[levels - 1];
	for (int i = 0; i < num_internal_nodes; i++)
	{
		btree[i] = btree[next_node_start + i * fanout + fanout - 1];
	}

	// write meta_data
	btree_->level = levels;
	btree_->total_nodes = num_nodes;
	memcpy(btree_->levels_below, nodes_below, sizeof(nodes_below));
	memcpy(btree_->file_path, btree_file, strlen(btree_file));
	free(btree_file);

	// unmap
	munmap(btree, btree_size);
	munmap(sorted, tbl->rows * (MAX_INT_LENGTH + 1));
	close(btree_fd);
	close(fd);
}

void create_btree2(Table *tbl, Column *col)
{
	const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);
	// laod the sorted column
	// create the btree
	// save the btree
	char *sorted_col = catnstr(2, col->file_path, ".sorted");
	int fd = open(sorted_col, O_RDONLY);
	free(sorted_col);
	// mmap sorted file
	char *sorted = mmap(NULL, tbl->rows * (MAX_INT_LENGTH + 1), PROT_READ, MAP_PRIVATE, fd, 0);

	char *btree_file = catnstr(2, col->file_path, ".btree");
	int btree_fd = open(btree_file, O_RDWR | O_CREAT, 0666);
	free(btree_file);

	lseek(btree_fd, tbl->rows * (MAX_INT_LENGTH + 1), SEEK_SET);
	write(btree_fd, " ", 1);

	int *btree = mmap(NULL, tbl->rows * (sizeof(int)), PROT_READ | PROT_WRITE, MAP_SHARED, btree_fd, 0);

	size_t levels = 0;
	size_t nodes_per_level[10] = {0}; // max 10 levels assumption

	size_t num_pages = (tbl->rows * (MAX_INT_LENGTH + 1)) / PAGE_SIZE;
	size_t num_elements_per_page = PAGE_SIZE / (MAX_INT_LENGTH + 1);
	size_t btree_index = 0;
	// create the lowest level of the btree
	for (size_t i = 0; i < num_pages; i += 1, btree_index += 1)
	{
		// get the last element of the page
		size_t num_elements = ((i + 1) * PAGE_SIZE) / (MAX_INT_LENGTH + 1);
		size_t last_page_element = (num_elements - 1) * (MAX_INT_LENGTH + 1);
		int last_element = atoi(sorted + last_page_element);
		btree[btree_index] = last_element;
	}
	nodes_per_level[levels] += num_pages * sizeof(int) / PAGE_SIZE;
	size_t last_level_start = 0;
	size_t last_level_end = btree_index;
	bool is_last_full = true;
	bool last_filled_index = 0;
	// add extra elements
	if (num_pages * sizeof(int) > PAGE_SIZE && num_pages % PAGE_SIZE != 0)
	{
		size_t last_page_element = tbl->rows * (MAX_INT_LENGTH + 1) - (MAX_INT_LENGTH + 1);
		int last_element = atoi(sorted + last_page_element);
		btree[num_pages] = last_element;
		btree[num_pages + 1] = -1; // this is to indicate the end of an incomplete node

		size_t next_page = (num_pages / PAGE_SIZE + 1) * PAGE_SIZE;

		btree[next_page - 1] = last_element; // for easy access
		btree[next_page - 2] = btree_index;	 // this stores the last place where the btree was written
		btree_index = next_page;
		nodes_per_level[levels] += 1;
		is_last_full = false;
	}
	last_level_end = btree_index;

	levels++;

	while (last_level_end - last_level_start > 1)
	{

		// becaue the last one might not be full
		for (size_t i = 0; i < (last_level_end - last_level_start) / fanout; i += 1, btree_index += 1)
		{
			btree[btree_index] = btree[last_level_start + i * fanout + fanout - 1];
		}

		last_level_start = last_level_end;
		last_level_end += nodes_per_level[levels - 1] / fanout;
		nodes_per_level[levels] += nodes_per_level[levels - 1] / fanout;

		if (!is_last_full)
		{
		}

		levels++;
	}

	// unmap file
	munmap(btree, tbl->rows);
	munmap(sorted, tbl->rows * (MAX_INT_LENGTH + 1));
	close(btree_fd);
	close(fd);
}

void laod_btree(Table *tble, Column *col) {}

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

	// if (idx.type == BTREE)
	// {
	// 	// create btree index
	// 	// create_btree2(tbl, col);
	// }
}

ColumnIndex create_index(
	Table *tbl, Column *col, IndexType index_type, ClusterType cluster_type, Status *ret_status)
{
	ColumnIndex idx = {
		.type = NO_INDEX,
		.clustered = NO_CLUSTER,
	};

	if (index_type == SORTED)
	{
		idx = create_sorted_index(tbl, col, cluster_type, ret_status);
	}
	else if (index_type == BTREE)
	{
		idx = create_btree_index(tbl, col, cluster_type, ret_status);
	}

	col->indexed = true;
	col->index = idx;
	serialize_data meta_data = serialize_column(col);
	write(col->fd, meta_data.data, meta_data.size);
	free(meta_data.data);

	return idx;
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

	memcpy(table.name, name, MAX_SIZE_NAME);
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

	memcpy(active_db->name, db_name, MAX_SIZE_NAME);
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
		// // unmap colums if any
		// for (size_t j = 0; j < current_db->tables[i].col_count; j++)
		// {
		// 	Column *col = &current_db->tables[i].columns[j];
		// 	if (col->file)
		// 	{
		// 		munmap(col->file, col->map_size);
		// 	}
		// 	if (col->read_map)
		// 	{
		// 		munmap(col->read_map, col->read_map_size);
		// 	}
		// }
		// free each table
		free(current_db->tables[i].columns);
	}
	free(current_db->tables);
	free(current_db);
}
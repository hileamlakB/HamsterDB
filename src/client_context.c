#include "client_context.h"
#include <string.h>
/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 *
 */
// Table *lookup_table(char *name)
// {
// 	// void pattern for 'using' a variable to prevent compiler unused variable warning
// 	(void)name;

// 	return NULL;
// }

/**
 *  Getting started hint:
 * 		What other entities are context related (and contextual with respect to what scope in your design)?
 * 		What else will you define in this file?
 **/

// I didn't see this file preivously but get table is what I kind want to do with
// this function

Table *lookup_table(Db *db, char *name)
{

	Table *tbls = db->tables;
	//  consider using some hash tables to store table names here
	for (size_t i = 0; i < db->tables_size; i++)
	{
		if (strcmp(tbls[i].name, name) == 0)
		{
			return &tbls[i];
		}
	}
	return NULL;
}

Column *lookup_column(Table *table, char *name)
{

	Column *columns = table->columns;
	//  consider using some hash tables to store table names here
	for (size_t i = 0; i < table->col_count; i++)
	{
		if (strcmp(columns[i].name, name) == 0)
		{
			return &columns[i];
		}
	}
	return NULL;
}
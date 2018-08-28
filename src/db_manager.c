#include "cs165_api.h"

// In this class, there will always be only one active database at a time
Db *current_db;

/* 
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	// void pattern for 'using' a variable to prevent compiler unused variable warning
	(void) (db);
	(void) name;
	(void) num_columns;

	ret_status->code=OK;
	return NULL;
}

/* 
 * Similarly, this method is meant to create a database.
 * As an implementation choice, one can use the same method
 * for creating a new database and for loading a database 
 * from disk, or one can divide the two into two different
 * methods.
 */
Status add_db(const char* db_name, bool is_new) {
	// void pattern for 'using' a variable to prevent compiler unused variable warning
	(void) (db_name);
	(void) (is_new);
	struct Status ret_status;
	
	ret_status.code = OK;
	return ret_status;
}

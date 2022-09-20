#include "cs165_api.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include "utils.h"

// In this class, there will always be only one active database at a time
Db *current_db;

/*
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table *create_table(Db *db, const char *name, size_t num_columns, Status *ret_status)
{
	// void pattern for 'using' a variable to prevent compiler unused variable warning
	(void)(db);
	(void)name;
	(void)num_columns;

	ret_status->code = OK;
	return NULL;
}

/*
 * Similarly, this method is meant to create a database.
 */
Status create_db(const char *db_name)
{
	// void pattern for 'using' a variable to prevent compiler unused variable warning

	struct Status ret_status;

	//  use an absolute path instead for next time
	DIR *dir = opendir("dbdir");
	if (ENOENT == errno)
	{
		mkdir("dbdir", 0777);
	}
	else if (dir)
	{
		closedir(dir);
	}
	else
	{
		ret_status.code = ERROR;
		ret_status.error_message = "Error opening db directory";
		return ret_status;
	}

	// makesure name of db isn't too long and doesn't already exist
	if (strlen(db_name) > MAX_SIZE_NAME)
	{
		ret_status.code = ERROR;
		ret_status.error_message = "Database name too long";
		return ret_status;
	}

	char *file_path = (char *)malloc(strlen(db_name) + strlen("dbdir/") + 1);
	if (!file_path)
	{
		ret_status.code = ERROR;
		ret_status.error_message = "Systems failure memory";
		return ret_status;
	}
	strcpy(file_path, "dbdir/");
	strcat(file_path, db_name);

	// make sure you are in the write directory

	if (access(file_path, F_OK) == 0)
	{
		ret_status.code = ERROR;
		ret_status.error_message = "Database already exists";
		return ret_status;
	}
	else
	{
		fopen(file_path, "a");
	}

	cs165_log(stdout, "Database %s Created succesfully\n", db_name);

	ret_status.code = OK;
	return ret_status;
}

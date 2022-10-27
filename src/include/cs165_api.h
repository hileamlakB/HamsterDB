

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include "utils.h"
// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define MAX_PATH_NAME 256 // path name at max = dir/db.table.column
#define HANDLE_MAX_SIZE 64

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

typedef struct vector
{
    int *values;
    float value;
    int size;
} vector;

typedef enum variable_type
{
    POSITION_VECTOR, // position vector
    VALUE_VECTOR,    // int vectors
    FLOAT_VALUE      // single values
} variable_type;

typedef struct Variable
{
    char *name;
    vector result;
    variable_type type;
} Variable;

typedef vector pos_vec;
typedef vector val_vec;

typedef enum DataType
{
    INT,
    LONG,
    FLOAT
} DataType;

struct Comparator;
// struct ColumnIndex;

typedef enum PrintType
{
    TUPLE,
    SINGLE_FLOAT
} PrintType;

typedef struct Column
{
    char name[MAX_SIZE_NAME];

    char file_path[MAX_PATH_NAME];

    // mmaped file
    int fd;
    // fd should be initialized to 0 at first
    // When I am keeping track of the fd, I am taking into account
    // the number of fd's I could keep as well as the cost of
    // opening and closing files which is the alternative

    // These values should be initalized during column
    // creation
    char *file;       // mapped file
    size_t file_size; // contains the file_size

    // end should be serialized
    size_t end; // offset of the last meaningfull character

    size_t map_size; // size of the mapp

    // possible writing cache ontop of the mapped file for more control
    char *pending_i;
    size_t pending_i_t;
    char *pending_delete;
    size_t pending_delete_t;

    // You will implement column indexes later.
    // void *index;
    // struct ColumnIndex *index;
    // bool clustered;

    // metadata
    // the first index of these metadetas indicate
    // if the the data has been calculated before
    size_t meta_data_size; // number of pages used for metadata
    size_t count;
    size_t min[2];
    size_t max[2];
    size_t sum[2];

} Column;

extern Column empty_column;

/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/

typedef struct Table
{
    char name[MAX_SIZE_NAME];
    char *file_name;
    Column *columns;
    size_t col_count;
    size_t table_length;
    // this is an auto incrementing id
    size_t last_id;
    // what is table_length, the ai thinks it is, the number of rows in the table
    // for now I will take table_length to be the number of columns in the columns array
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db
{
    char name[MAX_SIZE_NAME];
    Table *tables;
    size_t tables_size;
    size_t tables_capacity; // size of tables array
} Db;

// Defines a comparator flag between two values.
typedef enum ComparatorType
{
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*
 * Declares the type of a result column,
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result
{
    size_t num_tuples;
    DataType data_type;
    void *payload;
} Result;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType
{
    RESULT,
    COLUMN
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer
{
    Result *result;
    Column *column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn
{
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle
{
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;
/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext
{
    GeneralizedColumnHandle *chandle_table;
    int chandles_in_use;
    int chandle_slots;
} ClientContext;

/**
 * comparator
 * A comparator defines a comparison operation over a column.
 **/
typedef struct Comparator
{
    long int p_low;  // used in equality and ranges.
    long int p_high; // used in range compares.
    GeneralizedColumn *gen_col;
    ComparatorType type1;
    ComparatorType type2;
    char *handle;
} Comparator;

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType
{
    CREATE,
    INSERT,
    LOAD,
    SELECT,
    SHUTDOWN,
    FETCH,
    PRINT,
    AVG,
    SUM,
    MIN,
    MAX,
    ADD,
    SUB,

} OperatorType;

typedef enum CreateType
{
    _DB,
    _TABLE,
    _COLUMN,
} CreateType;

/*
 * necessary fields for creation
 * "create_type" indicates what kind of object you are creating.
 * For example, if create_type == _DB, the operator should create a db named <<name>>
 * if create_type = _TABLE, the operator should create a table named <<name>> with <<col_count>> columns within db <<db>>
 * if create_type = = _COLUMN, the operator should create a column named <<name>> within table <<table>>
 */
typedef struct CreateOperator
{
    CreateType create_type;
    char name[MAX_SIZE_NAME];
    Db *db;
    Table *table;
    int col_count;
} CreateOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator
{
    Table *table;
    int *values;
} InsertOperator;
/*
 * necessary fields for insertion
 */
typedef struct LoadOperator
{
    Column *column;
    char *data;
    size_t size;
} LoadOperator;

typedef struct SelectOperator
{
    char *handler;
    int *result;
    int *low;
    int *high;
    Column *column;

} SelectOperator;

typedef struct FetchOperator
{
    char *handler;
    Variable *variable;
    Column *column;
} FetchOperator;

typedef struct PrintTuple
{
    int width;
    int height;
    Variable **data;
} PrintTuple;

typedef struct PrintOperator
{
    PrintType type;
    union
    {
        PrintTuple tuple;
        float value;
    } data;
} PrintOperator;

typedef struct AvgOperator
{
    char *handler;
    Variable *variable;

} AvgOperator;

/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields
{
    CreateOperator create_operator;
    InsertOperator insert_operator;
    LoadOperator load_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    AvgOperator avg_operator;
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator
{
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext *context;
} DbOperator;

extern Db *current_db;
extern Column empty_column;
extern Table empty_table;
extern linkedList *var_pool;

/*
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error.
 */
Status db_startup();

Status create_db(const char *db_name);

// loads a database from disk
Status load_db(const char *db_name);

Table *create_table(Db *db, const char *name, size_t num_columns, Status *status);

Column *create_column(Table *table, char *name, bool sorted, Status *ret_status);

Status shutdown_server();

char **execute_db_operator(DbOperator *query);
void db_operator_free(DbOperator *query);

// serilize.c

typedef struct serialize_data
{
    char *data;
    size_t size;

} serialize_data;

void generic_deserializer(void (*)(void *, char *, Status *), int, void *, Status *);

serialize_data serialize_column(Column *);
Column deserialize_column(char *, Status *);

serialize_data serialize_table(Table *);
Table deserialize_table(char *, int, Status *);

serialize_data serialize_db(Db *);
Db deserialize_db(int, Status *);

// load.c
int load_table(Db *, char *);
int load_column(char *, Table *, char *);

// read_write.c
void create_colf(Column *);
void write_col(Column *, char *, size_t);
void flush_col(Column *);

void flush_table(Table *);
void flush_db(Db *);

// select fetch
void select_col(Column *, char *, int *, int *);
void fetch_col(Column *, Variable *, char *);

// var_pool.c
void add_var(char *, vector, variable_type);
Variable *find_var(char *);

// server.c
char *print_tuple(PrintOperator);
void average(char *, Variable *);

#endif /* CS165_H */

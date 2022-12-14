

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
#include <pthread.h>
#include <stdatomic.h>
#include "data_structures.h"
#include <Utils/utils.h>
#include "message.h"

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define MAX_PATH_NAME 256 // path name at max = dir/db.table.column
#define HANDLE_MAX_SIZE 64
#define MAX_BATCH_OPERATIONS 4096

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
    size_t size;
} vector;

typedef vector pos_vec;
typedef vector val_vec;

typedef enum variable_type
{
    POSITION_VECTOR, // position vector
    VECTOR_CHAIN,    // position vector chain
    VALUE_VECTOR,    // int vectors
    FLOAT_VALUE,     // single float value
    INT_VALUE,       // single int value
    RANGE,           // range of values
} variable_type;

typedef struct Variable
{
    char *name;
    union
    {
        pos_vec values;
        linkedList *pos_vec_chain;
        double fvalue;
        long ivalue;
        int range[2];
    } result;

    bool is_sorted;    // only applicaple for pos_vecs
    bool is_clustered; // only applicaple for pos_vecs
    char sorting_column[MAX_SIZE_NAME];
    char sorting_column_path[MAX_PATH_NAME];

    size_t vec_chain_size;

    variable_type type;
    bool exists;
} Variable;

typedef enum DataType
{
    INT,
    LONG,
    FLOAT
} DataType;

struct Comparator;

typedef enum PrintType
{
    TUPLE,
    SINGLE_FLOAT,
    SINGLE_INT,
} PrintType;
typedef enum IndexType
{
    SORTED,
    ZONEMAP,
    BTREE,
    HASH,
    NO_INDEX
} IndexType;

typedef enum ClusterType
{
    CLUSTERED,
    UNCLUSTERED,
    NO_CLUSTER
} ClusterType;
typedef struct ColumnIndex
{
    char name[MAX_SIZE_NAME];
    IndexType type;
    ClusterType clustered;

    // mmaped file for sorted index
    int *sorted_file;
    Btree_node *btree; // if btree indexed

} ColumnIndex;

typedef struct ColumnMetaData
{
    // metadata
    double min;
    double max;
    double sum;
    // size_t sum[2];
} ColumnMetaData;

typedef struct Column
{
    char name[MAX_SIZE_NAME];
    char file_path[MAX_PATH_NAME];
    bool indexed;
    ColumnIndex index;

    // mmaped file
    int fd;
    char *file;
    size_t file_size;

    ColumnMetaData *metadata;
    int *data;
    size_t end;

} Column;

extern Column empty_column;
extern String empty_string;
extern String failed_string;

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
    char file_path[MAX_PATH_NAME];

    Column *columns;
    size_t col_count;
    size_t table_length;

    // this is an auto incrementing id
    size_t last_id;
    atomic_size_t rows; // number of tuples inserted

    // table tuple file
    int fd;
    int *file;
    size_t file_size;
    bool is_persistent;

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
    char file_path[MAX_PATH_NAME];

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
    BATCH_EXECUTE,
    BATCH_QUERY,
    INDEX,
    BATCH_LOAD,
    BATCH_LOAD_END,
    BATCH_LOAD_START,
    JOIN

} OperatorType;

typedef enum CreateType
{
    _DB,
    _TABLE,
    _COLUMN,
    _INDEX
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

    // this is to be used by the create(idx) command
    Column *column;
    IndexType index_type;
    ClusterType cluster_type;

} CreateOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator
{
    Table *table;
    char *value;
} InsertOperator;

typedef struct EntityAddress
{
    Db *db;
    Column *col;
    Table *table;
} EntityAddress;
/*
 * necessary fields for insertion
 */
typedef struct LoadOperator
{
    EntityAddress address;
    char *data;
    size_t size;

    // these is used to indicate loading is complete
    // and changes are to be persisted as correct
    bool complete;
} LoadOperator;

typedef enum SelectType
{
    SELECT_POS,
    SELECT_COL
} SelectType;

typedef struct SelectOperator
{

    char *handler;
    int *result;
    int *low;
    int *high;

    SelectType type;

    Variable *val_vec;
    Variable *pos_vec;

    Column *column;
    Table *table;

} SelectOperator;

typedef struct FetchOperator
{
    char *handler;
    Table *table;
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
        double fvalue;
        long int ivalue;
    } data;
} PrintOperator;

typedef enum SourceType

{
    VARIABLE_O,
    COLUMN_O
} SourceType;

typedef struct AvgOperator
{
    char *handler;
    Variable *variable;
    EntityAddress address;
    SourceType type;

} AvgOperator;

typedef struct MathOperator
{
    char *handler;
    Variable *operand_1;
    Variable *operand_2;
    OperatorType operation;
} MathOperator;

typedef struct MinMaxOperator
{
    char *handler;
    Variable *variable;
    OperatorType operation;

} MinMaxOperator;

#define PRIME_SIZE 7919
typedef enum join_type
{
    NESTED_LOOP,
    HASH_JOIN,
    SORT_MERGE
} join_type;

typedef struct JoinOperator
{
    char *handler1;
    char *handler2;

    Variable *pos1;
    Variable *pos2;

    Variable *val1;
    Variable *val2;

    join_type type;

} JoinOperator;

typedef struct ParallelLoad
{
    size_t load_size;
    String data;
} ParallelLoad;

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
    MathOperator math_operator;
    MinMaxOperator min_max_operator;
    JoinOperator join_operator;
    ParallelLoad parellel_load;
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

typedef struct batch_query
{
    bool mode;
    DbOperator *queries[MAX_BATCH_OPERATIONS];
    size_t num_queries;
} batch_query;

typedef struct ParallelLoader
{
    atomic_bool mode; // prevent more than one batch load at a time

    // atomic_size_t current_loc;

    // atomic_size_t given_tickets;
    // atomic_size_t current_ticket;
    // pthread_mutex_t ticket_lock;
    // pthread_cond_t ticket_cond;

    // pthread_t *writing_threads; // threads that are currenlty writing
    // size_t num_writig_threads;
    // size_t thread_capacity;
    // pthread_mutex_t thread_lock; // mutex for the writing threads

    Table *table; // table to load into

    pthread_mutex_t batch_load_mutex; // mutex load to modify the linked list
    pthread_cond_t batch_load_cond;   // condition variable for the linked list
    linkedList *data;
    linkedList *end;
    atomic_bool done;
    pthread_t writer_thread;

} ParallelLoader;

extern Db *current_db;
extern Column empty_column;
extern Table empty_table;
extern hashtable *var_pool;
extern pthread_mutex_t var_pool_lock;
extern batch_query batch;
extern ParallelLoader bload;

/*
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error.
 */
Status db_startup();

Status create_db(const char *db_name);

// loads a database from disk
int load_db(const char *db_name);
void free_db();

Table *create_table(Db *db, const char *name, size_t num_columns);

Column *create_column(Table *table, char *name, Status *ret_status);
ColumnIndex create_index(Table *table, Column *col, IndexType, ClusterType);
void populate_index(Table *tbl, Column *col);

void shutdown_server(DbOperator *);

void db_operator_free(DbOperator *query);

// serilize.c

typedef struct serialize_data
{
    char *data;
    size_t size;

} serialize_data;

void generic_deserializer(void (*)(void *, char *, Status *), int, void *, Status *);

serialize_data serialize_column(Column *);

serialize_data serialize_table(Table *);
Table deserialize_table(char *);

serialize_data serialize_db(Db *);
Db deserialize_db(char *);

// read_write.c

void write_load(Table *, Column *, char *, size_t, Status *);
void flush_col(Table *, Column *);

void flush_table(Table *);
void flush_db(Db *);

void update_col_end(Table *);

// select fetch

typedef struct thread_select_args
{
    int *low;
    int *high;
    char *file;
    size_t read_size;
    char *handle;
    int **result;
    size_t result_capacity;
    size_t result_size;
    size_t offset; // which part of the file is this thread reading
} thread_select_args;

// void *select_col(Table *, Column *, char *, int *, int *, Status *);
void *select_col(void *args);
void select_pos(Variable *, Variable *, char *, int *, int *, Status *);
void fetch_col(Table *, Column *, Variable *, char *, Status *);

// var_pool.c
void add_var(Variable *);
void free_var(void *var);
Variable *find_var(char *);
void free_var_pool();

// server.c

String print_tuple(PrintOperator);
void average(char *, Variable *);
void sum(AvgOperator);

void add(char *, Variable *, Variable *);
void sub(char *, Variable *, Variable *);
void MinMax(MinMaxOperator, Status *);

void join(DbOperator *query);

String batch_execute(DbOperator **queries, size_t n);
String batch_execute2(DbOperator **queries, size_t n);
Table *lookup_table(Db *, char *);
Column *lookup_column(Table *, char *);

typedef struct select_args
{
    int *low; // lower int boundery
    int *high;
    Column *col; // searched column
    Table *tbl;  // searched table
    int *file;   // a sectino of the file mapped for the column to be searched
    size_t read_size;
    char *handle;       // handle for a certain search query
    size_t offset;      // which section of the file is passes, to be used in multithreaded sorting
    int *result;        // point to a location for result
    size_t result_size; // point to a location for result size

    atomic_bool *is_done; // point to a location for is_done

} select_args;

typedef struct batch_select_args
{

    int *file;
    size_t offset;
    size_t read_size;
    size_t n;

    // number of batched args
    char **handle;
    int **low;
    int **high;
    int **result;
    size_t *result_size;

    atomic_bool *is_done;

} batch_select_args;

Variable generic_select(select_args);
Variable sorted_select(select_args);
void shared_scan(batch_select_args args);
Variable btree_select(select_args);
Variable (*choose_algorithm(select_args args))(select_args);

// available threads
typedef struct num_threads
{
    size_t threads;
    pthread_mutex_t lock;
} num_threads;

#endif /* CS165_H */

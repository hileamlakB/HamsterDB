#include <cs165_api.h>
#include <Serializer/serialize.h>
#include "Utils/utils.h"
#include "data_structures.h"
#include "Loader/load.h"
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

ParallelLoader bload = {
    .mode = false,
    .done = false,
};

int insert_col(Table *table, Column *col, int value)
{

    if (map_col(table, col, 0) < -1)
    {
        return -1;
    }

    col->data[col->end] = value;
    col->end += 1;
    return 0;
}
// // insert to database
int insert(Table *table, String values)
{
    // the size of the columns must be remaped before
    // this function is called
    // log_info("table rows before insert: %d\n", table->rows);
    size_t index = 0;
    while (index < values.len)
    {

        for (size_t i = 0; i < table->col_count; i++)
        {

            int num = 0;
            int sign = 1;
            while (index < values.len && values.str[index] != ',' && values.str[index] != '\n' && values.str[index] != '\0')
            {
                if (values.str[index] == ' ')
                {
                    index++;
                    continue;
                }

                if (values.str[index] == '+' || values.str[index] == '-')
                {
                    sign *= -1;
                    index++;
                    continue;
                }

                if (values.str[index] < '0' || values.str[index] > '9')
                {
                    break;
                }

                num = num * 10 + (values.str[index] - '0');
                index++;
            }
            num *= sign;
            index++;
            table->columns[i].data[table->columns[i].end] = num;
            table->columns[i].end += 1;

            if (table->columns[i].metadata->max < num)
            {
                table->columns[i].metadata->max = num;
            }
            if (table->columns[i].metadata->min > num)
            {
                table->columns[i].metadata->min = num;
            }
            table->columns[i].metadata->sum += num;

            if (table->is_persistent)
            {
                table->file[table->rows * table->col_count + i] = num;
            }
        }
        table->rows += 1;
        if (table->is_persistent && (table->rows + 1) * table->col_count * sizeof(int) > table->file_size)
        {
            table->file_size *= 2;
            lseek(bload.table->fd, table->file_size, SEEK_SET);
            write(bload.table->fd, " ", 1);
            // remap the file
            table->file = mmap(table->file, table->file_size, PROT_READ | PROT_WRITE, MAP_SHARED, table->fd, 0);
        }
    }
    return 0;
}

void *batch_writer(void *args)
{
    (void)args;
    // sleep and wait on the conditional mutex batch_load_cond
    // and when it awakens if there is adata to be loaded in bload.data
    // then load it into the table and goes back to sleep
    while (true)
    {
        linkedList *node = NULL;
        pthread_mutex_lock(&bload.batch_load_mutex);
        while (bload.data == NULL)
        {
            if (bload.done)
            {
                pthread_mutex_unlock(&bload.batch_load_mutex);
                return NULL;
            }
            pthread_cond_wait(&bload.batch_load_cond, &bload.batch_load_mutex);
        }
        // pop the top of the linked list
        node = bload.data;
        bload.data = bload.data->next;

        pthread_mutex_unlock(&bload.batch_load_mutex);

        String *data = (String *)node->data;

        insert(bload.table, *data);
        free(data->str);
        free(node->data);
        free(node);
    }
}

void batch_load(DbOperator *query)
{

    linkedList *new_list = malloc(sizeof(linkedList));
    new_list->next = NULL;
    new_list->data = malloc(sizeof(String));
    *(String *)new_list->data = query->operator_fields.parellel_load.data;

    pthread_mutex_lock(&bload.batch_load_mutex);
    if (bload.data == NULL)
    {
        bload.data = new_list;
        bload.end = new_list;
    }
    else
    {
        bload.end->next = new_list;
        bload.end = new_list;
    }
    pthread_mutex_unlock(&bload.batch_load_mutex);
    // awaken writer thread

    pthread_cond_broadcast(&bload.batch_load_cond);

    // get a new ticket
    // size_t ticket = atomic_fetch_add(&bload.given_tickets, 1);
    // scheduled_write *swrite = malloc(sizeof(scheduled_write));
    // swrite->data = query->operator_fields.parellel_load.data;
    // swrite->ticket = ticket;

    // starta thread that will sleep until its turn reachs and execute

    // pthread_mutex_lock(&bload.thread_lock);
    // if (bload.thread_capacity == bload.num_writig_threads)
    // {
    //     bload.thread_capacity += 1;
    //     bload.thread_capacity *= 2;
    //     bload.writing_threads = realloc(bload.writing_threads, bload.thread_capacity * sizeof(pthread_t));
    // }
    // pthread_create(bload.writing_threads + bload.num_writig_threads,
    //                NULL,
    //                preprocess_load,
    //                swrite);
    // bload.num_writig_threads += 1;
    // pthread_mutex_unlock(&bload.thread_lock);
}

void prepare_load(DbOperator *query)
{

    // goes through each column and maps it to much incoming size
    size_t incoming_size = query->operator_fields.parellel_load.load_size;
    for (size_t i = 0; i < bload.table->col_count; i++)
    {
        if (bload.table->columns[i].indexed && bload.table->columns[i].index.clustered == CLUSTERED && bload.table->is_persistent != true)
        {

            bload.table->is_persistent = true;
            // create a tuple file for the table
            char *tuple_file_name = catnstr(2, bload.table->file_path, ".tuple");
            bload.table->fd = open(tuple_file_name, O_CREAT | O_RDWR, 0666);
            free(tuple_file_name);

            // expand file
            lseek(bload.table->fd, MILLION * sizeof(int) * bload.table->col_count, SEEK_SET);
            write(bload.table->fd, " ", 1);

            // map the tuple file
            bload.table->file = mmap(NULL, MILLION * sizeof(int) * bload.table->col_count, PROT_READ | PROT_WRITE, MAP_SHARED, bload.table->fd, 0);
            bload.table->file_size = MILLION * sizeof(int) * bload.table->col_count;
        }
        remap_col(bload.table, bload.table->columns + i, incoming_size);
    }

    // create the writer thread
    pthread_create(&bload.writer_thread, NULL, batch_writer, NULL);

    return;
}

void finish_load(DbOperator *query)
{
    (void)query;
    // wait for the linked list to be empty

    bload.done = true;
    // awaken the writer thread
    pthread_cond_broadcast(&bload.batch_load_cond);

    // wait for the writer thread to close
    pthread_join(bload.writer_thread, NULL);

    // create indexs

    Table *table = bload.table;
    for (size_t i = 0; i < table->col_count; i++)
    {
        populate_index(table, &table->columns[i]);
    }
}
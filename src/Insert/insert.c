#include <cs165_api.h>
#include <Serializer/serialize.h>
#include "Utils/utils.h"
#include <data_structures.h>
#include "Loader/load.h"
#include <stdatomic.h>
#include <pthread.h>

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

    // log_info("table rows before insert: %d\n", table->rows);
    size_t index = 0;
    while (index < values.len)
    {

        for (size_t i = 0; i < table->col_count; i++)
        {

            int num = 0;
            int sign = 1;
            while (values.str[index] != ',' && values.str[index] != '\n' && values.str[index] != '\0')
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
        }
        table->rows += 1;
    }
    return 0;
}

// void *parallel_load(void *arg)
// {

//     scheduled_write *sw = (scheduled_write *)arg;

//     insert(bload.table, sw->data);

//     free(sw->data);
//     free(sw);
//     return NULL;
// }

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

// void *preprocess_load(void *data)
// {
//     scheduled_write *swrite = (scheduled_write *)data;
//     // sleep and wait on a condition until ticket is reached
//     pthread_mutex_lock(&bload.ticket_lock);
//     while (bload.current_ticket != swrite->ticket)
//     {
//         pthread_cond_wait(&bload.ticket_cond, &bload.ticket_lock);
//     }
//     pthread_mutex_unlock(&bload.ticket_lock);

//     // execute the load
//     parallel_load(swrite);

//     // increment the ticket and signal the next thread
//     pthread_mutex_lock(&bload.ticket_lock);
//     bload.current_ticket += 1;
//     pthread_cond_signal(&bload.ticket_cond);
//     pthread_mutex_unlock(&bload.ticket_lock);

//     return NULL;
// }

void prepare_load(DbOperator *query)
{
    // goes through each column and maps it to much incoming size
    size_t incoming_size = query->operator_fields.parellel_load.load_size;
    for (size_t i = 0; i < bload.table->col_count; i++)
    {
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

    // load indexs

    Table *table = bload.table;
    for (size_t i = 0; i < table->col_count; i++)
    {
        populate_index(table, &table->columns[i]);
    }
}
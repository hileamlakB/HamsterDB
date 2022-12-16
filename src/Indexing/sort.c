#include "sort.h"
#include "thread_pool.h"
#include "sort.h"
#include <unistd.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sys/mman.h>
#include <string.h>

#include <errno.h>
#include "Utils/utils.h"
#include "cs165_api.h"

#include <Serializer/serialize.h>
#include <assert.h>

// compare two integers that are represented as a tuple
// of two strings, where the first string is the integer
// and the second string is the integer's location
int compare_external_ints(const void *a, const void *b)
{
    int num_a = atoi((char *)a), num_b = atoi((char *)b);

    size_t loc_a = atoi((char *)(a) + MAX_INT_LENGTH + 1),
           loc_b = atoi((char *)(b) + MAX_INT_LENGTH + 1);

    if (num_a == num_b)
    {
        if (loc_a < loc_b)
        {
            return -1;
        }
        else if (loc_a > loc_b)
        {
            return 1;
        }

        return 0;
    }

    if (num_a < num_b)
    {
        return -1;
    }
    return 1;
}

int compare_int_loc_tuple(const void *a, const void *b)
{
    // compare two 2 dimentsional integer arrays

    int x[2] = {((int *)a)[0], ((int *)a)[1]};
    int y[2] = {((int *)b)[0], ((int *)b)[1]};

    if (x[0] > y[0])
    {
        return 1;
    }
    else if (x[0] < y[0])
    {
        return -1;
    }

    if (x[1] > y[1])
    {
        return 1;
    }
    else if (x[1] < y[1])
    {
        return -1;
    }

    return 0;
}

// compare two integers that are represented as a tuple
// using the compare_external_ints function
// and sort them using the qsort function
void *sort_block(void *sarg)
{
    sort_args *arg = (sort_args *)sarg;
    qsort(arg->array, arg->size, sizeof(int[2]), arg->compare);
    arg->is_sorted = true;
    return NULL;
}

// merge two sorted runs and write them back to the first array
void *merge(void *sarg)
{
    merge_args *arg = (merge_args *)sarg;
    int *array_1 = arg->array1,
        *array_2 = arg->array2;

    size_t size_1 = arg->size1,
           size_2 = arg->size2,
           size = (size_1 + size_2) * (sizeof(int[2]));

    // const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);
    // size += PAGE_SIZE - (size - ((size / PAGE_SIZE) * PAGE_SIZE));

    tmp_file tmp = create_tmp_file("merge", size, true, true, true);

    // map the file to memory
    int *result = (int *)tmp.map;
    size_t p1 = 0, p2 = 0;

    while (p1 < size_1 && p2 < size_2)
    {

        if (compare_int_loc_tuple(array_1 + (2 * p1), array_2 + (2 * p2)) < 0)
        {
            memcpy(result, array_1 + p1 * 2, sizeof(int[2]));
            p1 += 1;
        }
        else
        {
            memcpy(result, array_2 + p2 * 2, sizeof(int[2]));
            p2 += 1;
        }
        result += 2;
    }

    if (p1 < size_1)
    {

        memcpy(result, array_1 + p1 * 2, (size_1 - p1) * sizeof(int[2]));
        result += (size_1 - p1) * 2;
    }
    else
    {
        memcpy(result, array_2 + p2 * 2, (size_2 - p2) * sizeof(int[2]));
        result += (size_1 - p2) * 2;
    }

    // copy the result back to the first array
    free(tmp.file_name);
    memcpy(array_1, tmp.map, size);
    munmap(tmp.map, size);
    close(tmp.fd);

    arg->is_merged = true;

    return NULL;
}

// implemntation of parralized external two-way merge sort
// the mapped_file to be sorted has to satisfy the following conditions:
// 1. Contains numbers in the form n1,n1_loc,n2,n2_loc,...,nn,nn_loc

// NOTE: This function is for internal use only
void external_sort(int *maped_file, int file_size)
{

    // create runs and sort them using the sort and merge threads
    size_t PAGE_SIZE = 4 * sysconf(_SC_PAGESIZE);
    size_t num_runs = file_size / PAGE_SIZE;
    if (file_size % PAGE_SIZE != 0)
    {
        num_runs++;
    }

    size_t num_ints = PAGE_SIZE / sizeof(int[2]);

    sort_args *args = malloc(sizeof(sort_args) * num_runs);
    pthread_t *threads = malloc(sizeof(pthread_t) * num_runs);
    for (size_t i = 0; i < num_runs; i++)
    {
        num_ints = min(
            num_ints,
            (file_size - i * PAGE_SIZE) / (sizeof(int[2])));

        args[i] = (sort_args){
            .array = maped_file + i * PAGE_SIZE,
            .size = num_ints,
            .compare = compare_int_loc_tuple,
            .is_sorted = false};

        pthread_create(&threads[i], NULL, sort_block, &args[i]);
        pthread_detach(threads[i]);
    }

    atomic_size_t *individual_run_sizes = malloc(sizeof(atomic_size_t) * num_runs);
    int **runs = malloc(sizeof(int *) * num_runs);

    // wait for the sort threads to finish
    for (size_t i = 0; i < num_runs; i++)
    {
        while (!args[i].is_sorted)
        {
            sched_yield();
        }

        individual_run_sizes[i] = args[i].size;
        runs[i] = args[i].array;
    }

    if (num_runs == 1)
    {
        return;
    }

    while (num_runs > 1)
    {

        size_t odd = num_runs % 2;
        merge_args *margs = malloc(sizeof(merge_args) * num_runs / 2);
        pthread_t *mthreads = malloc(sizeof(merge_args) * num_runs / 2);
        int **new_runs = malloc(sizeof(int *) * num_runs / 2 + odd);

        for (size_t i = 0, j = 0; i < num_runs - (odd); i += 2, j++)
        {
            margs[j] = (merge_args){
                .array1 = runs[i],
                .size1 = individual_run_sizes[i],
                .array2 = runs[i + 1],
                .size2 = individual_run_sizes[i + 1],
                .is_merged = false,
            };
            pthread_create(&mthreads[j], NULL, merge, &margs[j]);
            pthread_detach(mthreads[j]);
        }

        for (size_t i = 0; i < num_runs / 2; i++)
        {
            while (!margs[i].is_merged)
            {
                sched_yield();
            }
            individual_run_sizes[i] = margs[i].size1 + margs[i].size2;
            new_runs[i] = margs[i].result;
        }

        if (odd)
        {
            individual_run_sizes[num_runs / 2] = individual_run_sizes[num_runs - 1];
            new_runs[num_runs / 2] = runs[num_runs - 1];
        }
        free(runs);
        runs = new_runs;
        num_runs = num_runs / 2 + odd;
    }

    free(individual_run_sizes);
    free(args);
    free(threads);
}
void simple_sort(int *maped_file, int file_size)
{
    qsort(maped_file, file_size, sizeof(int[2]), compare_int_loc_tuple);
}
// this function prepraes a nomral integer file to be sorted using the external
// sort
int prepare_file(Table *tbl, Column *col, bool simple)
{

    char *file_name = col->file_path;
    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);

    // open the original file
    int fd = open(file_name, O_RDONLY, 0666);
    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;

    size_t data_per_page = PAGE_SIZE / (2 * (MAX_INT_LENGTH + 1));
    size_t required_pages = tbl->rows / data_per_page;

    size_t new_size = required_pages * PAGE_SIZE + (tbl->rows % data_per_page) * (2 * (MAX_INT_LENGTH + 1));

    // create a new file
    char *to_sort_col = catnstr(2, file_name, "_to_sort");
    int fd_to_sort = open(to_sort_col, O_RDWR | O_CREAT | O_TRUNC, 0666);
    lseek(fd_to_sort, new_size, SEEK_SET);
    write(fd_to_sort, " ", 1);

    // map both the original file and the new file
    char *orignal_file = mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
    char *to_sort = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_to_sort, 0);
    if (orignal_file == MAP_FAILED || to_sort == MAP_FAILED)
    {
        return -1;
    }

    // get the number of pages used for metadata

    size_t block = PAGE_SIZE / (MAX_INT_LENGTH + MAX_INT_LENGTH + 2);
    size_t diff = PAGE_SIZE % (MAX_INT_LENGTH + MAX_INT_LENGTH + 2);

    // copy the original file to the new file along the location
    // of the number in the original file in the form n1,n1_loc,n2,n2_loc,...,nn,nn_loc
    // satisfying the page requirment
    size_t i = atoi(orignal_file) * PAGE_SIZE, j = 0, k = 0;
    size_t int_location = 0;
    bool is_location = false;
    while (i - atoi(orignal_file) * PAGE_SIZE < tbl->rows * (MAX_INT_LENGTH + 1))
    {
        if (k == block && !simple)
        {
            // add the padding and move to the next page
            for (size_t l = 0; l < diff; l++)
            {
                to_sort[j] = ' ';
                j++;
            }
            k = 0;
        }

        if (!is_location)
        {
            memcpy(to_sort + j, orignal_file + i, MAX_INT_LENGTH + 1);
            j += MAX_INT_LENGTH + 1;
            i += MAX_INT_LENGTH + 1;
        }
        else
        {
            sprintf(to_sort + j, "%012d,", (int)int_location);
            j += MAX_INT_LENGTH + 1;
            k++;
            int_location += 1;
        }
        is_location = !is_location;
    }
    sprintf(to_sort + j, "%012d,", (int)int_location);
    free(to_sort_col);
    munmap(orignal_file, file_size);
    munmap(to_sort, new_size);
    close(fd);
    close(fd_to_sort);
    return 0;
}

// extracts the integers and the files into individual files
// after external sort is done
int extract_sorted(Table *tbl, Column *col)
{

    char *file_name = col->file_path;
    char *sorted_mk = catnstr(2, file_name, "_to_sort");
    int fd_sorted_mk = open(sorted_mk, O_RDONLY, 0666);

    int *sorted_file = mmap(NULL, 2 * tbl->rows * sizeof(int), PROT_READ, MAP_PRIVATE, fd_sorted_mk, 0);

    char *sorted_filename = catnstr(2, file_name, ".sorted");
    char *map_colname = catnstr(2, file_name, ".map");

    int fd_sorted = open(sorted_filename, O_RDWR | O_CREAT | O_TRUNC, 0666);
    int fd_map = open(map_colname, O_RDWR | O_CREAT | O_TRUNC, 0666);

    lseek(fd_sorted, tbl->rows * sizeof(int), SEEK_SET);
    write(fd_sorted, " ", 1);

    lseek(fd_map, tbl->rows * sizeof(int), SEEK_SET);
    write(fd_map, " ", 1);

    int *sorted = mmap(NULL, tbl->rows * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd_sorted, 0);
    int *map = mmap(NULL, tbl->rows * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd_map, 0);

    size_t i = 0, j = 0;
    while (i < tbl->rows * 2)
    {
        sorted[j] = sorted_file[i];
        map[j] = sorted_file[i + 1];
        i += 2;
        j++;
    }

    munmap(sorted_file, 2 * tbl->rows * sizeof(int));
    munmap(sorted, tbl->rows * sizeof(int));
    munmap(map, tbl->rows * sizeof(int));

    free(sorted_filename);
    free(map_colname);
    free(sorted_mk);

    close(fd_sorted);
    close(fd_map);
    return 0;
}

// sorts the file using the external sort
// and writes the result along with a map file
int sort_col(Table *tbl, Column *col)
{

    // thread_pool_init(1000);

    map_col(tbl, col, 0);
    char *file_name = col->file_path;

    char *to_sort_col = catnstr(2, file_name, "_to_sort");
    int fd_to_sort = open(to_sort_col, O_RDWR | O_CREAT, 0666);

    // expand the file
    lseek(fd_to_sort, tbl->rows * sizeof(int) * 2, SEEK_SET);
    write(fd_to_sort, " ", 1);

    int *to_sort = mmap(NULL, tbl->rows * sizeof(int) * 2, PROT_READ | PROT_WRITE, MAP_SHARED, fd_to_sort, 0);
    // copy data to the to_sort file along with location
    for (size_t i = 0, j = 0; i < tbl->rows; i++, j += 2)
    {
        to_sort[j] = col->data[i];
        to_sort[j + 1] = i;
    }

    simple_sort(to_sort, tbl->rows);

    // external_sort(to_sort, tbl->rows * sizeof(int[2]));

    munmap(to_sort, tbl->rows * sizeof(int));
    close(fd_to_sort);

    if (extract_sorted(tbl, col) < 0)
    {
        return -1;
    }

    // delete to_sort file
    remove(to_sort_col);
    free(to_sort_col);
    // destroy_thread_pool();

    return 0;
}

// create a tuple of multiple files
// and write it to a temporary file
tmp_file create_tuple(size_t n, size_t tuple_size, int **tuple_elements)
{

    tmp_file res = create_tmp_file("tuple", tuple_size * n * sizeof(int), true, true, true);
    int *tuple = res.map;

    size_t i = 0;
    // create a tuple

    for (size_t k = 0; k < n; k++)
    {
        for (size_t j = 0; j < tuple_size; j++)
        {

            tuple[i] = tuple_elements[j][k];
            i += 1;
        }
    }

    return res;
}

void separate_tuple(Table *table, Column *idx_column, tmp_file tuple, size_t n)
{

    int **res = calloc(table->col_count, sizeof(int *));
    int fds[table->col_count - 1];
    for (size_t i = 0; i < table->col_count; i++)
    {
        if (&table->columns[i] == idx_column)
        {
            continue;
        }
        char *file_name = catnstr(3, table->columns[i].file_path, ".clustered.", idx_column->name);
        //    open and  map the file
        int fd = open(file_name, O_RDWR | O_CREAT | O_TRUNC, 0666);
        // expand the file
        lseek(fd, table->rows * sizeof(int), SEEK_SET);
        write(fd, " ", 1);

        fds[i] = fd;
        res[i] = mmap(NULL, table->rows * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        free(file_name);
    }

    int *tuple_map = tuple.map;
    for (size_t i = 0; i < n; i++)
    {
        for (size_t j = 0; j < table->col_count; j++)
        {
            if (&table->columns[j] == idx_column)
            {
                continue;
            }
            res[j][i] = tuple_map[i * (table->col_count - 1) + j];
        }
    }

    // unmap and close all files
    for (size_t i = 0; i < table->col_count - 1; i++)
    {
        if (&table->columns[i] == idx_column)
        {
            continue;
        }
        close(fds[i]);
        munmap(res[i], n * sizeof(int));
    }

    free(res);
}

// propagate_sort, sorts all other columns based on
// the sorting order of the first column
void propagate_sort(Table *tbl, Column *idx_column)
{

    // open the map file
    char *file_path = catnstr(2, idx_column->file_path, ".map");
    int fd = open(file_path, O_RDONLY, 0666);
    int *map = mmap(NULL, tbl->rows * sizeof(int), PROT_READ, MAP_SHARED, fd, 0);
    free(file_path);

    // open the tuple file
    assert(tbl->file);
    int *tmap = tbl->file;

    int **res = calloc(tbl->col_count, sizeof(int *));
    int fds[tbl->col_count - 1];
    for (size_t i = 0; i < tbl->col_count; i++)
    {
        if (&tbl->columns[i] == idx_column)
        {
            continue;
        }
        // create the clustered files related to each non index column
        char *file_name = catnstr(3, tbl->columns[i].file_path, ".clustered.", idx_column->name);
        int fd = open(file_name, O_RDWR | O_CREAT | O_TRUNC, 0666);
        lseek(fd, tbl->rows * sizeof(int), SEEK_SET);
        write(fd, " ", 1);

        fds[i] = fd;
        res[i] = mmap(NULL, tbl->rows * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        free(file_name);
    }

    size_t i = 0;
    while (i < tbl->rows)
    {

        int old_location = map[i];

        // copy the tuple to the new files
        for (size_t j = 0; j < tbl->col_count; j++)
        {
            if (&tbl->columns[j] == idx_column)
            {
                continue;
            }
            res[j][i] = tmap[old_location * tbl->col_count + j];
        }

        i += 1;
    }

    for (size_t i = 0; i < tbl->col_count; i++)
    {
        if (&tbl->columns[i] == idx_column)
        {
            continue;
        }
        munmap(res[i], tbl->rows * sizeof(int));
        close(fds[i]);
    }

    free(res);
}

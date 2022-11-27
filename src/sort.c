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
#include "utils.h"
#include "cs165_api.h"

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

// compare two integers that are represented as a tuple
// using the compare_external_ints function
// and sort them using the qsort function
void *sort_block(void *sarg)
{
    sort_args *arg = (sort_args *)sarg;
    qsort(arg->array, arg->size, sizeof(char[MAX_INT_LENGTH + MAX_INT_LENGTH + 2]), arg->compare);
    arg->is_sorted = true;
    return NULL;
}

// merge two sorted runs and write them back to the first array
void *merge(void *sarg)
{
    merge_args *arg = (merge_args *)sarg;
    char *array_1 = (char *)arg->array1,
         *p1 = array_1,
         *array_2 = (char *)arg->array2,
         *p2 = array_2;

    size_t size_1 = arg->size1,
           size_2 = arg->size2,
           size = (size_1 + size_2) * (MAX_INT_LENGTH + MAX_INT_LENGTH + 2);

    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);
    size += PAGE_SIZE - (size - ((size / PAGE_SIZE) * PAGE_SIZE));

    tmp_file tmp = create_tmp_file("merge", size, true, true);

    // map the file to memory
    char *result = tmp.map;

    while (
        (size_t)(p1 - array_1) < size_1 * (MAX_INT_LENGTH + MAX_INT_LENGTH + 2) &&
        (size_t)(p2 - array_2) < size_2 * (MAX_INT_LENGTH + MAX_INT_LENGTH + 2))
    {
        if (p1[0] == ' ')
        {
            p1 += 1;
            continue;
        }
        if (p2[0] == ' ')
        {
            p2 += 1;
            continue;
        }
        // the runs might create empty spaces so we need to skip them
        if (compare_external_ints(p1, p2) < 0)
        {
            memcpy(result, p1, MAX_INT_LENGTH + MAX_INT_LENGTH + 2);
            p1 += MAX_INT_LENGTH + MAX_INT_LENGTH + 2;
        }
        else
        {
            memcpy(result, p2, MAX_INT_LENGTH + MAX_INT_LENGTH + 2);
            p2 += MAX_INT_LENGTH + MAX_INT_LENGTH + 2;
        }
        result += MAX_INT_LENGTH + MAX_INT_LENGTH + 2;
    }

    if ((size_t)(p1 - array_1) < size_1 * (MAX_INT_LENGTH + MAX_INT_LENGTH + 2))
    {

        memcpy(result, p1, size_1 * (MAX_INT_LENGTH + MAX_INT_LENGTH + 2) - (p1 - array_1));
        result += size_1 * (MAX_INT_LENGTH + MAX_INT_LENGTH + 2) - (p1 - array_1);
    }
    else
    {
        memcpy(result, p2, size_2 * (MAX_INT_LENGTH + MAX_INT_LENGTH + 2) - (p2 - array_2));
        result += size_2 * (MAX_INT_LENGTH + MAX_INT_LENGTH + 2) - (p2 - array_2);
    }

    size_t fill_size = size - ((size_1 + size_2) * (MAX_INT_LENGTH + MAX_INT_LENGTH + 2));

    while (fill_size > 0)
    {

        *result = ' ';
        result++;
        fill_size--;
    }

    // copy the result back to the first array
    free(tmp.file_name);
    memcpy(array_1, tmp.map, size);
    munmap(tmp.map, size);
    close(tmp.fd);

    *arg->is_merged = true;

    return NULL;
}

// implemntation of parralized external two-way merge sort
// the mapped_file to be sorted has to satisfy the following conditions:
// 1. Contains numbers in the form n1,n1_loc,n2,n2_loc,...,nn,nn_loc
// 2. The numbers should fit in a page (4KB), if the don't the page should be
//    padded with the empty space the next number should start in the next page
// 3. A page must be filled before going to the next page
// NOTE: This function is for internal use only
void external_sort(char *maped_file, int file_size)
{

    // create runs and sort them using the sort thread
    // merge the runs

    size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);
    size_t num_runs = file_size / PAGE_SIZE + 1;

    size_t num_ints = PAGE_SIZE / (MAX_INT_LENGTH + MAX_INT_LENGTH + 2);

    sort_args args[num_runs];
    for (size_t i = 0; i < num_runs; i++)
    {
        num_ints = min(
            num_ints,
            (file_size - i * PAGE_SIZE) / (MAX_INT_LENGTH + MAX_INT_LENGTH + 2));

        args[i] = (sort_args){
            .array = maped_file + i * PAGE_SIZE,
            .size = num_ints,
            .compare = compare_external_ints,
            .is_sorted = false};

        add_job(sort_block, &args[i]);
    }

    if (num_runs == 1)
    {
        return;
    }

    atomic_size_t individual_run_sizes[num_runs];
    for (size_t i = 0; i < num_runs; i++)
    {
        individual_run_sizes[i] = args[i].size;
    }

    // size_t pass = 1;
    size_t num_blocks = 1;
    while (num_runs > 1)
    {

        size_t odd = num_runs % 2;
        merge_args margs[num_runs / 2];
        atomic_bool is_merged[num_runs / 2];

        for (size_t i = 0, j = 0; i < num_runs - (odd); i += 2, j++)
        {

            is_merged[j] = false;
            margs[j] = (merge_args){
                .array1 = maped_file + i * (num_blocks * PAGE_SIZE),
                .size1 = individual_run_sizes[i],
                .array2 = maped_file + (i + 1) * (num_blocks * PAGE_SIZE),
                .size2 = individual_run_sizes[i + 1],
                .is_merged = &is_merged[j]};

            add_job(merge, &margs[j]);
        }

        for (size_t i = 0; i < num_runs / 2; i++)
        {
            while (!is_merged[i])
            {

                sleep(0);
            }
            individual_run_sizes[i] = margs[i].size1 + margs[i].size2;
        }
        if (odd)
        {
            individual_run_sizes[num_runs / 2] = individual_run_sizes[num_runs - 1];
        }
        num_runs = num_runs / 2 + odd;
        num_blocks *= 2;
    }
}

// this function prepraes a nomral integer file to be sorted using the external
// sort
int prepare_file(Table *tbl, Column *col)
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
    while (i - atoi(orignal_file) * PAGE_SIZE < tbl->rows * (MAX_INT_LENGTH + 1))
    {
        if (k == block)
        {
            // add the padding and move to the next page
            for (size_t l = 0; l < diff; l++)
            {
                to_sort[j] = ' ';
                j++;
            }
            k = 0;
        }

        if (orignal_file[i] == ',')
        {
            to_sort[j] = ',';
            j++;
            sprintf(to_sort + j, "%012d,", (int)int_location);
            j += MAX_INT_LENGTH + 1;
            k++;
            i++;
            int_location += 1;
        }
        else
        {
            to_sort[j] = orignal_file[i];
            j++;
            i++;
        }
    }
    to_sort[j] = '\0';
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
    struct stat st;
    fstat(fd_sorted_mk, &st);
    size_t file_size = st.st_size;

    char *sorted_file = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd_sorted_mk, 0);

    char *sorted_filename = catnstr(2, file_name, ".sorted");
    char *map_filename = catnstr(2, file_name, ".map");

    int fd_sorted = open(sorted_filename, O_RDWR | O_CREAT | O_TRUNC, 0666);
    int fd_map = open(map_filename, O_RDWR | O_CREAT | O_TRUNC, 0666);

    lseek(fd_sorted, file_size / 2, SEEK_SET);
    write(fd_sorted, " ", 1);

    lseek(fd_map, file_size / 2, SEEK_SET);
    write(fd_map, " ", 1);

    char *sorted = mmap(NULL, tbl->rows * (MAX_INT_LENGTH + 1), PROT_READ | PROT_WRITE, MAP_SHARED, fd_sorted, 0);
    char *map = mmap(NULL, tbl->rows * (MAX_INT_LENGTH + 1), PROT_READ | PROT_WRITE, MAP_SHARED, fd_map, 0);

    size_t i = 0, s = 0, m = 0;
    bool is_location = false;
    while (i < tbl->rows * (2 * (MAX_INT_LENGTH + 1)))
    {
        if (is_location)
        {
            memcpy(map + m, sorted_file + i, MAX_INT_LENGTH + 1);
            m += MAX_INT_LENGTH + 1;
        }
        else
        {
            memcpy(sorted + s, sorted_file + i, MAX_INT_LENGTH + 1);
            s += MAX_INT_LENGTH + 1;
        }
        i += MAX_INT_LENGTH + 1;
        is_location = !is_location;
    }
    munmap(sorted_file, file_size);
    munmap(sorted, file_size / 2);
    munmap(map, file_size / 2);

    free(sorted_filename);
    free(map_filename);
    free(sorted_mk);

    close(fd_sorted);
    close(fd_map);
    return 0;
}

// sorts the file using the external sort
// and writes the result along with a map file
int sort_col(Table *tbl, Column *col)
{

    thread_pool_init(5);

    // const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);
    char *file_name = col->file_path;

    int prep = prepare_file(tbl, col);
    if (prep != 0)
    {
        return -1;
    }

    char *to_sort_col = catnstr(2, file_name, "_to_sort");
    int fd_to_sort = open(to_sort_col, O_RDWR, 0666);
    struct stat st;
    fstat(fd_to_sort, &st);
    size_t file_size = st.st_size;

    char *to_sort = mmap(NULL, file_size + 1, PROT_READ | PROT_WRITE, MAP_SHARED, fd_to_sort, 0);
    to_sort[file_size + 1] = '\0';
    if (to_sort == MAP_FAILED)
    {
        return -1;
    }

    external_sort(to_sort, file_size);

    munmap(to_sort, file_size + 1);
    close(fd_to_sort);

    if (true)
    {
        if (extract_sorted(tbl, col) < 0)
        {
            return -1;
        }
    }
    // remove(to_sort_col);
    free(to_sort_col);
    destroy_thread_pool();

    return 0;
}

// reorders the file according to the map file
tmp_file reorder(tmp_file tuple_file, char *map_name)
{

    // open and mape map file and the original file
    int fd_map = open(map_name, O_RDONLY, 0666);
    if (tuple_file.fd == -1)
    {
        tuple_file.fd = open(tuple_file.file_name, O_RDWR, 0666);
    }

    // map size must be equal to the file size
    struct stat st;
    fstat(tuple_file.fd, &st);
    size_t file_size = st.st_size;

    char *map = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd_map, 0);
    if (!tuple_file.map)
    {
        tuple_file.map = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, tuple_file.fd, 0);
    }

    // create a new file to write the reordered file
    tmp_file reordered = create_tmp_file(
        "reordered_tuple",
        file_size,
        true,
        true);

    // reorder the file and write it to the new file
    for (size_t i = 0; i < file_size; i += MAX_INT_LENGTH + 1)
    {
        int location = atoi(map + i);
        memcpy(reordered.map + i, tuple_file.map + location * (MAX_INT_LENGTH + 1), MAX_INT_LENGTH + 1);
    }

    // unmap and close all files
    free(reordered.file_name);
    munmap(map, file_size);
    close(fd_map);

    return reordered;
}

// create a tuple of multiple files
// and write it to a temporary file
tmp_file create_tuple(size_t n, size_t tuple_size, char **index_maps)
{

    // get the size of the tuple
    char *columns[tuple_size];
    for (size_t i = 0; i < tuple_size; i++)
    {
        columns[i] = index_maps[i];
    }

    tmp_file res = create_tmp_file("tuple", tuple_size * n * (MAX_INT_LENGTH + 1), true, true);
    char *tuple = res.map;

    size_t i = 0;
    // create a tuple

    for (size_t k = 0; k < n; k++)
    {
        for (size_t j = 0; j < tuple_size; j++)
        {

            memcpy(tuple + i, columns[j] + k * (MAX_INT_LENGTH + 1), MAX_INT_LENGTH);
            i += MAX_INT_LENGTH;
            tuple[i] = '-';
            i++;
        }
        tuple[i - 1] = ',';
        i++;
    }
    tuple[i - 1] = '\0';

    return res;
}

char **separate_tuple(Table *table, Column *idx_column, tmp_file tuple, size_t n)
{

    char **res = malloc(table->col_count * sizeof(char *));
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
        struct stat st;
        fstat(fd, &st);
        size_t file_size = st.st_size;
        fds[i] = fd;
        res[i] = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        free(file_name);
    }

    char *tuple_map = tuple.map;
    for (size_t i = 0; i < n; i++)
    {
        for (size_t j = 0; j < table->col_count; j++)
        {
            if (&table->columns[j] == idx_column)
            {
                continue;
            }
            memcpy(
                res[j] + i * (MAX_INT_LENGTH + 1),
                tuple_map + i * (MAX_INT_LENGTH + 1) * (table->col_count - 1) + j * (MAX_INT_LENGTH + 1),
                MAX_INT_LENGTH + 1);
        }
    }

    // unmap and close all files
    for (size_t i = 0; i < table->col_count - 1; i++)
    {
        close(fds[i]);
        munmap(res[i], n * (MAX_INT_LENGTH + 1));
    }

    return res;
}

// propagate_sort, sorts all other columns based on
// the sorting order of the first column
void propagate_sort(Table *tbl, Column *idx_column)
{
    return;
    const size_t PAGE_SIZE = (size_t)sysconf(_SC_PAGESIZE);

    char *index_maps[tbl->col_count - 1];

    for (size_t i = 0; i < tbl->col_count - 1; i++)
    {
        if (idx_column == &tbl->columns[i])
        {
            continue;
        }

        Status col_stat;
        create_colf(tbl, &tbl->columns[i], &col_stat);
        if (col_stat.code != OK)
        {
            return;
        }

        if (!tbl->columns[i].read_map)
        {
            struct stat st;
            fstat(tbl->columns[i].fd, &st);

            tbl->columns[i].read_map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, tbl->columns[i].fd, tbl->columns[i].meta_data_size * PAGE_SIZE);
            tbl->columns[i].read_map_size = st.st_size;
        }
        index_maps[i] = tbl->columns[i].read_map + tbl->columns[i].meta_data_size * PAGE_SIZE;
    }

    tmp_file tuple = create_tuple(tbl->col_count - 1, tbl->rows, index_maps);

    if (!idx_column->read_map)
    {
        char *file_path = catnstr(2, idx_column->name, ".map");
        int fd = open(file_path, O_RDONLY, 0666);
        struct stat st;
        fstat(fd, &st);
        size_t file_size = st.st_size;

        idx_column->read_map = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    }

    tmp_file reordered = reorder(tuple, idx_column->read_map);
    separate_tuple(tbl, idx_column, reordered, tbl->rows);
}

#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE
#include <string.h>
#include <stdio.h>

#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <stdarg.h>
#include <unistd.h>
#include "utils.h"
#include <sys/mman.h>
#include <stdlib.h>

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

#define LOG 1
#define LOG_ERR 1
#define LOG_INFO 1

/* removes newline characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of newline characters.
 */
char *trim_newline(char *str)
{
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i)
    {
        if (!(str[i] == '\r' || str[i] == '\n'))
        {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}
/* removes space characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of space characters.
 */
char *trim_whitespace(char *str)
{
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i)
    {
        if (!isspace(str[i]))
        {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

/* removes parenthesis characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of parenthesis characters.
 */
char *trim_parenthesis(char *str)
{
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i)
    {
        if (!(str[i] == '(' || str[i] == ')'))
        {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

char *trim_quotes(char *str)
{
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i)
    {
        if (str[i] != '\"')
        {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

void remove_files(files *head)
{

    while (head != NULL)
    {
        files *next = head->next;
        remove(head->data);
        free(head);
        head = next;
    }
}

void close_files(files *head)
{

    while (head != NULL)
    {
        files *next = head->next;
        close(*(int *)head->data);
        free(head);
        head = next;
    }
}

// clean_up - give a linked list of allocated memory
// this function frees them all
void clean_up(linkedList *head)
{
    while (head != NULL)
    {
        linkedList *next = head->next;
        free(head->data);
        free(head);
        head = next;
    }
}

int prepend(memallocs **head, void *ptr)
{
    memallocs *new = malloc(sizeof(memallocs));
    if (new == NULL)
    {
        log_err("Error allocating memory for memallocs");
        return -1;
    }
    new->data = ptr;
    new->next = *head;
    *head = new;
    return 0;
}

// retrack - removes all memory allocated and closes
// all files opened before failure
Status retrack(retrack_props props, char *error_msg)
{
    // it is important that files are closed beifore remove is callsed
    // as remove might try to remove files that are yet to be closed
    close_files(props.to_close);

    remove_files(props.to_remove);
    clean_up(props.to_free);
    clean_up(props.outside);

    Status status = {
        .code = ERROR,
        .error_message = error_msg,
    };

    return status;
}

// catnstr - concatenates n strings
char *catnstr(int n, ...)
{
    va_list argList;

    char *result = NULL;
    char *next; // A variable to hold the next argument.

    // This call initializes the var-arg package:
    va_start(argList, n);

    // Extract arguments one by one:
    for (int i = 0; i < n; i++)
    {
        next = va_arg(argList, char *);
        if (next != NULL)
        {

            if (result == NULL)
            {
                result = malloc(strlen(next) + 1);
                if (!result)
                {
                    return NULL;
                }
                strcpy(result, next);
            }
            else
            {
                char *realloced = realloc(result, strlen(result) + strlen(next) + 1);
                if (!realloced)
                {
                    free(result);
                    return NULL;
                }
                result = realloced;
                strcat(result, next);
            }
        }
    }

    // Required call to signal end of var-args:
    va_end(argList);

    // Return result.
    return result;
}

// line_length - returns the length of one line string
// one ling string is a string that starts at file and ends with
// either \n or \0, which ever comes first
size_t line_length(char *file)
{
    size_t i = 0;
    while (file[i] != '\0' && file[i] != '\n')
    {
        i++;
    }
    return i;
}

String read_line(char *file)
{
    size_t size = 100;
    char *line = malloc(size * sizeof(char));

    size_t i = 0;
    while (file[i] != '\0' && file[i] != '\n')
    {
        if (i + 1 > size)
        {
            size *= 2;
            char *new_line = realloc(line, size * sizeof(char));
            if (!new_line)
            {
                free(line);
                return (String){.str = NULL, .len = 0};
            }
            line = new_line;
        }
        line[i] = file[i];
        i++;
    }
    line[i] = '\0';
    return (String){.str = line, .len = i};
}

char *zeropadd(char *str, char *dst)
{

    char *padd_str = dst;

    if (!dst)
    {
        padd_str = malloc(MAX_INT_LENGTH + 1);
    }

    if (!padd_str)
    {
        return NULL;
    }

    int num = atoi(str);

    sprintf(padd_str, "%012d", num);
    return padd_str;
}

// extracts a number from a string until reaching a separator
// and returns the unpadded number
// opposeit of zeropadd
int zerounpadd(char *data, char sep)
{
    int i = 0;
    int num = 0;
    int sign = 1;
    while (data[i] != sep && data[i] != '\0')
    {
        if (data[i] == '-')
        {
            sign = -1;
            i += 1;
            continue;
        }

        // assums the characters are numeric if not
        // it will fail
        if (data[i] < '0' || data[i] > '9')
        {
            break;
        }
        num = num * 10 + (data[i] - '0');
        i++;
    }
    return num * sign;
}

// create_tmp_file - creates a temporary file whose name has the prefix file_name
// and returns the file descriptor of the file along with the map if mapped is set
// to true
tmp_file create_tmp_file(char *file_name, size_t size, bool mapped, bool unlinked, bool shared)
{

    char tmp_file_name[256];
    sprintf(tmp_file_name, "%sXXXXXX", file_name);

    int fd = mkstemp(tmp_file_name);

    tmp_file file = (tmp_file){
        .file_name = strdup(tmp_file_name),
        .fd = fd,
        .size = size,
        .map = NULL};

    // expand the file
    lseek(file.fd, size, SEEK_SET);
    write(file.fd, " ", 1);

    if (unlinked)
    {
        unlink(tmp_file_name);
    }

    if (mapped)
    {
        file.map = mmap(NULL, size, PROT_READ | PROT_WRITE, shared ? MAP_SHARED : MAP_PRIVATE, file.fd, 0);
    }
    return file;
}

// compare string ints
int compare_sints(const void *a, const void *b)
{
    int a_int = atoi((char *)a);
    int b_int = atoi((char *)b);
    if (a_int < b_int)
    {
        return -1;
    }

    if (a_int > b_int)
    {
        return 1;
    }
    return 0;
}

void *closest_search(const void *key, const void *base,
                     size_t nmemb, size_t size,
                     int (*compar)(const void *, const void *))
{
    // uses binary search to find the closest value in a sorted array
    // returns a pointer to the closest value

    // if (compar(key, (const char *)base + (nmemb - 1) * size) > 0)
    // {
    //     return (void *)((const char *)base + nmemb * size);
    // }
    size_t l, u, idx;
    const char *p;
    int r;

    l = 0;
    u = nmemb;
    while (l < u)
    {
        idx = (l + u) / 2;
        p = (const char *)base + idx * size;
        r = compar(key, p);
        if (r < 0)
        {
            u = idx;
        }
        else if (r > 0)
        {
            l = idx + 1;
        }
        else
        {
            return (void *)p;
        }
    }

    return (void *)p;
}

/* The following three functions will show output on the terminal
 * based off whether the corresponding level is defined.
 * To see log output, define LOG.
 * To see error output, define LOG_ERR.
 * To see info output, define LOG_INFO
 */
void cs165_log(FILE *out, const char *format, ...)
{
#ifdef LOG
    va_list v;
    va_start(v, format);
    vfprintf(out, format, v);
    va_end(v);
#else
    (void)out;
    (void)format;
#endif
}

void log_err(const char *format, ...)
{
#ifdef LOG_ERR
    va_list v;
    va_start(v, format);
    fprintf(stderr, ANSI_COLOR_RED);
    vfprintf(stderr, format, v);
    fprintf(stderr, ANSI_COLOR_RESET);
    va_end(v);
#else
    (void)format;
#endif
}

void log_info(const char *format, ...)
{
#ifdef LOG_INFO
    va_list v;
    va_start(v, format);
    fprintf(stdout, ANSI_COLOR_GREEN);
    vfprintf(stdout, format, v);
    fprintf(stdout, ANSI_COLOR_RESET);
    fflush(stdout);
    va_end(v);
#else
    (void)format;
#endif
}

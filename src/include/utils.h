// utils.h
// CS165 Fall 2015
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdarg.h>
#include <stdio.h>

// 12 including negative sign
#define MAX_INT_LENGTH 12

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode
{
    /* The operation completed successfully */
    OK,
    /* There was an error with the call. */
    ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status
{
    StatusCode code;
    char *error_message;
} Status;

/**
 * trims newline characters from a string (in place)
 **/

char *trim_newline(char *str);

/**
 * trims parenthesis characters from a string (in place)
 **/

char *trim_parenthesis(char *str);

/**
 * trims whitespace characters from a string (in place)
 **/

char *trim_whitespace(char *str);

/**
 * trims quotations characters from a string (in place)
 **/

char *trim_quotes(char *str);

// cs165_log(out, format, ...)
// Writes the string from @format to the @out pointer, extendable for
// additional parameters.
//
// Usage: cs165_log(stderr, "%s: error at line: %d", __func__, __LINE__);
void cs165_log(FILE *out, const char *format, ...);

// log_err(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters. Like cs165_log, but specifically to stderr.
//
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
void log_err(const char *format, ...);

// log_info(format, ...)
// Writes the string from @format to stdout, extendable for
// additional parameters. Like cs165_log, but specifically to stdout.
// Only use this when appropriate (e.g., denoting a specific checkpoint),
// else defer to using printf.
//
// Usage: log_info("Command received: %s", command_string);
void log_info(const char *format, ...);

typedef struct linkedList
{
    void *data;
    struct linkedList *next;
} linkedList;

// mmallocs - a structure to keep track of allocated functions per function call
// There are two types of memory allocations that can be tracked, in the software
// 1. mallocs that are meant to be freed at the end of the function call
// 2. mallocs that returned, or used to update a global variable
// Both types of functions must be freed during function failures
// while the first group should be freed at the end of a successful function call
// to add a new malloc to the list, use prepend function and to free all the mallocs
// use clean_up function
typedef linkedList memallocs;

// a linked list to keep track of opened files
typedef linkedList files;

// a structure containing all the information needed incase of
// a failure in a function call
typedef struct retrack_props
{
    memallocs *to_free;
    memallocs *outside;
    files *to_remove;
    files *to_close;
    const char *error_msg;
} retrack_props;

// clean_up - given a linked list of allocated memory
// this function frees them all
void clean_up(memallocs *);
// prepend - given a pointer and the address of a memallocs list
// adds a pointer to the head of a linked list of allocated memory
int prepend(linkedList **, void *);

// given a linked list of files
// this function delets them all
void remove_files(files *);

// given a linked list of files
// this functions closes all opened files
void close_files(files *head);

// retrack - a function to be called during functin faliures, used to retrack
// all memory allocations
// It takes 2 linked list of allocated momories as explained above in the mmallocs
// struture defination. This function then frees both the lists and and updates
// the status variable
Status retrack(retrack_props, char *);

// catnstr - concatenates n strings and returns a new string
char *catnstr(int n, ...);

// line_length
size_t line_length(char *);

typedef struct String
{
    char *str;
    size_t len;
} String;
String read_line(char *);

// takes an integer of length len and returns a padded string
// of length MAX_INT_LENGTH, that can easly be converted to integer
// using atoi
char *zeropadd(char *, char *);

// does the opposit of zeropadd by extracting a number from a string
// until reaching a separator character
int zerounpadd(char *, char);

#endif /* __UTILS_H__ */

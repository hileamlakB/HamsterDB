#define _DEFAULT_SOURCE
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <cs165_api.h>

typedef struct test_timer
{
    clock_t start;
    clock_t end;
    double cpu_time_used;
} test_timer;

test_timer global_timer;

DbOperator *parse_timer(char *timer_arguments)
{
    timer_arguments++; // remove the first parenthesis
    if (strncmp(timer_arguments, "start", 5) == 0)
    {
        global_timer.start = clock();
        return NULL;
    }
    else if (strncmp(timer_arguments, "end", 3) == 0)
    {
        global_timer.end = clock();
        printf("%f\n", (double)(global_timer.end - global_timer.start) / CLOCKS_PER_SEC);
    }

    return NULL;
}

#ifndef __PARSER_H__
#define __PARSER_H__

#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>

#include <cs165_api.h>
#include <Utils/utils.h>
#include <Loader/load.h>

DbOperator *parse_batch_query(char *query_command, message *send_message);
DbOperator *parse_batch_execute(char *query_command, message *send_message);
DbOperator *parse_create_col(char *create_arguments);
DbOperator *parse_create_db(char *create_arguments);
DbOperator *parse_create_tbl(char *create_arguments);
DbOperator *parse_create(char *drop_arguments);
DbOperator *parse_create_idx(char *create_arguments);

DbOperator *parse_select(char *handle, char *select_argument);
DbOperator *parse_fetch(char *handle, char *fetch_arguments);

DbOperator *parse_load_start(char *token, message *send_message);
DbOperator *parse_load_end(char *query_command, message *send_message);
DbOperator *parse_load_parallel(String query_command, message *send_message);
DbOperator *parse_load(char *query_command, message *send_message);
DbOperator *parse_insert(char *query_command, message *send_message);

DbOperator *parse_join(char *handle, char *join_args);
DbOperator *parse_sum(char *handle, char *sum_arg);
DbOperator *parse_add(char *handle, char *add_arg);
DbOperator *parse_sub(char *handle, char *sub_arg);
DbOperator *parse_avg(char *handle, char *avg_arg);
DbOperator *parse_min_max(char *handle, char *min_max_arg, OperatorType type);

DbOperator *parse_timer(char *timer_arguments);

DbOperator *parse_print(char *print_argument);

EntityAddress parse_column_name(char *token, Status *status);

#endif
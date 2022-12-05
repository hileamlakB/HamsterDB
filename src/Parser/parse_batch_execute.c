
#include <Parser/parse.h>

DbOperator *parse_batch_query(char *query_command, message *send_message)
{
    (void)send_message;
    (void)query_command;
    batch.mode = true;
    batch.num_queries = 0;
    return NULL;
}

DbOperator *parse_batch_execute(char *query_command, message *send_message)
{
    (void)query_command;
    (void)send_message;

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = BATCH_EXECUTE;
    return dbo;
}

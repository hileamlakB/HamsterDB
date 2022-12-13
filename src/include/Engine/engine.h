#include <cs165_api.h>

String execute_DbOperator(DbOperator *query);
double generic_sum(Variable *variable);
void sum(AvgOperator avg_operator);
void average(char *handler, Variable *variable);
void add_sub(char *handler, Variable *variable1, Variable *variable2, int (*op)(int a, int b));
void add(char *handler, Variable *variable1, Variable *variable2);
void sub(char *handler, Variable *variable1, Variable *variable2);
void MinMax(MinMaxOperator minmax_operator, Status *status);
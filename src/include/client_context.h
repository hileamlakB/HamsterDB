#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

// Table* lookup_table(char *name);
Table *lookup_table(Db *, char *);
Column *lookup_column(Table *, char *);

#endif

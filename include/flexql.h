#ifndef FLEXQL_H
#define FLEXQL_H

#define FLEXQL_OK 0
#define FLEXQL_ERROR 1

// opaque handle - hides the messy backend from the client
typedef struct FlexQL FlexQL;

// assignment required api
int flexql_open(const char *host, int port, FlexQL **db);
int flexql_close(FlexQL *db);

// executes query and fires the callback for each row
int flexql_exec(
    FlexQL *db, 
    const char *sql, 
    int (*callback)(void*, int, char**, char**), 
    void *arg, 
    char **errmsg
);

void flexql_free(void *ptr);

#endif
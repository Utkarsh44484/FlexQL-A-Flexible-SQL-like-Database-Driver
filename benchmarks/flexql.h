#ifndef FLEXQL_H
#define FLEXQL_H

// Opaque handle
struct FlexQL;

// Error codes
#define FLEXQL_OK 0
#define FLEXQL_ERROR 1

int flexql_open(const char *host, int port, struct FlexQL **db);

int flexql_close(struct FlexQL *db);

int flexql_exec(
    struct FlexQL *db,
    const char *sql,
    int (*callback)(void*, int, char**, char**),
    void *cb_data,
    char **errmsg
);

void flexql_free(void *ptr);

#endif
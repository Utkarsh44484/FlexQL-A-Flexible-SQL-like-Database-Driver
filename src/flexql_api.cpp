#include <iostream>
#include <string>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <sstream>

#include "../include/flexql.h"

struct FlexQL {
    int socket_fd;
};

int flexql_open(const char *host, int port, FlexQL **db) {
    if (!db) return FLEXQL_ERROR;

    *db = new FlexQL();
    (*db)->socket_fd = socket(AF_INET, SOCK_STREAM, 0);

    if ((*db)->socket_fd < 0) {
        delete *db;
        return FLEXQL_ERROR;
    }

    struct sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        close((*db)->socket_fd);
        delete *db;
        return FLEXQL_ERROR;
    }

    if (connect((*db)->socket_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close((*db)->socket_fd);
        delete *db;
        return FLEXQL_ERROR;
    }

    int flag = 1;
    setsockopt((*db)->socket_fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(flag));
    
    int sndbuf = 2 * 1024 * 1024;
    setsockopt((*db)->socket_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (!db) return FLEXQL_ERROR; // Matches "FLEXQL_ERROR: Invalid database handle"
    
    close(db->socket_fd);
    delete db;
    
    return FLEXQL_OK; // Matches "FLEXQL_OK: Connection closed successfully"
}

void flexql_free(void *ptr) {
    if (ptr) free(ptr);
}

int flexql_exec(FlexQL *db, const char *sql, int (*callback)(void*, int, char**, char**), void *cb_data, char **errmsg) {
    if (!db || db->socket_fd < 0 || !sql) {
        if (errmsg) *errmsg = strdup("Invalid input");
        return FLEXQL_ERROR;
    }

    size_t total_len = strlen(sql) + 1; 
    size_t sent_total = 0;

    while (sent_total < total_len) {
        ssize_t sent = send(db->socket_fd, sql + sent_total, total_len - sent_total, 0);
        if (sent <= 0) {
            if (errmsg) *errmsg = strdup("Network write failed");
            return FLEXQL_ERROR;
        }
        sent_total += sent;
    }

    std::string response;
    response.reserve(65536); 
    
    // Matched buffer size with the server for symmetrical throughput
    char buffer[262144];      

    while (true) {
        int bytes_read = read(db->socket_fd, buffer, sizeof(buffer));

        if (bytes_read <= 0) {
            if (errmsg) *errmsg = strdup("Network read failed");
            return FLEXQL_ERROR;
        }

        void* null_pos = memchr(buffer, '\0', bytes_read);
        if (null_pos != nullptr) {
            int null_idx = (char*)null_pos - buffer;
            response.append(buffer, null_idx);
            break;
        }

        response.append(buffer, bytes_read);
    }

    if (response.find("Error") != std::string::npos || response.find("Syntax Error") != std::string::npos) {
        if (errmsg) *errmsg = strdup(response.c_str());
        return FLEXQL_ERROR;
    }

   if (callback && response.find("0 rows") == std::string::npos && response.find("Table") == std::string::npos && response.find("Rows Inserted") == std::string::npos) {
        std::stringstream ss(response);
        std::string line;

        while (std::getline(ss, line)) {
            if (line.empty()) continue;
            if (!line.empty() && line.back() == '\r') line.pop_back();

            const char *argv_arr[1] = { line.c_str() };
            const char *col_arr[1]  = { "DATA" };

            // callback(cb_data, 1, (char **)argv_arr, (char **)col_arr);


            // 1. Capture the integer returned by the callback
            int cb_result = callback(cb_data, 1, (char **)argv_arr, (char **)col_arr);
            
            // 2. Check if the user requested an abort
            if (cb_result == 1) {
                break; // Exit the while-loop immediately
            }
        }
    } else if (response.find("Rows Inserted") == std::string::npos) {
        std::cout << response;
    }

    return FLEXQL_OK;
}
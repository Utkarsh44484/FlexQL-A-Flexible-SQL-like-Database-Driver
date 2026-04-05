#include <iostream>
#include <string>
#include <unistd.h>
//thisis client
#include "../include/flexql.h"

#define FLEXQL_OK 0

// The callback just prints the rows returned by the server
int repl_callback(void *data, int argc, char **argv, char **azColName) {
    if (argc > 0 && argv[0]) {
        std::cout << "> " << argv[0] << "\n";
    }
    return 0;
}

int main(int argc, char **argv) {
    std::string host = "127.0.0.1";
    int port = 9000;

    if (argc == 3) {
        host = argv[1];
        port = std::stoi(argv[2]);
    }

    FlexQL *db = nullptr;
    if (flexql_open(host.c_str(), port, &db) != FLEXQL_OK) {
        std::cerr << "Failed to connect to FlexQL server at " << host << ":" << port << "\n";
        return 1;
    }

    std::cout << "Connected to FlexQL server.\n";
    std::cout << "Type '.exit' to quit.\n\n";
std::string line;
    std::string accumulated_query;
    
    // PRE-ALLOCATE: Grab 4KB of memory upfront so we never have to ask the OS again
    accumulated_query.reserve(4096); 

    // SYSCALL ONCE: Check if a human is typing exactly ONE time, not a million times.
    bool is_interactive = isatty(STDIN_FILENO);

    while (true) {
        if (is_interactive) {
            std::cout << "flexql> ";
            std::cout.flush(); 
        }

        if (!std::getline(std::cin, line)) break; 

        if (line == ".exit" || line == "exit") break;
        if (line.empty()) continue;

        accumulated_query += line + " ";

        // FAST SEARCH: Only check the new line for the semicolon, not the whole buffer!
        if (line.find(';') != std::string::npos) {
            
            char *errmsg = nullptr;
            int rc = flexql_exec(db, accumulated_query.c_str(), repl_callback, nullptr, &errmsg);
            
            if (rc != FLEXQL_OK && errmsg) {
                std::cout << errmsg << "\n"; 
                flexql_free(errmsg);
            }
            
            if (is_interactive) {
                std::cout << "\n";
            }

            // ZERO-ALLOCATION RESET: Empties the text but keeps the physical RAM intact
            accumulated_query.clear(); 
        }
    }

    flexql_close(db);
    std::cout << "Connection closed.\n";
    return 0;
}



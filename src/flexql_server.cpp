#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <cstring>
#include <cstdlib>
#include "flexql_core.hpp"
#include <fstream>
#include <atomic>
#include <signal.h>
extern int parse_and_execute(const std::string& query, int client_fd);

// Declared in flexql_parser.cpp
extern Row* allocate_row_fast();
extern double fast_atod(const char* str, size_t len);
void handle_shutdown(int sig) {
  
    
    // Force the global writer to flush remaining RAM to the physical file
    g_data_writer.flush(); 
    
    std::cout << "[Server] Safe shutdown complete.\n";
    exit(0); // Exiting normally ensures destructors run safely
}
// BINARY DATA FILE REPLAY
// Reconstructs full DB state from flexql.db on startup
static void replay_data_file(const char* path) {
    FILE* f = fopen(path, "rb");
    if (!f) {
        std::cout << "No data file found. Starting fresh.\n";
        return;
    }

    int create_count = 0, insert_count = 0, delete_count = 0;
    long long total_rows = 0;

    while (true) {
        uint8_t op;
        if (fread(&op, 1, 1, f) != 1) break;

        if (op == 0x01) { // CREATE TABLE
            uint16_t name_len;
            if (fread(&name_len, 2, 1, f) != 1) break;
            char name[256];
            if (fread(name, name_len, 1, f) != 1) break;

            uint16_t num_cols;
            if (fread(&num_cols, 2, 1, f) != 1) break;

            // Drop existing table if any (handles repeated CREATEs in the log)
            std::string tname(name, name_len);
            Table* existing = db_catalog.get_table(tname);
            if (existing) db_catalog.drop_table(tname);

            Table* t = db_catalog.create_table(tname);

            for (int i = 0; i < num_cols; i++) {
                uint16_t cnl;
                if (fread(&cnl, 2, 1, f) != 1) break;
                char cname[256];
                if (fread(cname, cnl, 1, f) != 1) break;
                uint8_t ctype;
                if (fread(&ctype, 1, 1, f) != 1) break;
                if (t) t->cols.push_back(Column{std::string(cname, cnl), (DataType)ctype});
            }
            create_count++;
        }
        else if (op == 0x02) { // INSERT BATCH
            uint16_t name_len;
            if (fread(&name_len, 2, 1, f) != 1) break;
            char name[256];
            if (fread(name, name_len, 1, f) != 1) break;

            uint32_t num_rows;
            if (fread(&num_rows, 4, 1, f) != 1) break;

            uint16_t num_cols;
            if (fread(&num_cols, 2, 1, f) != 1) break;

            std::string tname(name, name_len);
            Table* t = db_catalog.get_table(tname);

            if (t && num_rows > 100) {
                t->index->reserve(t->index->total_rows() + num_rows);
            }

            for (uint32_t r = 0; r < num_rows; r++) {
                Row* row = nullptr;
                if (t) {
                    row = allocate_row_fast();
                    row->reset();
                }

                for (uint16_t c = 0; c < num_cols; c++) {
                    uint16_t vlen;
                    if (fread(&vlen, 2, 1, f) != 1) goto done;
                    char vbuf[256];
                    if (vlen > 0) {
                        if (fread(vbuf, vlen, 1, f) != 1) goto done;
                    }
                    if (row) row->append_value(vbuf, vlen);
                }

                if (row) {
                    row->expires_at = 2147483647;
                    row->is_dead = false;
                    if (t) {
                        double pk = fast_atod(row->get_value(0).data(), row->get_value(0).length());
                        t->index->insert(pk, row);
                    }
                }
                total_rows++;
            }
            insert_count++;
        }
        else if (op == 0x03) { // DELETE FROM
            uint16_t name_len;
            if (fread(&name_len, 2, 1, f) != 1) break;
            char name[256];
            if (fread(name, name_len, 1, f) != 1) break;

            std::string tname(name, name_len);
            Table* existing = db_catalog.get_table(tname);
            std::vector<Column> saved_cols;
            if (existing) saved_cols = existing->cols;

            db_catalog.drop_table(tname);
            Table* new_t = db_catalog.create_table(tname);
            if (new_t) new_t->cols = saved_cols;

            delete_count++;
        }
        else {
            std::cout << "Warning: Unknown record type 0x" << std::hex << (int)op << std::dec << " in data file. Stopping replay.\n";
            break;
        }
    }

done:
    fclose(f);
    std::cout << "Data file replay: " << create_count << " tables, "
              << insert_count << " batches (" << total_rows << " rows), "
              << delete_count << " deletes.\n";
}

// CLIENT HANDLER (unchanged logic, same buffer sizes)
void handle_client(int client_fd) {
    
    constexpr size_t BUF_SIZE = 1048576; 
    char* buffer = new char[BUF_SIZE]; 
    
    std::string accumulated_query;
    accumulated_query.reserve(1048576); // Pre-reserve 1MB

    while (true) {
        int bytes_read = read(client_fd, buffer, BUF_SIZE);
        if (bytes_read <= 0) break;

        int chunk_start = 0;
        while (chunk_start < bytes_read) {
            void* null_pos = memchr(buffer + chunk_start, '\0', bytes_read - chunk_start);

            if (null_pos) {
                int null_idx = (char*)null_pos - buffer;
                accumulated_query.append(buffer + chunk_start, null_idx - chunk_start);
                parse_and_execute(accumulated_query, client_fd);
                accumulated_query.clear();
                chunk_start = null_idx + 1;
            } else {
                accumulated_query.append(buffer + chunk_start, bytes_read - chunk_start);
                break;
            }
        }
    }
    close(client_fd);
    delete[] buffer;

    g_data_writer.sync_to_disk();
}

// MAIN: startup with binary data file replay
int main() {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, handle_shutdown);  // Catches Ctrl+C
    signal(SIGTERM, handle_shutdown); // Catches standard kill commands
    std::cout << "FlexQL Boot Sequence Initiated...\n";

    // 1. Replay binary data file (primary persistence)
    std::cout << "Replaying data file (flexql.db)...\n";
    replay_data_file("flexql.db");

    // 2. Also replay text WAL for backward compatibility
    //    (entries written by older versions before binary persistence)
    std::cout << "Replaying legacy WAL (flexql.wal)...\n";
    std::ifstream infile("flexql.wal");
    std::string line;
    int recovered_queries = 0;
    while (std::getline(infile, line)) {
        if (!line.empty()) {
            parse_and_execute(line, -1);
            recovered_queries++;
        }
    }
    if (recovered_queries > 0)
        std::cout << "Restored " << recovered_queries << " operations from legacy WAL.\n";

    // 3. Open binary data file for new writes
    g_data_writer.open("flexql.db");

    // 4. Start TCP server
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(9000);

    bind(server_fd, (struct sockaddr*)&address, sizeof(address));
    listen(server_fd, 10);

    std::cout << "FlexQL Server Ready. Port 9000.\n";


    // std::thread([]() {
    //     while (true) {
    //         std::this_thread::sleep_for(std::chrono::seconds(15));
    //         g_data_writer.sync_to_disk(); 
    //     }
    // }).detach();

    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd >= 0) {
            int flag = 1;
            setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(int));
            std::thread(handle_client, client_fd).detach();
        }
    }
    return 0;
}

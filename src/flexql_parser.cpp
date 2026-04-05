#include "flexql_core.hpp"
#include <sstream>
#include <unistd.h>
#include <list>
#include <unordered_map>
#include <mutex>
#include <ctime>
#include <shared_mutex>
#include <memory>
#include <atomic>
#include <cstdlib>

Catalog db_catalog;
DataWriter g_data_writer;

// --- CHUNKED SLAB ALLOCATOR ---
constexpr size_t CHUNK_SIZE = 131072;
constexpr size_t NUM_CHUNKS = 120;
Row* arena_chunks[NUM_CHUNKS] = {nullptr};
std::atomic<size_t> arena_offset{0};

static std::mutex chunk_alloc_mutexes[NUM_CHUNKS];

// THREAD-LOCAL BATCH ARENA: grabs rows in batches of 4096
static constexpr size_t TL_BATCH = 4096;

Row* allocate_row_fast() {
    thread_local size_t tl_base = 0;
    thread_local size_t tl_used = TL_BATCH;

    if (tl_used >= TL_BATCH) {
        tl_base = arena_offset.fetch_add(TL_BATCH, std::memory_order_relaxed);
        tl_used = 0;
    }

    size_t idx = tl_base + tl_used;
    tl_used++;

    size_t chunk_idx = idx >> 17;
    size_t item_idx  = idx & 131071;

    if (chunk_idx < NUM_CHUNKS) {
        if (!arena_chunks[chunk_idx]) {
            std::lock_guard<std::mutex> lk(chunk_alloc_mutexes[chunk_idx]);
            if (!arena_chunks[chunk_idx]) {
                arena_chunks[chunk_idx] = new Row[CHUNK_SIZE];
            }
        }
        return &arena_chunks[chunk_idx][item_idx];
    }
    return new Row();
}

std::atomic<long long> current_db_offset{0};

std::shared_mutex catalog_mutex;
std::unordered_map<std::string, std::shared_ptr<std::shared_mutex>> table_mutexes;
std::mutex t_mtx_lock;

std::shared_ptr<std::shared_mutex> get_table_mutex(const std::string& name) {
    std::lock_guard<std::mutex> lk(t_mtx_lock);
    auto it = table_mutexes.find(name);
    if (it == table_mutexes.end()) {
        auto p = std::make_shared<std::shared_mutex>();
        table_mutexes[name] = p;
        return p;
    }
    return it->second;
}

// QUERY CACHE
class QueryCache {
    size_t capacity;
    std::list<std::string> lru_list;
    std::unordered_map<std::string, std::pair<std::string, std::list<std::string>::iterator>> cache_map;
    std::mutex mtx;
public:
    QueryCache(size_t cap) : capacity(cap) {}

    bool get(const std::string& query, std::string& out_result) {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = cache_map.find(query);
        if (it == cache_map.end()) return false;
        lru_list.splice(lru_list.begin(), lru_list, it->second.second);
        out_result = it->second.first;
        return true;
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mtx);
        cache_map.clear();
        lru_list.clear();
    }

    void put(const std::string& query, const std::string& result) {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = cache_map.find(query);
        if (it != cache_map.end()) {
            lru_list.splice(lru_list.begin(), lru_list, it->second.second);
            it->second.first = result;
            return;
        }
        if (cache_map.size() >= capacity) {
            std::string lru = lru_list.back();
            lru_list.pop_back();
            cache_map.erase(lru);
        }
        lru_list.push_front(query);
        cache_map[query] = {result, lru_list.begin()};
    }
};

QueryCache global_cache(1000);

// FAST HELPERS
double fast_atod(const char* str, size_t len) {
    if (len == 0) return 0.0;
    double res = 0.0;
    size_t i = 0;
    bool is_neg = (str[0] == '-');
    if (is_neg) i++;
    for (; i < len && str[i] != '.'; ++i)
        res = res * 10.0 + (str[i] - '0');
    if (i < len && str[i] == '.') {
        double frac = 0.1;
        for (++i; i < len; ++i) {
            res += (str[i] - '0') * frac;
            frac *= 0.1;
        }
    }
    return is_neg ? -res : res;
}

inline bool fast_validate_type(const char* str, size_t len, DataType type) {
    if (type == TYPE_VARCHAR || type == TYPE_DATETIME) return true;
    if (len == 0) return false;
    size_t i = 0;
    if (str[0] == '-') i++;
    if (i == len) return false;
    if (type == TYPE_INT) {
        for (; i < len; ++i)
            if (str[i] < '0' || str[i] > '9') return false;
        return true;
    }
    if (type == TYPE_DECIMAL) {
        bool has_dot = false;
        for (; i < len; ++i) {
            if (str[i] == '.') {
                if (has_dot) return false;
                has_dot = true;
            } else if (str[i] < '0' || str[i] > '9') return false;
        }
        return true;
    }
    return true;
}

std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\r\n;'");
    if (std::string::npos == first) return "";
    size_t last = str.find_last_not_of(" \t\r\n;'");
    return str.substr(first, (last - first + 1));
}

std::string strip_table_prefix(const std::string& col) {
    size_t dot_pos = col.find(".");
    if (dot_pos != std::string::npos) return col.substr(dot_pos + 1);
    return col;
}

std::string to_upper(std::string s) {
    for (char& c : s)
        if (c >= 'a' && c <= 'z') c = c - ('a' - 'A');
    return s;
}

void send_response(int fd, const std::string& msg) {
    if (fd == -1) return;
    std::string payload = msg + '\0';
    size_t total_len = payload.length();
    size_t total_sent = 0;
    while (total_sent < total_len) {
        ssize_t sent = write(fd, payload.c_str() + total_sent, total_len - total_sent);
        if (sent <= 0) break;
        total_sent += sent;
    }
}

// BINARY PERSISTENCE HELPERS
static void persist_create_table(const std::string& tname, const std::vector<Column>& cols) {
    char hdr[512];
    size_t off = 0;
    hdr[off++] = 0x01;
    uint16_t nl = (uint16_t)tname.size();
    memcpy(hdr + off, &nl, 2); off += 2;
    memcpy(hdr + off, tname.data(), nl); off += nl;
    uint16_t nc = (uint16_t)cols.size();
    memcpy(hdr + off, &nc, 2); off += 2;
    for (auto& c : cols) {
        uint16_t cnl = (uint16_t)c.name.size();
        memcpy(hdr + off, &cnl, 2); off += 2;
        memcpy(hdr + off, c.name.data(), cnl); off += cnl;
        hdr[off++] = (uint8_t)c.type;
    }
    g_data_writer.append(hdr, off);
}

static void persist_delete(const std::string& tname) {
    char hdr[256];
    size_t off = 0;
    hdr[off++] = 0x03;
    uint16_t nl = (uint16_t)tname.size();
    memcpy(hdr + off, &nl, 2); off += 2;
    memcpy(hdr + off, tname.data(), nl); off += nl;
    g_data_writer.append(hdr, off);
}

// MAIN PARSER + EXECUTOR
int parse_and_execute(const std::string& query, int client_fd) {

    // 1. CREATE TABLE
    if (query.find("CREATE TABLE") == 0) {
        size_t name_start = query.find("TABLE ") + 6;
        if (query.find("IF NOT EXISTS ") != std::string::npos)
            name_start += 14;

        size_t paren_start = query.find("(");
        size_t paren_end = query.find_last_of(")");
        if (paren_start == std::string::npos || paren_end == std::string::npos) {
            send_response(client_fd, "Error: Syntax Error.\n");
            return 1;
        }

        std::string t_name = trim(query.substr(name_start, paren_start - name_start));

        std::string cols_str = query.substr(paren_start + 1, paren_end - paren_start - 1);
        std::vector<Column> parsed_cols;
       
        int detected_pk_index = 0;
        int current_col_idx = 0;

        {
            std::stringstream ss(cols_str);
            std::string col_def;
            while (std::getline(ss, col_def, ',')) {
                std::string upper_col_def = to_upper(trim(col_def));
               
                if (upper_col_def.find("PRIMARY KEY") != std::string::npos) {
                    detected_pk_index = current_col_idx;
                }

                std::stringstream css(upper_col_def);
                std::string c_name, c_type_str;
                css >> c_name >> c_type_str;
               
                DataType c_type = TYPE_VARCHAR;
                if (c_type_str.find("INT") != std::string::npos) c_type = TYPE_INT;
                else if (c_type_str.find("DECIMAL") != std::string::npos || c_type_str.find("DOUBLE") != std::string::npos) c_type = TYPE_DECIMAL;
                else if (c_type_str.find("DATETIME") != std::string::npos) c_type = TYPE_DATETIME;
               
                parsed_cols.push_back(Column{c_name, c_type});
                current_col_idx++;
            }
        }

        std::unique_lock<std::shared_mutex> cat_lock(catalog_mutex);

        Table* existing = db_catalog.get_table(t_name);
        if (existing) {
            db_catalog.drop_table(t_name);
        }

        Table* t = db_catalog.create_table(t_name);
        if (!t) {
            send_response(client_fd, "Error: Table creation failed.\n");
            return 1;
        }
        t->cols = parsed_cols;
        t->pk_index = detected_pk_index;

        get_table_mutex(t_name);
        if (client_fd != -1) persist_create_table(t_name, t->cols);
        send_response(client_fd, "Table Created.\n");
        return 0;
    }

    // 2. DELETE
    if (query.find("DELETE FROM") == 0) {
        std::string t_name = trim(query.substr(11));
        std::unique_lock<std::shared_mutex> cat_lock(catalog_mutex);
        auto t_lock_ptr = get_table_mutex(t_name);
        std::unique_lock<std::shared_mutex> write_lock(*t_lock_ptr);

        std::vector<Column> saved_cols;
        Table* old_t = db_catalog.get_table(t_name);
        if (old_t) saved_cols = old_t->cols;

        db_catalog.drop_table(t_name);
        Table* new_t = db_catalog.create_table(t_name);
        if (new_t) new_t->cols = saved_cols;

        if (client_fd != -1) persist_delete(t_name);
        send_response(client_fd, "Table Truncated.\n");
        return 0;
    }

    
    if (query.find("INSERT") == 0) {
        size_t into_pos = query.find("INTO");
        size_t val_pos = query.find("VALUES");

        if (into_pos == std::string::npos || val_pos == std::string::npos) {
            send_response(client_fd, "Error: Syntax Error in INSERT.\n");
            return 1;
        }

        std::string t_name = trim(query.substr(into_pos + 4, val_pos - (into_pos + 4)));
        
        // PHASE 1: Quick read-lock just to safely copy schema metadata
        catalog_mutex.lock_shared();
        Table* t = db_catalog.get_table(t_name);
        catalog_mutex.unlock_shared();

        if (!t) {
            send_response(client_fd, "Error: Table missing.\n");
            return 1;
        }

        // Copy schema info to local stack variables to avoid referencing 't' while unlocked
        auto t_lock_ptr = get_table_mutex(t_name);
        std::shared_lock<std::shared_mutex> schema_lock(*t_lock_ptr);
        const size_t num_cols = t->cols.size();
        const int pk_idx = t->pk_index;
        int expire_idx = -1;
        for (size_t i = 0; i < num_cols; i++) {
            if (to_upper(t->cols[i].name).find("EXPIRE") != std::string::npos) { expire_idx = i; break; }
        }
        std::vector<DataType> expected_types;
        expected_types.reserve(num_cols);
        for(auto& c : t->cols) expected_types.push_back(c.type);
        schema_lock.unlock(); // DROP THE LOCK! We parse in parallel now.

        // PHASE 2: Lock-free parsing (ZERO HEAP ALLOCATION ON HOT PATH)
        size_t remaining_len = query.length() - val_pos;
        size_t est_rows = (remaining_len / 30) + 10;
        
        // Thread-local keeps the memory alive across thousands of queries, bypassing malloc
        thread_local std::vector<double> tl_batch_keys;
        thread_local std::vector<Row*> tl_batch_rows;
        thread_local std::vector<char> tl_bin_buf;

        tl_batch_keys.clear();
        tl_batch_rows.clear();
        tl_bin_buf.clear();

        if (tl_batch_keys.capacity() < est_rows) {
            tl_batch_keys.reserve(est_rows);
            tl_batch_rows.reserve(est_rows);
        }
        
        size_t req_bin_cap = 256 + remaining_len * 2;
        if (tl_bin_buf.capacity() < req_bin_cap) tl_bin_buf.reserve(req_bin_cap);
        tl_bin_buf.resize(req_bin_cap); 
        char* bin_buf = tl_bin_buf.data();
        size_t bin_off = 0;

        bin_buf[bin_off++] = 0x02;
        uint16_t tnl = (uint16_t)t_name.size();
        memcpy(bin_buf + bin_off, &tnl, 2); bin_off += 2;
        memcpy(bin_buf + bin_off, t_name.data(), tnl); bin_off += tnl;
        size_t num_rows_pos = bin_off;
        bin_off += 4; 
        uint16_t nc16 = (uint16_t)num_cols;
        memcpy(bin_buf + bin_off, &nc16, 2); bin_off += 2;

        uint32_t row_count = 0;
        const char* ptr = query.data() + val_pos + 6;
        const char* end_ptr = query.data() + query.length();

        while (ptr < end_ptr) {
            const char* next_paren = (const char*)memchr(ptr, '(', end_ptr - ptr);
            if (!next_paren) break;
            ptr = next_paren + 1;

            Row* new_row = allocate_row_fast();
            new_row->reset();

            while (ptr < end_ptr && *ptr != ')') {
                while (ptr < end_ptr && *ptr <= ' ' && *ptr != '\0') ptr++;
                if (ptr < end_ptr && *ptr == '\'') ptr++;

                const char* val_start = ptr;
                
                // Raw CPU pointer scan: Fast for typical DB fields
                while (ptr < end_ptr && *ptr != ',' && *ptr != ')') ptr++;
                
                const char* val_end = ptr - 1;
                while (val_end >= val_start && (*val_end <= ' ' || *val_end == '\'')) val_end--;

                size_t len = (val_end >= val_start) ? (val_end - val_start + 1) : 0;

                if (new_row->size() < num_cols) {
                    if (!fast_validate_type(val_start, len, expected_types[new_row->size()])) {
                        send_response(client_fd, "Error: Type Mismatch.\n");
                        new_row->is_dead = true;
                        return 1;
                    }
                }

                uint16_t vl = (uint16_t)len;
                memcpy(bin_buf + bin_off, &vl, 2); bin_off += 2;
                if (len > 0) { memcpy(bin_buf + bin_off, val_start, len); bin_off += len; }

                new_row->append_value(val_start, len);
                if (ptr < end_ptr && *ptr == ',') ptr++;
            }

            if (new_row->size() != num_cols) {
                send_response(client_fd, "Error: Column count mismatch.\n");
                new_row->is_dead = true;
                return 1;
            }

            if (expire_idx != -1 && expire_idx < (int)new_row->size()) {
                auto val = new_row->get_value(expire_idx);
                new_row->expires_at = (long long)fast_atod(val.data(), val.length());
            } else {
                new_row->expires_at = 2051222400; 
            }
            new_row->is_dead = false;

            double pk = 0.0;
            if (pk_idx >= 0 && pk_idx < (int)new_row->size()) {
                auto pk_val = new_row->get_value(pk_idx);
                pk = fast_atod(pk_val.data(), pk_val.length());
            }

            tl_batch_keys.push_back(pk);
            tl_batch_rows.push_back(new_row);
            row_count++;
        }

        // PHASE 3: Fast write-lock & Bulk Commit
        if (row_count > 0) {
            std::unique_lock<std::shared_mutex> write_lock(*t_lock_ptr);
            if (row_count > 100) t->index->reserve(t->index->total_rows() + row_count);
            t->index->insert_batch(tl_batch_keys, tl_batch_rows);
            
            if (client_fd != -1) {
                memcpy(bin_buf + num_rows_pos, &row_count, 4);
                g_data_writer.append(bin_buf, bin_off);

                // Forces the OS to immediately write this batch to the physical hard drive
                // g_data_writer.sync_to_disk();
            }
        }

        global_cache.clear();
        send_response(client_fd, "Rows Inserted.\n");
        return 0;
    }

    // 
    // 4. SELECT (JOIN & STANDARD SELECT with Zero-Copy Streaming)
     if (query.find("SELECT") == 0) {
        std::string cached_result;
        if (global_cache.get(query, cached_result)) {
            send_response(client_fd, cached_result);
            return 0;
        }

        size_t from_pos = query.find(" FROM ");
        if (from_pos == std::string::npos) {
            send_response(client_fd, "Error: Syntax Error missing FROM.\n");
            return 1;
        }

        time_t current_time = std::time(nullptr);

        // --- ZERO COPY STREAMING BUFFER SETUP ---
        constexpr size_t OUT_BUF_SIZE = 1048576; // 1MB Buffer
        char* out_buf = (char*)malloc(OUT_BUF_SIZE);
        size_t out_pos = 0;
        int match_count = 0;

        // Smart Caching: Only cache queries that return < 1MB of data
        std::string cache_builder; 
        bool cache_eligible = true;

        auto flush_out_buf = [&](bool is_final = false) {
            if (out_pos > 0) {
                // Do not cache the final null-terminator byte
                size_t cache_len = is_final ? out_pos - 1 : out_pos;
                
                if (cache_eligible && cache_len > 0) {
                    if (cache_builder.size() + cache_len > 1048576) { 
                        cache_eligible = false;
                        cache_builder.clear(); // Free memory immediately
                    } else {
                        cache_builder.append(out_buf, cache_len);
                    }
                }
                
                // Blast raw bytes directly to the network card
                if (client_fd != -1) {
                    size_t total_sent = 0;
                    while (total_sent < out_pos) {
                        ssize_t sent = write(client_fd, out_buf + total_sent, out_pos - total_sent);
                        if (sent <= 0) break;
                        total_sent += sent;
                    }
                }
                out_pos = 0;
            }
        };

        // JOIN 

        if (query.find(" JOIN ") != std::string::npos) {
            size_t join_pos = query.find(" INNER JOIN ");
            size_t on_pos = query.find(" ON ");
            size_t where_pos = query.find(" WHERE ");

            if (join_pos == std::string::npos || on_pos == std::string::npos) {
                free(out_buf);
                send_response(client_fd, "Error: Syntax Error in JOIN.\n");
                return 1;
            }

            std::string t1_name = trim(query.substr(from_pos + 6, join_pos - (from_pos + 6)));
            std::string t2_name = trim(query.substr(join_pos + 12, on_pos - (join_pos + 12)));

            catalog_mutex.lock_shared();
            Table* t1 = db_catalog.get_table(t1_name);
            Table* t2 = db_catalog.get_table(t2_name);
            catalog_mutex.unlock_shared();
            
            if (!t1 || !t2) {
                free(out_buf);
                send_response(client_fd, "Error: Table missing.\n");
                return 1;
            }

            auto t1_lock_ptr = get_table_mutex(t1_name);
            auto t2_lock_ptr = get_table_mutex(t2_name);
            std::shared_lock<std::shared_mutex> read_lock1(*t1_lock_ptr, std::defer_lock);
            std::shared_lock<std::shared_mutex> read_lock2(*t2_lock_ptr, std::defer_lock);
            std::lock(read_lock1, read_lock2);

            std::string req_cols_str = trim(query.substr(6, from_pos - 6));
            std::vector<std::pair<int,int>> target_cols;

            if (req_cols_str == "*") {
                for (size_t i = 0; i < t1->cols.size(); i++) target_cols.push_back({1, i});
                for (size_t i = 0; i < t2->cols.size(); i++) target_cols.push_back({2, i});
            } else {
                std::stringstream ss(req_cols_str);
                std::string col_token;
                while (std::getline(ss, col_token, ',')) {
                    std::string c = to_upper(strip_table_prefix(trim(col_token)));
                    bool found = false;
                    for (size_t i = 0; i < t1->cols.size(); i++)
                        if (t1->cols[i].name == c) { target_cols.push_back({1, i}); found = true; break; }
                    if (!found)
                        for (size_t i = 0; i < t2->cols.size(); i++)
                            if (t2->cols[i].name == c) { target_cols.push_back({2, i}); found = true; break; }
                    if (!found) {
                        free(out_buf);
                        send_response(client_fd, "Error: Unknown target column.\n");
                        return 1;
                    }
                }
            }

            std::string on_clause = (where_pos != std::string::npos) ?
                trim(query.substr(on_pos + 4, where_pos - (on_pos + 4))) :
                trim(query.substr(on_pos + 4));
            size_t eq_pos = on_clause.find("=");
            std::string left_col = to_upper(strip_table_prefix(trim(on_clause.substr(0, eq_pos))));
            std::string right_col = to_upper(strip_table_prefix(trim(on_clause.substr(eq_pos + 1))));

            int j1_idx = -1, j2_idx = -1;
            for (size_t i = 0; i < t1->cols.size(); i++)
                if (t1->cols[i].name == left_col || t1->cols[i].name == right_col) j1_idx = i;
            for (size_t i = 0; i < t2->cols.size(); i++)
                if (t2->cols[i].name == left_col || t2->cols[i].name == right_col) j2_idx = i;

            int f_tid = 0, f_idx = -1;
            double f_target = 0;
            std::string f_target_str = "";
            std::string f_op = "";
            size_t op_len = 0;
            bool is_target_numeric = true;

            if (where_pos != std::string::npos) {
                std::string where_cond = trim(query.substr(where_pos + 7));
                size_t op_pos = std::string::npos;

                if ((op_pos = where_cond.find(">=")) != std::string::npos) { f_op = ">="; op_len = 2; }
                else if ((op_pos = where_cond.find("<=")) != std::string::npos) { f_op = "<="; op_len = 2; }
                else if ((op_pos = where_cond.find("!=")) != std::string::npos) { f_op = "!="; op_len = 2; }
                else if ((op_pos = where_cond.find("=")) != std::string::npos) { f_op = "="; op_len = 1; }
                else if ((op_pos = where_cond.find(">")) != std::string::npos) { f_op = ">"; op_len = 1; }
                else if ((op_pos = where_cond.find("<")) != std::string::npos) { f_op = "<"; op_len = 1; }

                if (f_op != "") {
                    std::string wc_name = to_upper(strip_table_prefix(trim(where_cond.substr(0, op_pos))));
                    f_target_str = trim(where_cond.substr(op_pos + op_len));
                    if (f_target_str.front() == '\'')
                        f_target_str = f_target_str.substr(1, f_target_str.length() - 2);
                    try { f_target = std::stod(f_target_str); } catch (...) { is_target_numeric = false; }

                    for (size_t i = 0; i < t1->cols.size(); i++)
                        if (t1->cols[i].name == wc_name) { f_tid = 1; f_idx = i; break; }
                    if (f_tid == 0)
                        for (size_t i = 0; i < t2->cols.size(); i++)
                            if (t2->cols[i].name == wc_name) { f_tid = 2; f_idx = i; break; }

                    if (f_idx == -1) {
                        free(out_buf);
                        send_response(client_fd, "Error: Unknown WHERE column.\n");
                        return 1;
                    }
                }
            }

            std::vector<Row*> r1_list, r2_list;
            t1->index->get_all_rows(r1_list);
            t2->index->get_all_rows(r2_list);

            
            // HASH JOIN ALGORITHM (O(N + M) implementation)
            // Uses zero-allocation string_views backed by active read-locks
            
            std::unordered_multimap<std::string_view, Row*> hash_table;
            hash_table.reserve(r2_list.size());

            // 1. BUILD PHASE: Load Table 2 into Hash Map
            for (Row* r2 : r2_list) {
                if (!r2 || r2->is_dead || r2->expires_at < current_time || r2->size() == 0) continue;
                if (j2_idx >= 0 && j2_idx < (int)r2->size()) {
                    hash_table.emplace(r2->get_value(j2_idx), r2);
                }
            }

            // 2. PROBE PHASE: Scan Table 1 and instant-lookup in Hash Map
            for (Row* r1 : r1_list) {
                if (!r1 || r1->is_dead || r1->expires_at < current_time || r1->size() == 0) continue;
                
                if (j1_idx >= 0 && j1_idx < (int)r1->size()) {
                    auto range = hash_table.equal_range(r1->get_value(j1_idx));
                    
                    for (auto it = range.first; it != range.second; ++it) {
                        Row* r2 = it->second;

                        // Verify Optional WHERE Clause
                        if (f_idx != -1) {
                            Row* fr = (f_tid == 1) ? r1 : r2;
                            if (f_idx >= (int)fr->size()) continue;
                            auto f_val = fr->get_value(f_idx);
                            if (is_target_numeric) {
                                double val = fast_atod(f_val.data(), f_val.length());
                                if (f_op == ">" && val <= f_target) continue;
                                if (f_op == "<" && val >= f_target) continue;
                                if (f_op == "=" && val != f_target) continue;
                                if (f_op == ">=" && val < f_target) continue;
                                if (f_op == "<=" && val > f_target) continue;
                                if (f_op == "!=" && val == f_target) continue;
                            } else {
                                if (f_op == "=" && f_val != f_target_str) continue;
                                if (f_op == "!=" && f_val == f_target_str) continue;
                            }
                        }

                        match_count++;
                        
                        // Buffer bounds check
                        size_t req_space = target_cols.size(); 
                        for(size_t i = 0; i < target_cols.size(); i++) {
                            Row* target_r = (target_cols[i].first == 1) ? r1 : r2;
                            int c_idx = target_cols[i].second;
                            req_space += (c_idx >= 0 && c_idx < (int)target_r->size()) ? target_r->get_value(c_idx).length() : 4;
                        }

                        if (out_pos + req_space > OUT_BUF_SIZE) flush_out_buf();

                        // Fast Memory Copy
                        for (size_t i = 0; i < target_cols.size(); i++) {
                            Row* target_r = (target_cols[i].first == 1) ? r1 : r2;
                            int c_idx = target_cols[i].second;
                            if (c_idx >= 0 && c_idx < (int)target_r->size()) {
                                auto val = target_r->get_value(c_idx);
                                memcpy(out_buf + out_pos, val.data(), val.length());
                                out_pos += val.length();
                            } else {
                                memcpy(out_buf + out_pos, "NULL", 4);
                                out_pos += 4;
                            }
                            if (i < target_cols.size() - 1) out_buf[out_pos++] = '|';
                        }
                        out_buf[out_pos++] = '\n';
                    }
                }
            }
        } 
        // STANDARD SELECT 
        else {
            size_t where_pos = query.find(" WHERE ");
            std::string t_name = (where_pos != std::string::npos) ?
                trim(query.substr(from_pos + 6, where_pos - (from_pos + 6))) :
                trim(query.substr(from_pos + 6));

            catalog_mutex.lock_shared();
            Table* t = db_catalog.get_table(t_name);
            catalog_mutex.unlock_shared();

            if (!t) {
                free(out_buf);
                send_response(client_fd, "Error: Table missing.\n");
                return 1;
            }

            auto t_lock_ptr = get_table_mutex(t_name);
            std::shared_lock<std::shared_mutex> read_lock(*t_lock_ptr);

            std::string req_cols_str = trim(query.substr(6, from_pos - 6));
            std::vector<int> target_cols;
            if (req_cols_str == "*") {
                for (size_t i = 0; i < t->cols.size(); i++) target_cols.push_back(i);
            } else {
                std::stringstream ss(req_cols_str);
                std::string col_token;
                while (std::getline(ss, col_token, ',')) {
                    std::string c = to_upper(strip_table_prefix(trim(col_token)));
                    bool found = false;
                    for (size_t i = 0; i < t->cols.size(); i++) {
                        if (t->cols[i].name == c) { target_cols.push_back(i); found = true; break; }
                    }
                    if (!found) {
                        free(out_buf);
                        send_response(client_fd, "Error: Unknown target column.\n");
                        return 1;
                    }
                }
            }

            int f_idx = -1;
            double f_target = 0;
            std::string f_target_str = "";
            std::string f_op = "";
            size_t op_len = 0;
            bool is_target_numeric = true;

            if (where_pos != std::string::npos) {
                std::string where_cond = trim(query.substr(where_pos + 7));
                size_t op_pos = std::string::npos;

                if ((op_pos = where_cond.find(">=")) != std::string::npos) { f_op = ">="; op_len = 2; }
                else if ((op_pos = where_cond.find("<=")) != std::string::npos) { f_op = "<="; op_len = 2; }
                else if ((op_pos = where_cond.find("!=")) != std::string::npos) { f_op = "!="; op_len = 2; }
                else if ((op_pos = where_cond.find("=")) != std::string::npos) { f_op = "="; op_len = 1; }
                else if ((op_pos = where_cond.find(">")) != std::string::npos) { f_op = ">"; op_len = 1; }
                else if ((op_pos = where_cond.find("<")) != std::string::npos) { f_op = "<"; op_len = 1; }

                if (f_op != "") {
                    std::string wc_name = to_upper(strip_table_prefix(trim(where_cond.substr(0, op_pos))));
                    f_target_str = trim(where_cond.substr(op_pos + op_len));
                    if (f_target_str.front() == '\'')
                        f_target_str = f_target_str.substr(1, f_target_str.length() - 2);
                    try { f_target = std::stod(f_target_str); } catch (...) { is_target_numeric = false; }

                    for (size_t i = 0; i < t->cols.size(); i++)
                        if (t->cols[i].name == wc_name) { f_idx = i; break; }

                    if (f_idx == -1) {
                        free(out_buf);
                        send_response(client_fd, "Error: Unknown WHERE column.\n");
                        return 1;
                    }
                } else {
                    free(out_buf);
                    send_response(client_fd, "Error: Syntax Error in WHERE.\n");
                    return 1;
                }
            }

            // O(1) PK fast path
            if (f_idx == 0 && f_op == "=" && is_target_numeric) {
                Row* r = t->index->search(f_target);
                if (r && !r->is_dead && r->expires_at >= current_time) {
                    match_count++;
                    size_t req_space = target_cols.size();
                    for (int c_idx : target_cols) req_space += (c_idx >= 0 && c_idx < (int)r->size()) ? r->get_value(c_idx).length() : 4;
                    if (out_pos + req_space > OUT_BUF_SIZE) flush_out_buf();

                    for (size_t i = 0; i < target_cols.size(); i++) {
                        int c_idx = target_cols[i];
                        if (c_idx >= 0 && c_idx < (int)r->size()) {
                            auto val = r->get_value(c_idx);
                            memcpy(out_buf + out_pos, val.data(), val.length());
                            out_pos += val.length();
                        } else {
                            memcpy(out_buf + out_pos, "NULL", 4);
                            out_pos += 4;
                        }
                        if (i < target_cols.size() - 1) out_buf[out_pos++] = '|';
                    }
                    out_buf[out_pos++] = '\n';
                }
            } 
            // O(N) full table scan fallback
            else {
                std::vector<Row*> all_rows;
                t->index->get_all_rows(all_rows);

                for (Row* r : all_rows) {
                    if (!r || r->is_dead || r->expires_at < current_time || r->size() == 0) continue;

                    bool match = true;
                    if (f_idx != -1 && f_idx < (int)r->size()) {
                        auto r_val = r->get_value(f_idx);
                        if (is_target_numeric) {
                            double val = fast_atod(r_val.data(), r_val.length());
                            if (f_op == ">" && val <= f_target) match = false;
                            else if (f_op == "<" && val >= f_target) match = false;
                            else if (f_op == "=" && val != f_target) match = false;
                            else if (f_op == ">=" && val < f_target) match = false;
                            else if (f_op == "<=" && val > f_target) match = false;
                            else if (f_op == "!=" && val == f_target) match = false;
                        } else {
                            if (f_op == "=" && r_val != f_target_str) match = false;
                            else if (f_op == "!=" && r_val == f_target_str) match = false;
                        }
                    }

                    if (match) {
                        match_count++;
                        
                        size_t req_space = target_cols.size();
                        for (int c_idx : target_cols) req_space += (c_idx >= 0 && c_idx < (int)r->size()) ? r->get_value(c_idx).length() : 4;
                        if (out_pos + req_space > OUT_BUF_SIZE) flush_out_buf();

                        for (size_t i = 0; i < target_cols.size(); i++) {
                            int c_idx = target_cols[i];
                            if (c_idx >= 0 && c_idx < (int)r->size()) {
                                auto val = r->get_value(c_idx);
                                memcpy(out_buf + out_pos, val.data(), val.length());
                                out_pos += val.length();
                            } else {
                                memcpy(out_buf + out_pos, "NULL", 4);
                                out_pos += 4;
                            }
                            if (i < target_cols.size() - 1) out_buf[out_pos++] = '|';
                        }
                        out_buf[out_pos++] = '\n';
                    }
                }
            }
        }

        // --- FINAL FLUSH & CLEANUP ---
        if (match_count == 0) {
            const char* empty_msg = "0 rows returned.\n";
            size_t len = strlen(empty_msg);
            memcpy(out_buf + out_pos, empty_msg, len);
            out_pos += len;
        }

        // Add Null Terminator required by the client loop
        if (out_pos + 1 > OUT_BUF_SIZE) flush_out_buf();
        out_buf[out_pos++] = '\0';
        
        // Trigger final flush (true = exclude the \0 from cache)
        flush_out_buf(true); 

        // Safely cache only if the result was small enough
        if (cache_eligible && match_count > 0) {
            global_cache.put(query, cache_builder);
        }

        free(out_buf);
        return 0;
    }
   
    
    // 5. DESCRIBE TABLE (O(1) Schema Access)
    if (query.find("DESCRIBE ") == 0) {
        std::string t_name = trim(query.substr(9));
        if (t_name.back() == ';') t_name.pop_back(); // Remove trailing semicolon if present

        catalog_mutex.lock_shared();
        Table* t = db_catalog.get_table(t_name);
        catalog_mutex.unlock_shared();

        if (!t) {
            send_response(client_fd, "Error: Table missing.\n");
            return 1;
        }

        auto t_lock_ptr = get_table_mutex(t_name);
        std::shared_lock<std::shared_mutex> read_lock(*t_lock_ptr);

        std::string response = "COLUMN_NAME | DATA_TYPE\n";
        response += "------------------------\n";

        for (const auto& col : t->cols) {
            std::string type_str;
            switch(col.type) {
                case TYPE_INT: type_str = "INT"; break;
                case TYPE_VARCHAR: type_str = "VARCHAR"; break;
                case TYPE_DECIMAL: type_str = "DECIMAL"; break;
                case TYPE_DATETIME: type_str = "DATETIME"; break;
                default: type_str = "UNKNOWN"; break;
            }
            response += col.name + " | " + type_str + "\n";
        }
        
        send_response(client_fd, response);
        return 0;
    }

    send_response(client_fd, "Error: Unknown Command.\n");
    return 1;
}
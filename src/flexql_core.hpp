#pragma once
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <algorithm>
#include <string_view>
#include <cstring>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <fcntl.h>
#include <unistd.h>

enum DataType
{
    TYPE_INT = 1,
    TYPE_VARCHAR = 2,
    TYPE_DECIMAL = 3,
    TYPE_DATETIME = 4
};

struct Column
{
    std::string name;
    DataType type;
};

struct alignas(8) Row
{
    long long expires_at = 0;      // 8 bytes (Top for strict alignment)
    char buffer[80];               // 80 bytes
    uint8_t val_offsets[10];       // 10 bytes
    uint8_t val_lens[10];          // 10 bytes
    uint8_t buffer_offset = 0;     // 1 byte
    uint8_t col_count = 0;         // 1 byte
    bool is_dead = false;          // 1 byte
                                   // Total: 111 bytes + 1 byte padding = 112 bytes!

    size_t size() const { return col_count; }

    void reset()
    {
        buffer_offset = 0;
        col_count = 0;
    }

    inline void append_value(const char *start, size_t len)
    {
        if (col_count < 10)
        {
            size_t copy_len = std::min((size_t)len, (size_t)(80 - buffer_offset));
            if (copy_len > 0)
            {
                std::memcpy(buffer + buffer_offset, start, copy_len);
            }
            val_offsets[col_count] = buffer_offset;
            val_lens[col_count] = copy_len;
            buffer_offset += copy_len;
            col_count++;
        }
    }

    // Generates the string_view on the fly in CPU registers, costing ZERO memory!
    inline std::string_view get_value(int idx) const
    {
        if (idx < 0 || idx >= col_count) return {};
        return std::string_view(buffer + val_offsets[idx], val_lens[idx]);
    }
};

// OPTIMIZED INDEX: vector for scan + LAZY unordered_map for O(1) PK lookup
// Key insight: INSERT only appends to vectors (fast, sequential, cache-friendly).
// The hash map is built on-demand at first SELECT, avoiding millions of
// expensive hash insertions during bulk loading.
class BTree
{
private:
    std::vector<Row *> all_rows;
    std::vector<double> all_keys;          
    std::unordered_map<double, Row *> pk_index;
    std::atomic<bool> pk_valid{false};     
    std::mutex pk_build_mtx;               
    size_t indexed_count = 0; //  Tracks how many rows are already safely in the hash map!

public:
    BTree()
    {
        all_rows.reserve(4096);
        all_keys.reserve(4096);
    }

    inline void insert(double key, Row *row_data)
    {
        all_rows.push_back(row_data);
        all_keys.push_back(key);
        pk_valid.store(false, std::memory_order_relaxed);
    }
    inline void insert_batch(const std::vector<double>& keys, const std::vector<Row*>& rows)
    {
        size_t n = rows.size();
        if (n == 0) return;
        
        // Single allocation and block memory copy instead of N push_backs
        all_rows.insert(all_rows.end(), rows.begin(), rows.end());
        all_keys.insert(all_keys.end(), keys.begin(), keys.end());
        
        pk_valid.store(false, std::memory_order_relaxed);
    }
    void ensure_pk_index()
    {
        if (pk_valid.load(std::memory_order_acquire)) return;
        std::lock_guard<std::mutex> lk(pk_build_mtx);
        if (pk_valid.load(std::memory_order_relaxed)) return;

         // Do NOT clear the map. Just pick up where we left off and only hash the newest rows!
        size_t current_size = all_rows.size();
        for (size_t i = indexed_count; i < current_size; i++)
        {
            Row *r = all_rows[i];
            if (r && !r->is_dead)
                pk_index[all_keys[i]] = r;
        }
        
        indexed_count = current_size; // Update our bookmark
        pk_valid.store(true, std::memory_order_release);
        // ------------------------------------
    }

    Row *search(double key)
    {
        ensure_pk_index();
        auto it = pk_index.find(key);
        if (it != pk_index.end())
            return it->second;
        return nullptr;
    }

    void get_all_rows(std::vector<Row *> &out_rows)
    {
        out_rows.reserve(all_rows.size());
        for (auto *r : all_rows)
        {
            if (r && !r->is_dead)
                out_rows.push_back(r);
        }
    }

    size_t total_rows() const { return all_rows.size(); }

    void reserve(size_t n)
    {
        if (n > all_rows.capacity())
        {
            size_t next_cap = std::max(n, all_rows.capacity() * 2);
            all_rows.reserve(next_cap);
            all_keys.reserve(next_cap);
        }
    }

    void clear()
    {
        all_rows.clear();
        all_keys.clear();
        pk_index.clear();
        indexed_count = 0; // <-- Reset the bookmark on clear!
        pk_valid.store(false, std::memory_order_relaxed);
    }
};

struct Table
{
    std::string name;
    std::vector<Column> cols;
    BTree *index;
    int pk_index = 0;

    Table(std::string n) : name(n) { index = new BTree(); }
};

class Catalog
{
public:
    std::unordered_map<std::string, Table *> tables;
    std::mutex catalog_lock;

    Table *get_table(const std::string &name)
    {
        std::lock_guard lock(catalog_lock);
        auto it = tables.find(name);
        if (it != tables.end())
            return it->second;
        return nullptr;
    }

    Table *create_table(const std::string &name)
    {
        std::lock_guard lock(catalog_lock);
        if (tables.find(name) != tables.end())
            return nullptr;
        Table *t = new Table(name);
        tables[name] = t;
        return t;
    }

    void drop_table(const std::string &name)
    {
        std::lock_guard lock(catalog_lock);
        tables.erase(name);
    }
};

// ASYNC BINARY DATA FILE WRITER (double-buffered, non-blocking)
class DataWriter
{
    int fd = -1;
    static constexpr size_t BUF_CAP = 32 * 1024 * 1024;
    char *buf_a;
    char *buf_b;
    char *active_buf;
    size_t active_off = 0;

    char *flush_buf = nullptr;
    size_t flush_size = 0;
    bool has_pending = false;

    std::mutex mtx;
    std::mutex append_mtx;
    std::condition_variable cv;
    std::thread bg_thread;
    std::atomic<bool> running{true};

    void bg_worker()
    {
        while (running.load(std::memory_order_relaxed))
        {
            std::unique_lock<std::mutex> lk(mtx);
            cv.wait(lk, [&] { return has_pending || !running; });
            if (!has_pending) continue;
           
            char *to_write = flush_buf;
            size_t to_write_size = flush_size;
            has_pending = false;
            lk.unlock();
            cv.notify_all();

            if (to_write_size > 0 && fd >= 0)
            {
                size_t written = 0;
                while (written < to_write_size)
                {
                    ssize_t r = ::write(fd, to_write + written, to_write_size - written);
                    if (r <= 0) break;
                    written += r;
                }
                // ::fdatasync(fd);
            }
        }
    }
 
    void swap_and_flush()
    {
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [&] { return !has_pending; });
       
        flush_buf = active_buf;
        flush_size = active_off;
        has_pending = true;
        active_buf = (active_buf == buf_a) ? buf_b : buf_a;
        active_off = 0;
        cv.notify_one();
    }

public:
    DataWriter()
    {
        buf_a = new char[BUF_CAP];
        buf_b = new char[BUF_CAP];
        active_buf = buf_a;
    }

    void open(const char *path)
    {
        fd = ::open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        running = true;
        bg_thread = std::thread(&DataWriter::bg_worker, this);
    }

    inline void append(const void *data, size_t len)
    {
        std::lock_guard<std::mutex> app_lk(append_mtx);
       
        if (active_off + len > BUF_CAP) {
            swap_and_flush();
        }
        std::memcpy(active_buf + active_off, data, len);
        active_off += len;
    }

    void flush()
    {
        std::lock_guard<std::mutex> app_lk(append_mtx);
        if (active_off > 0) {
            swap_and_flush();
        }
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [&] { return !has_pending; });
    }

    void sync_to_disk()
    {
        flush();
        std::lock_guard<std::mutex> lk(mtx);
        if (fd >= 0) {
            ::fdatasync(fd);
        }
    }

    int raw_fd() const { return fd; }

    ~DataWriter()
    {
        flush();
        running = false;
        cv.notify_all();
        if (bg_thread.joinable()) bg_thread.join();
        if (fd >= 0) ::close(fd);
        delete[] buf_a;
        delete[] buf_b;
    }
};

// Global catalog
extern Catalog db_catalog;
// Global data writer
extern DataWriter g_data_writer;
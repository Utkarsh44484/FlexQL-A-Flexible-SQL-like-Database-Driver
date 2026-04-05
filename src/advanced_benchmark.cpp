//  This is just a custom testing file





// How to use it:

//     Start your FlexQL server in one terminal.

//     Compile the script: g++ advanced_benchmark.cpp flexql_api.cpp -o advanced_bench -O3 -lpthread

//     Run it with default settings (4 clients): ./advanced_bench

//     Run it with custom settings (e.g., 10 clients doing 100,000 operations each): ./advanced_bench 10 100000

#include <iostream>
#include <chrono>
#include <string>
#include <sstream>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include "flexql.h"

using namespace std;
using namespace std::chrono;

const int INSERT_BATCH_SIZE = 1000;

enum WorkloadType {
    WRITE_HEAVY,
    READ_HEAVY,
    MIXED
};

// Thread-safe console printing
mutex print_mtx;
void safe_print(const string& msg) {
    lock_guard<mutex> lock(print_mtx);
    cout << msg;
}

// Dummy callback to consume SELECT rows without printing them
static int consume_rows_callback(void *data, int argc, char **argv, char **azColName) {
    long long* row_count = static_cast<long long*>(data);
    if (row_count) (*row_count)++;
    return 0;
}

// Global atomic ID generator to prevent unique key collisions across concurrent threads
atomic<long long> global_insert_id{1};

void client_worker(int client_id, WorkloadType type, int operations, atomic<long long>& total_success) {
    FlexQL *db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        safe_print("Client " + to_string(client_id) + " failed to connect.\n");
        return;
    }

    int success_count = 0;
    char *errMsg = nullptr;

    // Determine what this specific thread should do
    bool do_writes = (type == WRITE_HEAVY) || (type == MIXED && client_id % 2 == 0);
    bool do_reads  = (type == READ_HEAVY) || (type == MIXED && client_id % 2 != 0);

    int ops_completed = 0;
    while (ops_completed < operations) {
        
        // --- WRITE OPERATION ---
        if (do_writes) {
            stringstream ss;
            ss << "INSERT INTO CONCURRENT_TEST VALUES ";
            
            int in_batch = 0;
            while (in_batch < INSERT_BATCH_SIZE && ops_completed < operations) {
                long long id = global_insert_id.fetch_add(1);
                ss << "(" << id << ", 'User" << id << "', " << (id % 1000) << ", 2147483647)";
                
                ops_completed++;
                in_batch++;
                if (in_batch < INSERT_BATCH_SIZE && ops_completed < operations) ss << ",";
            }
            ss << ";";

            if (flexql_exec(db, ss.str().c_str(), nullptr, nullptr, &errMsg) == FLEXQL_OK) {
                success_count += in_batch;
            } else {
                if (errMsg) { flexql_free(errMsg); errMsg = nullptr; }
            }
        } 
        
        // --- READ OPERATION ---
       // --- READ OPERATION (FAST PATH: PRIMARY KEY) ---
        else if (do_reads) {
            // Cycle through IDs that we know exist in the database to test the B-Tree
            long long target_id = (ops_completed % 100000) + 1; 
            
            stringstream ss;
            // Now querying strictly by ID so it hits the O(log N) Fast Path!
            ss << "SELECT NAME FROM CONCURRENT_TEST WHERE ID = " << target_id << ";";
            
            long long rows_found = 0;
            if (flexql_exec(db, ss.str().c_str(), consume_rows_callback, &rows_found, &errMsg) == FLEXQL_OK) {
                success_count++;
            } else {
                if (errMsg) { flexql_free(errMsg); errMsg = nullptr; }
            }
            ops_completed++;
        }

        // Non pk
        // else if (do_reads) {
        //     // Pick a balance we know exists
        //     long long target_balance = (ops_completed % 1000); 
            
        //     stringstream ss;
        //     // Querying BALANCE forces the database to check every single row!
        //     ss << "SELECT NAME FROM CONCURRENT_TEST WHERE BALANCE = " << target_balance << ";";
            
        //     long long rows_found = 0;
        //     if (flexql_exec(db, ss.str().c_str(), consume_rows_callback, &rows_found, &errMsg) == FLEXQL_OK) {
        //         success_count++;
        //     } else {
        //         if (errMsg) { flexql_free(errMsg); errMsg = nullptr; }
        //     }
        //     ops_completed++;
        // }
    }

    total_success += success_count;
    flexql_close(db);
}

void run_multithreaded_test(string test_name, WorkloadType type, int num_clients, int ops_per_client) {
    cout << "\n======================================================\n";
    cout << "RUNNING: " << test_name << "\n";
    cout << "Clients: " << num_clients << " | Ops per client: " << ops_per_client << "\n";
    cout << "======================================================\n";

    atomic<long long> total_success{0};
    vector<thread> clients;

    auto start_time = high_resolution_clock::now();

    for (int i = 0; i < num_clients; ++i) {
        clients.emplace_back(client_worker, i, type, ops_per_client, ref(total_success));
    }

    for (auto& t : clients) {
        t.join();
    }

    auto end_time = high_resolution_clock::now();
    long long elapsed_ms = duration_cast<milliseconds>(end_time - start_time).count();
    
    long long total_ops = num_clients * ops_per_client;
    long long throughput = (elapsed_ms > 0) ? (total_success.load() * 1000LL / elapsed_ms) : 0;

    cout << "-> Test Completed in " << elapsed_ms << " ms\n";
    cout << "-> Successful Ops: " << total_success.load() << " / " << total_ops << "\n";
    cout << "-> Throughput:     " << throughput << " operations/sec\n";
}

int main(int argc, char **argv) {
    int num_clients = 4;        // Default to 4 concurrent client connections
    int ops_per_client = 50000; // Default to 50k ops per client

    if (argc == 3) {
        num_clients = stoi(argv[1]);
        ops_per_client = stoi(argv[2]);
    }

    // 1. SETUP PHASE: Create the table using a single connection first
// 1. SETUP PHASE: Create the table using a single connection first
    FlexQL *setup_db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &setup_db) != FLEXQL_OK) {
        cout << "Fatal: Server not running on 127.0.0.1:9000\n";
        return 1;
    }
    
    char* errMsg = nullptr;
    cout << "Setting up schema for concurrent tests...\n";
    
    // STRICT FORMAT ENFORCEMENT: Only "column TYPE" as per Section 3.a
    const char* strict_create_sql = "CREATE TABLE CONCURRENT_TEST(ID DECIMAL, NAME VARCHAR, BALANCE DECIMAL, EXPIRES_AT DECIMAL);";
    
    flexql_exec(setup_db, strict_create_sql, nullptr, nullptr, &errMsg);
    if(errMsg) { 
        flexql_free(errMsg); 
        errMsg = nullptr; 
    }
    flexql_close(setup_db);
    // 2. RUN BENCHMARKS
    // Phase 1: Write-Heavy (All clients inserting at the same time)
    run_multithreaded_test("WRITE-HEAVY (100% Inserts)", WRITE_HEAVY, num_clients, ops_per_client);

    // Phase 2: Read-Heavy (All clients selecting at the same time)
    // We lower the ops for reading because SELECTs don't batch like inserts do over the network.
    run_multithreaded_test("READ-HEAVY (100% Selects)", READ_HEAVY, num_clients, 1000);

    // Phase 3: Mixed Workload (Half clients inserting, Half clients selecting)
    run_multithreaded_test("MIXED WORKLOAD (50% Read / 50% Write)", MIXED, num_clients, 5000);

    cout << "\nAll concurrent benchmarks completed.\n";

    // ==========================================================
    // 3. CORRECTNESS VERIFICATION PHASE
    // ==========================================================
   // ==========================================================
    // 3. CORRECTNESS VERIFICATION PHASE (CORRECTED MATH)
    // ==========================================================
    cout << "\n======================================================\n";
    cout << "VERIFYING DATA CORRECTNESS...\n";
    cout << "======================================================\n";
    
    FlexQL *verify_db = nullptr;
    flexql_open("127.0.0.1", 9000, &verify_db);
    
    // Test 1: Did we lose any rows due to multithreaded race conditions?
    long long total_rows = 0;
    flexql_exec(verify_db, "SELECT NAME FROM CONCURRENT_TEST WHERE BALANCE >= 0;", consume_rows_callback, &total_rows, nullptr);
    
    // Calculate true expected rows: Write-Heavy (All clients) + Mixed (Half clients)
    long long expected_rows = (num_clients * ops_per_client) + ((num_clients / 2) * 5000);

    cout << "Total Rows in Database: " << total_rows << " (Expected: " << expected_rows << ")\n";
    if (total_rows == expected_rows) {
        cout << "[PASS] Thread Safety Verified. No data lost during concurrent inserts.\n";
    } else {
        cout << "[FAIL] RACE CONDITION DETECTED! Data was overwritten.\n";
    }

    // Test 2: Is the data actually accurate, or is it corrupted garbage?
    long long balance_500_count = 0;
    flexql_exec(verify_db, "SELECT NAME FROM CONCURRENT_TEST WHERE BALANCE = 500;", consume_rows_callback, &balance_500_count, nullptr);
    
    long long expected_500s = expected_rows / 1000;
    cout << "Users with Balance 500: " << balance_500_count << " (Expected: " << expected_500s << ")\n";
    if (balance_500_count == expected_500s) {
        cout << "[PASS] Data Integrity Verified. Values are perfectly accurate.\n";
    } else {
        cout << "[FAIL] DATA CORRUPTION DETECTED! Indexes or Rows are mismatched.\n";
    }

    flexql_close(verify_db);
    return 0;
}
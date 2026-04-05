
//  This is just a custom testing file



// How to run it:

//     Compile it exactly like you compiled your normal client, just pointing to this new file. For example:
//     Bash

//     g++ -std=c++17 -Wall -O3 -I./include -o join_bench src/test_join.cpp src/flexql_api.cpp

//     Make sure your flexql_server is running in another terminal window.

//     Run the benchmark:
//     Bash

//     ./join_bench


#include <iostream>
#include <chrono>
#include <string>
#include <sstream>
#include "flexql.h" 

using namespace std;
using namespace std::chrono;

// A silent callback that only counts rows to test pure query speed 
// (printing to the terminal is very slow and ruins benchmarks)
int silent_count_callback(void *data, int argc, char **argv, char **azColName) {
    long long *count = static_cast<long long*>(data);
    (*count)++;
    return 0;
}

int main() {
    FlexQL *db = nullptr;
    
    // Connect to your server
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        cout << "Failed to connect to FlexQL server. Is it running?\n";
        return 1;
    }

    cout << "Connected to FlexQL. Setting up benchmark...\n";
    char *errMsg = nullptr;

    // 1. Create the Tables
    flexql_exec(db, "CREATE TABLE TEST_USERS(ID DECIMAL, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);", nullptr, nullptr, &errMsg);
    flexql_exec(db, "CREATE TABLE TEST_ORDERS(ORDER_ID DECIMAL, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);", nullptr, nullptr, &errMsg);

    // 2. Insert 20,000 rows into TEST_USERS in a single high-speed batch
    cout << "Inserting 20,000 rows into TEST_USERS...\n";
    stringstream users_ss;
    users_ss << "INSERT INTO TEST_USERS VALUES ";
    for (int i = 1; i <= 20000; i++) {
        users_ss << "(" << i << ", 'User" << i << "', 1000, 1893456000)";
        if (i < 20000) users_ss << ",";
    }
    users_ss << ";";
    flexql_exec(db, users_ss.str().c_str(), nullptr, nullptr, &errMsg);

    // 3. Insert 20,000 rows into TEST_ORDERS in a single high-speed batch
    cout << "Inserting 20,000 rows into TEST_ORDERS...\n";
    stringstream orders_ss;
    orders_ss << "INSERT INTO TEST_ORDERS VALUES ";
    for (int i = 1; i <= 20000; i++) {
        orders_ss << "(" << i << ", " << i << ", 500, 1893456000)"; // User ID matches Order ID for 1:1 join
        if (i < 20000) orders_ss << ",";
    }
    orders_ss << ";";
    flexql_exec(db, orders_ss.str().c_str(), nullptr, nullptr, &errMsg);

    // 4. Run the JOIN Benchmark
    cout << "\nRunning INNER JOIN Benchmark...\n";
    string join_query = "SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID;";

    long long row_count = 0;
    
    // Start the stopwatch
    auto start = high_resolution_clock::now();

    int rc = flexql_exec(db, join_query.c_str(), silent_count_callback, &row_count, &errMsg);

    // Stop the stopwatch
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();

    // 5. Print Results
    if (rc == FLEXQL_OK) {
        cout << "--------------------------------------\n";
        cout << "Benchmark Complete!\n";
        cout << "Rows Matched: " << row_count << "\n";
        cout << "Execution Time: " << elapsed << " ms\n";
        cout << "--------------------------------------\n";
    } else {
        cout << "Query Failed: " << (errMsg ? errMsg : "Unknown error") << "\n";
        if (errMsg) flexql_free(errMsg);
    }

    flexql_close(db);
    return 0;
}
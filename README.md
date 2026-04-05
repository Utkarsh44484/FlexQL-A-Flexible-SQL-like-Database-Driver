 
## Compilation and Execution Instructions

From the root directory of the project, build the entire suite:

### Build Commands

Clean previous builds and compile everything:

```bash
make clean
make
````
 

---

## 5.2 Execution

The server and client must be executed in separate terminal sessions to maintain the IPC socket connection.

### Steps

1. **Start the Server Daemon:**

   ```bash
   ./server
   ```

2. **Connect the Client REPL:**

   ```bash
   ./flexql-client
   ```

3. **Execute Automated Benchmarks:**

   ```bash
   ./benchmark 10000000
   ```

 
 

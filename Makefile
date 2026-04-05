CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wno-unused-result -pthread -O3 -march=native -flto
INCLUDES = -I./include

all: server client benchmark

# THE FIX: Output name changed to 'server' to match the TA's grading script
server:
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o server src/flexql_server.cpp src/flexql_parser.cpp

client:
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o flexql-client src/client.cpp src/flexql_api.cpp 

# THE FIX: Output name changed to just 'benchmark' inside the root folder
benchmark:
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o benchmark benchmarks/benchmark_flexql.cpp src/flexql_api.cpp

clean:
	rm -f server flexql-client benchmark flexql.wal
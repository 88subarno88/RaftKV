cmake_minimum_required(VERSION 3.20)
project(raftkv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(-Wall -Wextra -Wpedantic)

#  Core library: pure consensus, no networking dependency
add_library(raft_core
    src/raft/raft.cpp
    src/raft/election.cpp
    src/raft/replication.cpp
    src/raft/snapshot.cpp
    src/storage/wal.cpp
    src/storage/snapshot_store.cpp
)
target_include_directories(raft_core PUBLIC include)

# gRPC and Protobuf Setup 
# Prevent gRPC from building its own tests and examples to save time
set(RE2_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)

include(FetchContent)
FetchContent_Declare(gRPC GIT_REPOSITORY https://github.com/grpc/grpc GIT_TAG v1.62.0)
FetchContent_MakeAvailable(gRPC)

# Create the raft_proto target using CMake's built-in protobuf generator
add_library(raft_proto)
target_link_libraries(raft_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)
target_include_directories(raft_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

# Modern CMake command to generate C++ headers/sources from .proto files
# Requires CMake 3.20+ and FindProtobuf (which gRPC provides)
protobuf_generate(
    TARGET raft_proto
    LANGUAGE cpp
    IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/proto
    PROTOS proto/raft.proto
)

# Executables 
add_executable(raftkv-server src/server/main.cpp src/transport/grpc_transport.cpp)
# Use gRPC::grpc++ instead of grpc++
target_link_libraries(raftkv-server raft_core raft_proto gRPC::grpc++)

add_executable(kvcli client/kvcli.cpp)
target_link_libraries(kvcli raft_proto gRPC::grpc++)

# Tests 
FetchContent_Declare(googletest GIT_REPOSITORY https://github.com/google/googletest GIT_TAG v1.14.0)
FetchContent_MakeAvailable(googletest)
enable_testing()

add_executable(unit_tests
    tests/election_test.cpp 
    tests/replication_test.cpp
    tests/persistence_test.cpp 
    tests/linearizability_test.cpp
)

target_link_libraries(unit_tests raft_core GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(unit_tests)

#  Benchmarks 
add_executable(bench bench/throughput.cpp)
target_link_libraries(bench raft_proto gRPC::grpc++)
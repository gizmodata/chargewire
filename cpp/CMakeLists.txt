cmake_minimum_required(VERSION 3.28)
project(warpsql VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

# Boost 1.92: Beast (HTTP over Asio), JSON and URL. Only these are built.
set(BOOST_INCLUDE_LIBRARIES beast json url)
set(BOOST_ENABLE_CMAKE ON)
FetchContent_Declare(
  Boost
  URL https://github.com/boostorg/boost/releases/download/boost-1.92.0/boost-1.92.0-cmake.tar.xz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  EXCLUDE_FROM_ALL)
FetchContent_MakeAvailable(Boost)

# OpenSSL 3 for TLS (on macOS: OPENSSL_ROOT_DIR=$(brew --prefix openssl@3)).
find_package(OpenSSL 3 REQUIRED)

# Arrow C++ (IPC only) and DuckDB 2.0, built by scripts/build-arrow.sh and
# scripts/build-duckdb.sh at pinned versions. A project that includes this one can
# point at its own builds.
set(WARPSQL_ARROW_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/.deps/arrow-install"
    CACHE PATH "Arrow C++ install prefix")
set(WARPSQL_DUCKDB_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/.deps/duckdb-install"
    CACHE PATH "DuckDB install prefix (with duckdb_v2.h)")
find_package(Arrow CONFIG REQUIRED PATHS "${WARPSQL_ARROW_PREFIX}" NO_DEFAULT_PATH)
find_library(DUCKDB_LIBRARY duckdb PATHS "${WARPSQL_DUCKDB_PREFIX}/lib" NO_DEFAULT_PATH REQUIRED)

# The SDK: everything the protocol requires, independent of any engine.
add_library(warpsql
  src/sdk/api.cc
  src/sdk/auth.cc
  src/sdk/errors.cc
  src/sdk/http.cc
  src/sdk/ids.cc
  src/sdk/ingests.cc
  src/sdk/ipc.cc
  src/sdk/json.cc
  src/sdk/metadata.cc
  src/sdk/server.cc
  src/sdk/sessions.cc
  src/sdk/signal.cc
  src/sdk/statements.cc
  src/sdk/tls_stream.cc
  src/sdk/upload.cc)
target_include_directories(warpsql PUBLIC include PRIVATE src/sdk)
target_link_libraries(warpsql PUBLIC Boost::beast Boost::json Boost::url Arrow::arrow_static
                                     OpenSSL::SSL OpenSSL::Crypto)
target_compile_options(warpsql PRIVATE -Wall -Wextra)

# The client library: the protocol's client side (sign-in, sessions, statements on the
# streamed and status-document paths, prepared statements, ingest, metadata, cancel,
# TLS and mutual TLS). It needs Arrow, Boost and OpenSSL, not DuckDB.
add_library(warpsql_client
  src/client/client.cc
  src/client/transport.cc)
target_include_directories(warpsql_client PUBLIC include PRIVATE src/client)
target_link_libraries(warpsql_client PUBLIC Boost::beast Boost::json Boost::url Arrow::arrow_static
                                            OpenSSL::SSL OpenSSL::Crypto)
target_compile_options(warpsql_client PRIVATE -Wall -Wextra)

# The DuckDB backend: DuckDB 2.0 behind the SDK, through its v2 C API only. Servers
# built on it extend DuckDBBackend and DuckDBSession.
add_library(warpsql_duckdb
  src/duckdb/duckdb_backend.cc
  src/duckdb/duckdb_session.cc
  src/duckdb/duckdb_sql.cc
  src/duckdb/duckdb_upload.cc
  src/duckdb/duckdb_values.cc)
target_include_directories(warpsql_duckdb PUBLIC src/duckdb "${WARPSQL_DUCKDB_PREFIX}/include")
target_link_libraries(warpsql_duckdb PUBLIC warpsql "${DUCKDB_LIBRARY}")
target_compile_options(warpsql_duckdb PRIVATE -Wall -Wextra)

# The reference server.
add_executable(warp-duckdb src/duckdb/main.cc)
target_link_libraries(warp-duckdb PRIVATE warpsql_duckdb)
set_target_properties(warp-duckdb PROPERTIES BUILD_RPATH "${WARPSQL_DUCKDB_PREFIX}/lib")
target_compile_options(warp-duckdb PRIVATE -Wall -Wextra)

# The client library's tests (googletest 1.18.0): unit tests, and live tests against
# warp-duckdb (they start it themselves; WARPSQL_TEST_SERVER_BINARY names another).
option(WARPSQL_BUILD_TESTS "Build the client library's tests" OFF)
if(WARPSQL_BUILD_TESTS)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.18.0.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
  FetchContent_MakeAvailable(googletest)
  add_executable(warpsql-client-tests tests/client_test.cc)
  target_link_libraries(warpsql-client-tests PRIVATE warpsql_client GTest::gtest_main)
  target_compile_definitions(warpsql-client-tests PRIVATE
                             WARPSQL_TEST_DEFAULT_SERVER="$<TARGET_FILE:warp-duckdb>")
  add_dependencies(warpsql-client-tests warp-duckdb)
endif()

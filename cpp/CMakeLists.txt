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
# scripts/build-duckdb.sh at pinned versions.
set(WARPSQL_DEPS "${CMAKE_CURRENT_SOURCE_DIR}/.deps")
find_package(Arrow CONFIG REQUIRED PATHS "${WARPSQL_DEPS}/arrow-install" NO_DEFAULT_PATH)
find_library(DUCKDB_LIBRARY duckdb PATHS "${WARPSQL_DEPS}/duckdb-install/lib" NO_DEFAULT_PATH
             REQUIRED)

# The SDK: everything the protocol requires, independent of any engine.
add_library(warpsql
  src/sdk/api.cc
  src/sdk/auth.cc
  src/sdk/errors.cc
  src/sdk/http.cc
  src/sdk/ids.cc
  src/sdk/ipc.cc
  src/sdk/json.cc
  src/sdk/metadata.cc
  src/sdk/server.cc
  src/sdk/sessions.cc
  src/sdk/signal.cc
  src/sdk/statements.cc
  src/sdk/upload.cc)
target_include_directories(warpsql PUBLIC include PRIVATE src/sdk)
target_link_libraries(warpsql PUBLIC Boost::beast Boost::json Boost::url Arrow::arrow_static
                                     OpenSSL::SSL OpenSSL::Crypto)
target_compile_options(warpsql PRIVATE -Wall -Wextra)

# The reference server: the SDK in front of DuckDB 2.0, through its v2 C API only.
add_executable(warp-duckdb
  src/duckdb/duckdb_backend.cc
  src/duckdb/duckdb_session.cc
  src/duckdb/duckdb_sql.cc
  src/duckdb/duckdb_upload.cc
  src/duckdb/duckdb_values.cc
  src/duckdb/main.cc)
target_include_directories(warp-duckdb PRIVATE src/duckdb "${WARPSQL_DEPS}/duckdb-install/include")
target_link_libraries(warp-duckdb PRIVATE warpsql "${DUCKDB_LIBRARY}")
set_target_properties(warp-duckdb PROPERTIES BUILD_RPATH "${WARPSQL_DEPS}/duckdb-install/lib")
target_compile_options(warp-duckdb PRIVATE -Wall -Wextra)

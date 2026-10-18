cmake_minimum_required(VERSION 3.28)
project(warpsql VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

# MSVC: the static C runtime (/MT), as DuckDB always builds with it (a project that
# includes this one sets the same before it does).
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")

# Windows 10 and later; no min/max macros; Beast's and DuckDB's objects outgrow the
# default section count; sources are UTF-8. For each library, and so its users.
function(warpsql_windows_settings target)
  if(WIN32)
    target_compile_definitions(${target} PUBLIC _WIN32_WINNT=0x0A00 NOMINMAX WIN32_LEAN_AND_MEAN)
    target_compile_options(${target} PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/bigobj /utf-8 /Zc:__cplusplus>)
    target_link_libraries(${target} PUBLIC ws2_32 crypt32)
  endif()
endfunction()

# Warnings, as each compiler spells them.
if(MSVC)
  set(WARPSQL_WARNINGS /W3)
else()
  set(WARPSQL_WARNINGS -Wall -Wextra)
endif()

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
if(WIN32)
  # DuckDB's DLL exports its C API only, and an embedding server may use DuckDB's C++
  # internals too (Hardcharger's access control does): on Windows, DuckDB is linked
  # statically, with what its shared library links (its extensions, plus HTTP and
  # extension loading), registered before main() as DuckDB's own static programs do.
  # A DuckDB built with extensions that link other libraries (httpfs: libcurl; the
  # postgres scanner: libpq; ducklake: CRoaring) names their imported targets: find the
  # ones the toolchain provides (Hardcharger's CI: vcpkg's static triplet).
  foreach(package OpenSSL PostgreSQL CURL)
    find_package(${package} QUIET)
  endforeach()
  find_package(roaring CONFIG QUIET)
  find_package(DuckDB CONFIG REQUIRED PATHS "${WARPSQL_DUCKDB_PREFIX}" NO_DEFAULT_PATH)
  # Every extension the install has a static library of is one its shared library links
  # (DuckDB's four, and those an extension config links in); DuckDB_EXTENSIONS names only
  # the configs' ones when there are any.
  # Registered in DuckDB's own order, which is their load order: its four first, as
  # others need them loaded (ducklake reads with parquet, and would otherwise install it).
  file(GLOB _extension_libs "${WARPSQL_DUCKDB_PREFIX}/lib/*_extension.lib")
  set(_installed "")
  foreach(lib IN LISTS _extension_libs)
    get_filename_component(name "${lib}" NAME_WE)
    string(REGEX REPLACE "_extension$" "" name "${name}")
    list(APPEND _installed ${name})
  endforeach()
  set(WARPSQL_DUCKDB_LINKED httplib loadable_extensions)
  foreach(name IN ITEMS core_functions parquet json icu)
    if(name IN_LIST _installed)
      list(APPEND WARPSQL_DUCKDB_LINKED ${name})
      list(REMOVE_ITEM _installed ${name})
    endif()
  endforeach()
  list(APPEND WARPSQL_DUCKDB_LINKED ${_installed})
  if(NOT "parquet" IN_LIST WARPSQL_DUCKDB_LINKED)
    message(FATAL_ERROR "no DuckDB extension libraries in ${WARPSQL_DUCKDB_PREFIX}/lib")
  endif()
  message(STATUS "DuckDB, statically, with: ${WARPSQL_DUCKDB_LINKED}")
  set(DUCKDB_LIBRARY duckdb_static)
  set(DECLARATIONS "")
  set(REGISTRATIONS "")
  foreach(name IN LISTS WARPSQL_DUCKDB_LINKED)
    if(TARGET ${name}_extension)
      list(APPEND DUCKDB_LIBRARY ${name}_extension)
    elseif(TARGET duckdb_${name})
      list(APPEND DUCKDB_LIBRARY duckdb_${name})
    else()
      list(APPEND DUCKDB_LIBRARY "${WARPSQL_DUCKDB_PREFIX}/lib/${name}_extension.lib")
    endif()
    string(APPEND DECLARATIONS
           "int32_t duckdb_extension_${name}_describe(duckdb_extension_descriptor *descriptor);\n")
    string(APPEND REGISTRATIONS
           "    duckdb_register_static_extension(duckdb_extension_${name}_describe);\n")
  endforeach()
  set(ANCHOR warpsql_duckdb_static_extensions)
  configure_file(src/duckdb/static_extensions.cc.in
                 "${CMAKE_CURRENT_BINARY_DIR}/generated/static_extensions.cc" @ONLY)
else()
  find_library(DUCKDB_LIBRARY duckdb PATHS "${WARPSQL_DUCKDB_PREFIX}/lib" NO_DEFAULT_PATH REQUIRED)
endif()

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
  src/sdk/platform.cc
  src/sdk/security_events.cc
  src/sdk/server.cc
  src/sdk/sessions.cc
  src/sdk/signal.cc
  src/sdk/statements.cc
  src/sdk/stop_signals.cc
  src/sdk/tls_context.cc
  src/sdk/tls_stream.cc
  src/sdk/upload.cc)
target_include_directories(warpsql PUBLIC include PRIVATE src/sdk)
target_link_libraries(warpsql PUBLIC Boost::beast Boost::json Boost::url Arrow::arrow_static
                                     OpenSSL::SSL OpenSSL::Crypto)
target_compile_options(warpsql PRIVATE ${WARPSQL_WARNINGS})
warpsql_windows_settings(warpsql)

# The client library: the protocol's client side (sign-in, sessions, statements on the
# streamed and status-document paths, prepared statements, ingest, metadata, cancel,
# TLS and mutual TLS). It needs Arrow, Boost and OpenSSL, not DuckDB.
add_library(warpsql_client
  src/client/client.cc
  src/client/transport.cc)
target_include_directories(warpsql_client PUBLIC include PRIVATE src/client)
target_link_libraries(warpsql_client PUBLIC Boost::beast Boost::json Boost::url Arrow::arrow_static
                                            OpenSSL::SSL OpenSSL::Crypto)
target_compile_options(warpsql_client PRIVATE ${WARPSQL_WARNINGS})
warpsql_windows_settings(warpsql_client)

# The DuckDB backend: DuckDB 2.0 behind the SDK, through its v2 C API only. Servers
# built on it extend DuckDBBackend and DuckDBSession.
add_library(warpsql_duckdb
  src/duckdb/duckdb_backend.cc
  src/duckdb/duckdb_session.cc
  src/duckdb/duckdb_sql.cc
  src/duckdb/duckdb_upload.cc
  src/duckdb/duckdb_values.cc)
target_include_directories(warpsql_duckdb PUBLIC src/duckdb "${WARPSQL_DUCKDB_PREFIX}/include")
target_link_libraries(warpsql_duckdb PUBLIC warpsql ${DUCKDB_LIBRARY})
target_compile_options(warpsql_duckdb PRIVATE ${WARPSQL_WARNINGS})
if(WIN32)
  target_sources(warpsql_duckdb PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/generated/static_extensions.cc")
  target_compile_definitions(warpsql_duckdb PUBLIC DUCKDB_STATIC_BUILD)
  target_link_options(warpsql_duckdb INTERFACE "/INCLUDE:${ANCHOR}")
endif()

# The reference server.
add_executable(warp-duckdb src/duckdb/main.cc)
target_link_libraries(warp-duckdb PRIVATE warpsql_duckdb)
set_target_properties(warp-duckdb PROPERTIES BUILD_RPATH "${WARPSQL_DUCKDB_PREFIX}/lib")
target_compile_options(warp-duckdb PRIVATE ${WARPSQL_WARNINGS})

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

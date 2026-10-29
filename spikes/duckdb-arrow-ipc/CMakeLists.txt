cmake_minimum_required(VERSION 3.24)
project(chargewire_arrow_ipc_spike LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

# nanoarrow from main: writing compressed IPC (apache/arrow-nanoarrow#935) is not in a
# release yet. Move to the first release that has it.
set(NANOARROW_IPC ON CACHE BOOL "" FORCE)
set(NANOARROW_IPC_WITH_ZSTD ON CACHE BOOL "" FORCE)
set(NANOARROW_IPC_WITH_LZ4 ON CACHE BOOL "" FORCE)
# nanoarrow looks LZ4 up with pkg-config unless told where it is.
find_path(LZ4_INCLUDE_DIRS lz4frame.h REQUIRED)
find_library(LZ4_LIBRARY lz4 REQUIRED)
get_filename_component(LZ4_LIBRARY_DIRS "${LZ4_LIBRARY}" DIRECTORY)
set(LZ4_LIBRARIES lz4)
FetchContent_Declare(
  nanoarrow
  URL https://github.com/apache/arrow-nanoarrow/archive/e5f3d0a55d5576386359835f7bf052f983d1161f.tar.gz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_MakeAvailable(nanoarrow)

# libduckdb built by scripts/build-duckdb.sh (DuckDB 2.0, v2 C API only).
set(DUCKDB_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/.deps/duckdb-install")
find_library(DUCKDB_LIBRARY duckdb PATHS "${DUCKDB_PREFIX}/lib" NO_DEFAULT_PATH REQUIRED)

# Which library writes and reads Arrow IPC: Arrow C++ built by scripts/build-arrow.sh
# (the plan, IPC only, no Flight) or nanoarrow (for comparison). Both use nanoarrow's
# small C++ helpers for the Arrow C data interface.
set(CHARGEWIRE_IPC_LIBRARY "arrow" CACHE STRING "arrow (the plan) or nanoarrow")

if(CHARGEWIRE_IPC_LIBRARY STREQUAL "arrow")
  find_package(Arrow CONFIG REQUIRED PATHS "${CMAKE_CURRENT_SOURCE_DIR}/.deps/arrow-install"
               NO_DEFAULT_PATH)
  add_executable(chargewire-spike src/main.cc src/database.cc src/ipc_arrow.cc)
  target_link_libraries(chargewire-spike PRIVATE nanoarrow::nanoarrow Arrow::arrow_static)
else()
  add_executable(chargewire-spike src/main.cc src/database.cc src/ipc.cc)
  target_link_libraries(chargewire-spike PRIVATE nanoarrow::nanoarrow_ipc)
endif()
target_include_directories(chargewire-spike PRIVATE "${DUCKDB_PREFIX}/include")
target_link_libraries(chargewire-spike PRIVATE "${DUCKDB_LIBRARY}")
set_target_properties(chargewire-spike PROPERTIES BUILD_RPATH "${DUCKDB_PREFIX}/lib")
target_compile_options(chargewire-spike PRIVATE -Wall -Wextra)

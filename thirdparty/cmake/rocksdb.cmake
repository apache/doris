# rocksdb
set(WITH_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_CORE_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_ALL_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)
set(FAIL_ON_WARNINGS OFF CACHE BOOL "" FORCE)
set(PORTABLE ON CACHE BOOL "" FORCE)
set(WITH_SNAPPY ON CACHE BOOL "" FORCE)
set(WITH_LZ4 ON CACHE BOOL "" FORCE)
set(WITH_ZLIB ON CACHE BOOL "" FORCE)
set(WITH_ZSTD ON CACHE BOOL "" FORCE)
set(WITH_BZ2 OFF CACHE BOOL "" FORCE)
set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(USE_RTTI 1 CACHE BOOL "" FORCE)

# Patch rocksdb's bundled gtest CMakeLists.txt to avoid target conflict
# with our already-defined gtest target
set(_ROCKSDB_GTEST_CMAKE "${TP_SOURCE_DIR}/rocksdb-5.14.2/third-party/gtest-1.7.0/fused-src/gtest/CMakeLists.txt")
if(EXISTS "${_ROCKSDB_GTEST_CMAKE}")
    file(READ "${_ROCKSDB_GTEST_CMAKE}" _GTEST_CONTENT)
    if(_GTEST_CONTENT MATCHES "add_library\\(gtest")
        string(REPLACE "add_library(gtest" "add_library(rocksdb_gtest" _GTEST_CONTENT "${_GTEST_CONTENT}")
        file(WRITE "${_ROCKSDB_GTEST_CMAKE}" "${_GTEST_CONTENT}")
    endif()
endif()

add_subdirectory(${TP_SOURCE_DIR}/rocksdb-5.14.2 ${CMAKE_CURRENT_BINARY_DIR}/rocksdb EXCLUDE_FROM_ALL)
if(TARGET rocksdb)
    # Add include dirs as PRIVATE to avoid polluting global scope
    target_include_directories(rocksdb PRIVATE
        ${TP_SOURCE_DIR}/zstd-1.5.7/lib
        ${TP_SOURCE_DIR}/lz4-1.9.4/lib
    )
    # rocksdb 5.14.2 headers miss #include <cstdint>, add it globally (ref: build-thirdparty.sh)
    target_compile_options(rocksdb PRIVATE -include cstdint)
endif()

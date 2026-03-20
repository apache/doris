# snappy
set(SNAPPY_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SNAPPY_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(SNAPPY_INSTALL OFF CACHE BOOL "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/snappy-1.1.10 ${CMAKE_CURRENT_BINARY_DIR}/snappy EXCLUDE_FROM_ALL)
# Doris needs RTTI for SnappySlicesSource inheritance
if(TARGET snappy)
    target_compile_options(snappy PRIVATE -frtti)
endif()

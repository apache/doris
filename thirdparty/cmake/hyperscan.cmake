# hyperscan (vectorscan) - has CMake, build from source
# Requires Boost headers and ragel
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
set(BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_UNIT OFF CACHE BOOL "" FORCE)
set(FAT_RUNTIME OFF CACHE BOOL "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/hyperscan-5.4.2 ${CMAKE_CURRENT_BINARY_DIR}/hyperscan EXCLUDE_FROM_ALL)
if(TARGET hs)
    add_library(hyperscan ALIAS hs)
endif()

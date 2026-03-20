# libdeflate
set(LIBDEFLATE_BUILD_SHARED_LIB OFF CACHE BOOL "" FORCE)
set(LIBDEFLATE_BUILD_GZIP OFF CACHE BOOL "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/libdeflate-1.19 ${CMAKE_CURRENT_BINARY_DIR}/libdeflate EXCLUDE_FROM_ALL)
if(TARGET deflate_static)
    add_library(deflate ALIAS deflate_static)
endif()

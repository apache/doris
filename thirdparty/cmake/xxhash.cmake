# xxhash - tiny library, compile from source
add_library(xxhash STATIC ${TP_SOURCE_DIR}/xxHash-0.8.3/xxhash.c)
target_include_directories(xxhash PUBLIC ${TP_SOURCE_DIR}/xxHash-0.8.3)
set_target_properties(xxhash PROPERTIES POSITION_INDEPENDENT_CODE ON)

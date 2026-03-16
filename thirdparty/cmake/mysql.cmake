# mysql - build from source (only need client headers and a few source files)
set(MYSQL_SRC ${TP_SOURCE_DIR}/mysql-server-mysql-5.7.18)
set(MYSQL_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/mysql)

# mysql-server is massive, but Doris only needs the client library headers
# and sqltypes.h. Build the minimal set.
add_library(mysql_headers INTERFACE)
target_include_directories(mysql_headers INTERFACE ${MYSQL_SRC}/include)

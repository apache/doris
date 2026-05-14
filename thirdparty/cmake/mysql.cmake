# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# mysql - collect headers from source tree for client-side usage
# Only the public API headers are needed (mysql.h, errmsg.h, etc.)
set(MYSQL_SRC ${TP_SOURCE_DIR}/mysql-server-mysql-5.7.18)
set(MYSQL_NESTED_DIR ${CMAKE_CURRENT_BINARY_DIR}/mysql_headers/include/mysql)
file(MAKE_DIRECTORY ${MYSQL_NESTED_DIR})
file(MAKE_DIRECTORY ${MYSQL_NESTED_DIR}/mysql)

# Copy main include headers
file(GLOB _MYSQL_MAIN_HEADERS "${MYSQL_SRC}/include/*.h")
file(COPY ${_MYSQL_MAIN_HEADERS} DESTINATION ${MYSQL_NESTED_DIR})

# Copy binary_log_types.h from libbinlogevents
file(COPY "${MYSQL_SRC}/libbinlogevents/export/binary_log_types.h"
     DESTINATION ${MYSQL_NESTED_DIR})

# Copy entire mysql/ subdirectory tree (client_plugin.h, psi/*.h, etc.)
file(COPY "${MYSQL_SRC}/include/mysql/" DESTINATION ${MYSQL_NESTED_DIR}/mysql)

# Generate mysql_version.h from template
set(PROTOCOL_VERSION 10)
set(VERSION "5.7.18")
set(MYSQL_BASE_VERSION "5.7")
set(MYSQL_SERVER_SUFFIX "")
set(DOT_FRM_VERSION 6)
set(MYSQL_VERSION_ID 50718)
set(MYSQL_TCP_PORT 3306)
set(MYSQL_TCP_PORT_DEFAULT 0)
set(MYSQL_UNIX_ADDR "/tmp/mysql.sock")
set(COMPILATION_COMMENT "Doris MySQL Client")
set(SYS_SCHEMA_VERSION "1.5.1")
configure_file("${MYSQL_SRC}/include/mysql_version.h.in"
    "${MYSQL_NESTED_DIR}/mysql_version.h" @ONLY)

add_library(mysql_headers INTERFACE)
target_include_directories(mysql_headers SYSTEM INTERFACE
    ${CMAKE_CURRENT_BINARY_DIR}/mysql_headers/include)

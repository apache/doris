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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/exec")

set(EXEC_FILES
    arrow/arrow_reader.cpp
    arrow/orc_reader.cpp
    arrow/parquet_reader.cpp
    arrow/parquet_row_group_reader.cpp
    base_scanner.cpp
    data_sink.cpp
    decompressor.cpp
    exec_node.cpp
    hash_table.cpp
    text_converter.cpp
    olap_common.cpp
    tablet_info.cpp
    plain_binary_line_reader.cpp
    plain_text_line_reader.cpp
    es/es_predicate.cpp
    es/es_scan_reader.cpp
    es/es_scroll_query.cpp
    es/es_scroll_parser.cpp
    es/es_query_builder.cpp
    schema_scanner.cpp
    schema_scanner/schema_tables_scanner.cpp
    schema_scanner/schema_dummy_scanner.cpp
    schema_scanner/schema_schemata_scanner.cpp
    schema_scanner/schema_variables_scanner.cpp
    schema_scanner/schema_columns_scanner.cpp
    schema_scanner/schema_charsets_scanner.cpp
    schema_scanner/schema_collations_scanner.cpp
    schema_scanner/schema_helper.cpp
    schema_scanner/schema_views_scanner.cpp
    schema_scanner/schema_statistics_scanner.cpp
    schema_scanner/schema_table_privileges_scanner.cpp
    schema_scanner/schema_schema_privileges_scanner.cpp
    schema_scanner/schema_user_privileges_scanner.cpp
    schema_scanner/schema_files_scanner.cpp
    schema_scanner/schema_partitions_scanner.cpp
    schema_scanner/schema_rowsets_scanner.cpp
    schema_scanner/schema_backends_scanner.cpp
    scan_node.cpp
    odbc_connector.cpp
    table_connector.cpp
    schema_scanner.cpp
    parquet_scanner.cpp
)

if (WITH_LZO)
    set(EXEC_FILES ${EXEC_FILES}
        lzo_decompressor.cpp
    )
endif()

add_library(Exec STATIC
    ${EXEC_FILES}
)

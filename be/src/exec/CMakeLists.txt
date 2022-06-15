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
    analytic_eval_node.cpp
    blocking_join_node.cpp
    broker_scan_node.cpp
    broker_reader.cpp
    buffered_reader.cpp
    base_scanner.cpp
    broker_scanner.cpp
    cross_join_node.cpp
    csv_scan_node.cpp
    csv_scanner.cpp
    data_sink.cpp
    decompressor.cpp
    empty_set_node.cpp
    exec_node.cpp
    exchange_node.cpp
    hash_join_node.cpp
    hash_table.cpp
    local_file_reader.cpp
    merge_node.cpp
    scan_node.cpp
    select_node.cpp
    text_converter.cpp
    topn_node.cpp
    sort_exec_exprs.cpp
    olap_scan_node.cpp
    olap_scanner.cpp
    olap_common.cpp
    tablet_info.cpp
    tablet_sink.cpp
    plain_binary_line_reader.cpp
    plain_text_line_reader.cpp
    csv_scan_node.cpp
    csv_scanner.cpp
    table_function_node.cpp
    es_scan_node.cpp
    es_http_scan_node.cpp
    es_http_scanner.cpp
    es/es_predicate.cpp
    es/es_scan_reader.cpp
    es/es_scroll_query.cpp
    es/es_scroll_parser.cpp
    es/es_query_builder.cpp
    spill_sort_node.cc
    union_node.cpp
    set_operation_node.cpp
    intersect_node.cpp
    except_node.cpp
    repeat_node.cpp
    schema_scanner.cpp
    schema_scan_node.cpp
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

    partitioned_hash_table.cc
    partitioned_aggregation_node.cc
    odbc_scan_node.cpp
    local_file_writer.cpp
    broker_writer.cpp
    parquet_scanner.cpp
    parquet_reader.cpp
    parquet_writer.cpp
    orc_scanner.cpp
    odbc_connector.cpp
    json_scanner.cpp
    assert_num_rows_node.cpp
    s3_reader.cpp
    s3_writer.cpp
    hdfs_reader_writer.cpp
    hdfs_file_reader.cpp
    hdfs_writer.cpp
    hdfs_builder.cpp
)

if (WITH_MYSQL)
    set(EXEC_FILES
        ${EXEC_FILES}
        mysql_scan_node.cpp
        mysql_scanner.cpp
        )
endif()

if (WITH_LZO)
    set(EXEC_FILES ${EXEC_FILES}
        lzo_decompressor.cpp
    )
endif()

add_library(Exec STATIC
    ${EXEC_FILES}
)

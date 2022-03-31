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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/test/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test/exec")

# TODO: why is this test disabled?
#ADD_BE_TEST(new_olap_scan_node_test)
#ADD_BE_TEST(pre_aggregation_node_test)
ADD_BE_TEST(hash_table_test)
# ADD_BE_TEST(partitioned_hash_table_test)
#ADD_BE_TEST(olap_scanner_test)
#ADD_BE_TEST(olap_meta_reader_test)
ADD_BE_TEST(olap_common_test)
#ADD_BE_TEST(olap_scan_node_test)
#ADD_BE_TEST(mysql_scan_node_test)
#ADD_BE_TEST(mysql_scanner_test)
#ADD_BE_TEST(csv_scanner_test)
#ADD_BE_TEST(csv_scan_node_test)
# ADD_BE_TEST(csv_scan_bench_test)
ADD_BE_TEST(json_scanner_test)
ADD_BE_TEST(json_scanner_with_jsonpath_test)
ADD_BE_TEST(parquet_scanner_test)
ADD_BE_TEST(orc_scanner_test)
ADD_BE_TEST(plain_text_line_reader_uncompressed_test)
ADD_BE_TEST(plain_text_line_reader_gzip_test)
ADD_BE_TEST(plain_text_line_reader_bzip_test)
ADD_BE_TEST(plain_text_line_reader_lz4frame_test)
if(DEFINED DORIS_WITH_LZO)
ADD_BE_TEST(plain_text_line_reader_lzop_test)
endif()
# ADD_BE_TEST(broker_reader_test)
ADD_BE_TEST(broker_scanner_test)
ADD_BE_TEST(broker_scan_node_test)
ADD_BE_TEST(tablet_info_test)
ADD_BE_TEST(tablet_sink_test)
ADD_BE_TEST(buffered_reader_test)
# ADD_BE_TEST(es_scan_node_test)
ADD_BE_TEST(es_http_scan_node_test)
ADD_BE_TEST(es_predicate_test)
ADD_BE_TEST(es_query_builder_test)
ADD_BE_TEST(es_scan_reader_test)
#ADD_BE_TEST(schema_scan_node_test)
#ADD_BE_TEST(unix_odbc_test)
#ADD_BE_TEST(schema_scanner_test)
##ADD_BE_TEST(set_executor_test)
#ADD_BE_TEST(schema_scanner/schema_authors_scanner_test)
#ADD_BE_TEST(schema_scanner/schema_columns_scanner_test)
#ADD_BE_TEST(schema_scanner/schema_create_table_scanner_test)
#ADD_BE_TEST(schema_scanner/schema_open_tables_scanner_test)
#ADD_BE_TEST(schema_scanner/schema_schemata_scanner_test)
#ADD_BE_TEST(schema_scanner/schema_table_names_scanner_test)
#ADD_BE_TEST(schema_scanner/schema_tables_scanner_test)
#ADD_BE_TEST(schema_scanner/schema_variables_scanner_test)
#ADD_BE_TEST(schema_scanner/schema_engines_scanner_test)
#ADD_BE_TEST(schema_scanner/schema_collations_scanner_test)
#ADD_BE_TEST(schema_scanner/schema_charsets_scanner_test)
ADD_BE_TEST(s3_reader_test)
ADD_BE_TEST(multi_bytes_separator_test)

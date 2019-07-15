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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/test/olap")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test/olap")

ADD_BE_TEST(row_block_test)
ADD_BE_TEST(bit_field_test)
ADD_BE_TEST(byte_buffer_test)
ADD_BE_TEST(run_length_byte_test)
ADD_BE_TEST(run_length_integer_test)
ADD_BE_TEST(stream_index_test)
ADD_BE_TEST(lru_cache_test)
ADD_BE_TEST(bloom_filter_test)
ADD_BE_TEST(bloom_filter_index_test)
ADD_BE_TEST(comparison_predicate_test)
ADD_BE_TEST(in_list_predicate_test)
ADD_BE_TEST(null_predicate_test)
ADD_BE_TEST(file_helper_test)
ADD_BE_TEST(file_utils_test)
ADD_BE_TEST(delete_handler_test)
ADD_BE_TEST(column_reader_test)
ADD_BE_TEST(row_cursor_test)
ADD_BE_TEST(skiplist_test)
ADD_BE_TEST(delta_writer_test)
ADD_BE_TEST(serialize_test)
ADD_BE_TEST(olap_meta_test)
ADD_BE_TEST(field_info_test)
ADD_BE_TEST(rowset/segment_v2/bitshuffle_page_test)
ADD_BE_TEST(rowset/segment_v2/plain_page_test)
ADD_BE_TEST(rowset/segment_v2/binary_plain_page_test)
ADD_BE_TEST(rowset/segment_v2/column_reader_writer_test)
ADD_BE_TEST(rowset/segment_v2/encoding_info_test)
ADD_BE_TEST(rowset/segment_v2/ordinal_page_index_test)
ADD_BE_TEST(rowset/segment_v2/rle_page_test)
ADD_BE_TEST(tablet_meta_manager_test)
ADD_BE_TEST(tablet_mgr_test)
ADD_BE_TEST(rowset/rowset_meta_manager_test)
ADD_BE_TEST(rowset/rowset_meta_test)
ADD_BE_TEST(rowset/alpha_rowset_test)
ADD_BE_TEST(olap_snapshot_converter_test)
ADD_BE_TEST(txn_manager_test)

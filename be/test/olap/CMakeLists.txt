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

ADD_BE_TEST(timestamped_version_tracker_test)
ADD_BE_TEST(row_block_test)
ADD_BE_TEST(row_block_v2_test)
ADD_BE_TEST(bit_field_test)
ADD_BE_TEST(byte_buffer_test)
ADD_BE_TEST(engine_storage_migration_task_test)
ADD_BE_TEST(run_length_byte_test)
ADD_BE_TEST(run_length_integer_test)
ADD_BE_TEST(stream_index_test)
ADD_BE_TEST(lru_cache_test)
ADD_BE_TEST(bloom_filter_test)
ADD_BE_TEST(bloom_filter_column_predicate_test)
ADD_BE_TEST(bloom_filter_index_test)
ADD_BE_TEST(comparison_predicate_test)
ADD_BE_TEST(in_list_predicate_test)
ADD_BE_TEST(null_predicate_test)
ADD_BE_TEST(file_helper_test)
ADD_BE_TEST(file_utils_test)
ADD_BE_TEST(delete_handler_test)
ADD_BE_TEST(column_reader_test)
ADD_BE_TEST(cumulative_compaction_policy_test)
ADD_BE_TEST(schema_change_test)
ADD_BE_TEST(row_cursor_test)
ADD_BE_TEST(skiplist_test)
ADD_BE_TEST(delta_writer_test)
ADD_BE_TEST(serialize_test)
ADD_BE_TEST(olap_meta_test)
ADD_BE_TEST(decimal12_test)
ADD_BE_TEST(column_vector_test)
ADD_BE_TEST(storage_types_test)
ADD_BE_TEST(aggregate_func_test)
ADD_BE_TEST(rowset/segment_v2/bitshuffle_page_test)
ADD_BE_TEST(rowset/segment_v2/plain_page_test)
ADD_BE_TEST(rowset/segment_v2/binary_plain_page_test)
ADD_BE_TEST(rowset/segment_v2/binary_prefix_page_test)
ADD_BE_TEST(rowset/segment_v2/bitmap_index_test)
ADD_BE_TEST(rowset/segment_v2/column_reader_writer_test)
ADD_BE_TEST(rowset/segment_v2/encoding_info_test)
ADD_BE_TEST(rowset/segment_v2/ordinal_page_index_test)
ADD_BE_TEST(rowset/segment_v2/rle_page_test)
ADD_BE_TEST(rowset/segment_v2/binary_dict_page_test)
ADD_BE_TEST(rowset/segment_v2/segment_test)
ADD_BE_TEST(rowset/segment_v2/row_ranges_test)
ADD_BE_TEST(rowset/segment_v2/frame_of_reference_page_test)
ADD_BE_TEST(rowset/segment_v2/block_bloom_filter_test)
ADD_BE_TEST(rowset/segment_v2/bloom_filter_index_reader_writer_test)
ADD_BE_TEST(rowset/segment_v2/zone_map_index_test)
ADD_BE_TEST(tablet_meta_test)
ADD_BE_TEST(tablet_meta_manager_test)
ADD_BE_TEST(tablet_mgr_test)
ADD_BE_TEST(tablet_test)
ADD_BE_TEST(rowset/rowset_meta_manager_test)
ADD_BE_TEST(rowset/rowset_meta_test)
ADD_BE_TEST(rowset/alpha_rowset_test)
ADD_BE_TEST(rowset/beta_rowset_test)
ADD_BE_TEST(rowset/unique_rowset_id_generator_test)
ADD_BE_TEST(rowset/rowset_converter_test)
ADD_BE_TEST(txn_manager_test)
ADD_BE_TEST(generic_iterators_test)
ADD_BE_TEST(key_coder_test)
ADD_BE_TEST(short_key_index_test)
ADD_BE_TEST(page_cache_test)
ADD_BE_TEST(hll_test)
# ADD_BE_TEST(memtable_flush_executor_test)
ADD_BE_TEST(selection_vector_test)
ADD_BE_TEST(block_column_predicate_test)
ADD_BE_TEST(options_test)
ADD_BE_TEST(fs/file_block_manager_test)
#ADD_BE_TEST(push_handler_test)
ADD_BE_TEST(common_test)

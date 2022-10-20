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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/olap")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/olap")

add_subdirectory(rowset)

add_library(Olap STATIC
    aggregate_func.cpp
    base_compaction.cpp
    base_tablet.cpp
    bloom_filter.hpp
    block_column_predicate.cpp
    byte_buffer.cpp
    collect_iterator.cpp
    compaction.cpp
    compaction_permit_limiter.cpp
    compress.cpp
    cumulative_compaction.cpp
    cumulative_compaction_policy.cpp
    delete_handler.cpp
    delta_writer.cpp
    file_helper.cpp
    generic_iterators.cpp
    hll.cpp
    bloom_filter_predicate.cpp
    like_column_predicate.cpp
    key_coder.cpp
    lru_cache.cpp
    memtable.cpp
    memtable_flush_executor.cpp
    merger.cpp
    null_predicate.cpp
    olap_meta.cpp
    olap_server.cpp
    options.cpp
    page_cache.cpp
    push_handler.cpp
    reader.cpp
    tuple_reader.cpp
    row_block.cpp
    row_block2.cpp
    row_cursor.cpp
    version_graph.cpp
    schema.cpp
    schema_change.cpp
    storage_engine.cpp
    data_dir.cpp
    short_key_index.cpp
    snapshot_manager.cpp
    stream_index_common.cpp
    stream_name.cpp
    tablet.cpp
    tablet_manager.cpp
    tablet_meta.cpp
    tablet_meta_manager.cpp
    tablet_schema.cpp
    txn_manager.cpp
    types.cpp 
    utils.cpp
    wrapper_field.cpp
    primary_key_index.cpp
    rowset/segment_v2/bitmap_index_reader.cpp
    rowset/segment_v2/bitmap_index_writer.cpp
    rowset/segment_v2/bitshuffle_page.cpp
    rowset/segment_v2/bitshuffle_wrapper.cpp
    rowset/segment_v2/column_reader.cpp
    rowset/segment_v2/column_writer.cpp
    rowset/segment_v2/encoding_info.cpp
    rowset/segment_v2/index_page.cpp
    rowset/segment_v2/indexed_column_reader.cpp
    rowset/segment_v2/indexed_column_writer.cpp
    rowset/segment_v2/ordinal_page_index.cpp
    rowset/segment_v2/page_io.cpp
    rowset/segment_v2/binary_dict_page.cpp
    rowset/segment_v2/binary_prefix_page.cpp
    rowset/segment_v2/segment.cpp
    rowset/segment_v2/segment_iterator.cpp
    rowset/segment_v2/empty_segment_iterator.cpp
    rowset/segment_v2/segment_writer.cpp
    rowset/segment_v2/block_split_bloom_filter.cpp
    rowset/segment_v2/bloom_filter_index_reader.cpp
    rowset/segment_v2/bloom_filter_index_writer.cpp
    rowset/segment_v2/bloom_filter.cpp
    rowset/segment_v2/zone_map_index.cpp
    task/engine_batch_load_task.cpp
    task/engine_checksum_task.cpp
    task/engine_clone_task.cpp
    task/engine_storage_migration_task.cpp
    task/engine_publish_version_task.cpp
    task/engine_alter_tablet_task.cpp
    column_vector.cpp
    segment_loader.cpp
    storage_policy_mgr.cpp
)

if (NOT USE_MEM_TRACKER)
    target_compile_options(Olap PRIVATE -Wno-unused-lambda-capture)
endif()

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

add_library(Olap STATIC
    aggregate_func.cpp
    base_compaction.cpp
    bit_field_reader.cpp
    bit_field_writer.cpp
    bloom_filter.hpp
    bloom_filter_reader.cpp
    bloom_filter_writer.cpp
    byte_buffer.cpp
    column_data.cpp
    column_reader.cpp
    column_writer.cpp
    comparison_predicate.cpp
    compress.cpp
    cumulative_compaction.cpp
    data_writer.cpp
    delete_handler.cpp
    delta_writer.cpp
    field.cpp
    field_info.cpp
    file_helper.cpp
    file_stream.cpp
    hll.cpp
    in_list_predicate.cpp
    in_stream.cpp
    lru_cache.cpp
    memtable.cpp
    merger.cpp
    new_status.cpp
    null_predicate.cpp
    olap_cond.cpp
    olap_engine.cpp
    olap_header.cpp
    olap_header_manager.cpp
    olap_index.cpp
    olap_meta.cpp
    olap_server.cpp
    olap_snapshot.cpp
    olap_table.cpp
    options.cpp
    out_stream.cpp
    push_handler.cpp
    reader.cpp
    row_block.cpp
    row_cursor.cpp
    segment_group.cpp
    run_length_byte_reader.cpp
    run_length_byte_writer.cpp
    run_length_integer_reader.cpp
    run_length_integer_writer.cpp
    schema_change.cpp
    segment_reader.cpp
    segment_writer.cpp
    serialize.cpp
    store.cpp
    stream_index_common.cpp
    stream_index_reader.cpp
    stream_index_writer.cpp
    stream_name.cpp
    types.cpp 
    utils.cpp
    wrapper_field.cpp
    rowset/segment_v2/bitshuffle_wrapper.cpp
    rowset/segment_v2/column_reader.cpp
    rowset/segment_v2/column_writer.cpp
    rowset/segment_v2/encoding_info.cpp
    rowset/segment_v2/ordinal_page_index.cpp
)

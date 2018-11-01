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
    comparison_predicate.cpp
    cumulative_compaction.cpp
    delta_writer.cpp
    delete_handler.cpp
    field.cpp
    field_info.cpp
    file_helper.cpp
    hll.cpp
    i_data.cpp
    in_list_predicate.cpp
    lru_cache.cpp
    memtable.cpp
    merger.cpp
    new_status.cpp
    null_predicate.cpp
    olap_cond.cpp
    olap_data.cpp
    olap_engine.cpp
    olap_header.cpp
    olap_index.cpp
    olap_server.cpp
    olap_snapshot.cpp
    options.cpp
    store.cpp
    olap_table.cpp
    push_handler.cpp
    reader.cpp
    row_block.cpp
    row_cursor.cpp
    rowset.cpp
    schema_change.cpp
    types.cpp 
    utils.cpp
    wrapper_field.cpp
    writer.cpp
    olap_header_manager.cpp
    olap_meta.cpp
    column_file/bit_field_reader.cpp
    column_file/bit_field_writer.cpp
    column_file/bloom_filter.hpp
    column_file/bloom_filter_reader.cpp
    column_file/bloom_filter_writer.cpp
    column_file/byte_buffer.cpp
    column_file/column_data.cpp
    column_file/column_reader.cpp
    column_file/column_writer.cpp
    column_file/compress.cpp
    column_file/data_writer.cpp
    column_file/file_stream.cpp
    column_file/in_stream.cpp
    column_file/out_stream.cpp
    column_file/run_length_byte_reader.cpp
    column_file/run_length_byte_writer.cpp
    column_file/run_length_integer_reader.cpp
    column_file/run_length_integer_writer.cpp
    column_file/segment_reader.cpp
    column_file/segment_writer.cpp
    column_file/serialize.cpp
    column_file/stream_index_common.cpp
    column_file/stream_index_reader.cpp
    column_file/stream_index_writer.cpp
    column_file/stream_name.cpp
)


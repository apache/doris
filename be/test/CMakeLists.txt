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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/test")

# where to put generated libraries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test")

file(GLOB_RECURSE UT_FILES CONFIGURE_DEPENDS *.cpp)

# Remove all cpp files from vector_search subdirectory
# Since cpp files in vector search subdirs use header files from faiss.
# The compile check used by doris can not be applied to faiss headers.
# So vector_search cpp files are compiled in a separate library using different compile options.
file(GLOB_RECURSE VECTOR_FILES CONFIGURE_DEPENDS olap/vector_search/*.cpp)
list(REMOVE_ITEM UT_FILES ${VECTOR_FILES})

if(NOT DEFINED DORIS_WITH_LZO)
    list(REMOVE_ITEM UT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/exec/plain_text_line_reader_lzop_test.cpp)
endif()

if (OS_MACOSX)
    list(REMOVE_ITEM UT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/util/system_metrics_test.cpp)
endif()

list(REMOVE_ITEM UT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/tools/benchmark_tool.cpp)

# todo: need fix those ut
list(REMOVE_ITEM UT_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/agent/heartbeat_server_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common/config_validator_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/http/metrics_action_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/binary_plain_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/binary_prefix_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/bitshuffle_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/column_reader_writer_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/frame_of_reference_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/plain_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/rle_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/runtime/decimal_value_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/util/decompress_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/util/url_coding_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/io/fs/remote_file_system_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/remote_rowset_gc_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/runtime/jsonb_value_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/runtime/large_int_value_test.cpp
)

set(RUNTIME_TEST_FILES
    # runtime/buffered_tuple_stream_test.cpp
    # runtime/sorter_test.cpp
    # runtime/buffer_control_block_test.cpp
    # runtime/result_buffer_mgr_test.cpp
    # runtime/result_sink_test.cpp
    # runtime/data_stream_test.cpp
    # runtime/parallel_executor_test.cpp
    # runtime/datetime_value_test.cpp
    # runtime/dpp_sink_internal_test.cpp
    # runtime/dpp_sink_test.cpp
    # runtime/data_spliter_test.cpp
    # runtime/tmp_file_mgr_test.cpp
    # runtime/disk_io_mgr_test.cpp
    # runtime/thread_resource_mgr_test.cpp
    # runtime/qsorter_test.cpp
    # runtime/buffered_block_mgr2_test.cpp
    # runtime/buffered_tuple_stream2_test.cpp
    # runtime/export_task_mgr_test.cpp
    runtime/mem_pool_test.cpp
    runtime/string_buffer_test.cpp
    runtime/decimalv2_value_test.cpp
    runtime/large_int_value_test.cpp
    runtime/string_value_test.cpp
    runtime/fragment_mgr_test.cpp
    runtime/mem_limit_test.cpp
    runtime/stream_load_pipe_test.cpp
    # TODO this test will override DeltaWriter, will make other test failed
    # runtime/load_channel_mgr_test.cpp
    runtime/snapshot_loader_test.cpp
    runtime/user_function_cache_test.cpp
    runtime/kafka_consumer_pipe_test.cpp
    runtime/routine_load_task_executor_test.cpp
    runtime/small_file_mgr_test.cpp
    runtime/heartbeat_flags_test.cpp
    runtime/result_queue_mgr_test.cpp
    runtime/memory_scratch_sink_test.cpp
    runtime/test_env.cc
    runtime/external_scan_context_mgr_test.cpp
    runtime/memory/chunk_allocator_test.cpp
    runtime/memory/system_allocator_test.cpp
    runtime/cache/partition_cache_test.cpp
    runtime/collection_value_test.cpp
    #runtime/array_test.cpp
)
set(TESTUTIL_TEST_FILES
    testutil/test_util.cpp
    testutil/array_utils.cpp
    testutil/desc_tbl_builder.cc
    testutil/function_utils.cpp
    testutil/run_all_tests.cpp
)
set(UDF_TEST_FILES
    # udf/udf_test.cpp
    # udf/uda_test.cpp
)
set(UTIL_TEST_FILES
    util/bit_util_test.cpp
    util/brpc_client_cache_test.cpp
    util/path_trie_test.cpp
    util/coding_test.cpp
    util/crc32c_test.cpp
    util/lru_cache_util_test.cpp
    util/filesystem_util_test.cpp
    util/internal_queue_test.cpp
    util/cidr_test.cpp
    util/metrics_test.cpp
    util/doris_metrics_test.cpp
    util/system_metrics_test.cpp
    util/string_util_test.cpp
    util/string_parser_test.cpp
    util/core_local_test.cpp
    util/byte_buffer2_test.cpp
    util/uid_util_test.cpp
    util/encryption_util_test.cpp
    util/md5_test.cpp
    util/sm3_test.cpp
    util/bitmap_test.cpp
    util/bitmap_value_test.cpp
    util/faststring_test.cpp
    util/rle_encoding_test.cpp
    util/tdigest_test.cpp
    util/block_compression_test.cpp
    util/arrow/arrow_row_block_test.cpp
    util/arrow/arrow_row_batch_test.cpp
    util/arrow/arrow_work_flow_test.cpp
    util/counter_cond_variable_test.cpp
    util/frame_of_reference_coding_test.cpp
    util/bit_stream_utils_test.cpp
    util/radix_sort_test.cpp
    util/zip_util_test.cpp
    util/utf8_check_test.cpp
    util/cgroup_util_test.cpp
    util/path_util_test.cpp
    util/file_cache_test.cpp
    util/parse_util_test.cpp
    util/countdown_latch_test.cpp
    util/scoped_cleanup_test.cpp
    util/thread_test.cpp
    util/threadpool_test.cpp
    util/mysql_row_buffer_test.cpp
    util/trace_test.cpp
    util/easy_json-test.cpp
    util/http_channel_test.cpp
    util/histogram_test.cpp
    util/s3_uri_test.cpp
    util/s3_storage_backend_test.cpp
    util/broker_storage_backend_test.cpp
    util/sort_heap_test.cpp
    util/counts_test.cpp
    util/date_func_test.cpp
    util/tuple_row_zorder_compare_test.cpp
    util/array_parser_test.cpp
    util/quantile_state_test.cpp
    util/hdfs_storage_backend_test.cpp
    util/interval_tree_test.cpp
)
set(VEC_TEST_FILES
    vec/aggregate_functions/agg_collect_test.cpp
    vec/aggregate_functions/agg_test.cpp
    vec/aggregate_functions/agg_min_max_test.cpp
    vec/aggregate_functions/vec_window_funnel_test.cpp
    vec/aggregate_functions/agg_min_max_by_test.cpp
    vec/core/block_test.cpp
    vec/core/column_array_test.cpp
    vec/core/column_complex_test.cpp
    vec/core/column_nullable_test.cpp
    vec/exec/vgeneric_iterators_test.cpp
    vec/exec/vbroker_scan_node_test.cpp
    vec/exec/vbroker_scanner_test.cpp
    vec/exec/vjson_scanner_test.cpp
    vec/exec/vtablet_sink_test.cpp
    vec/exec/vorc_scanner_test.cpp
    vec/exec/vparquet_scanner_test.cpp
    vec/exprs/vexpr_test.cpp
    vec/function/function_array_aggregation_test.cpp
    vec/function/function_array_element_test.cpp
    vec/function/function_array_index_test.cpp
    vec/function/function_array_size_test.cpp
    vec/function/function_arrays_overlap_test.cpp
    vec/function/function_bitmap_test.cpp
    vec/function/function_comparison_test.cpp
    vec/function/function_hash_test.cpp
    vec/function/function_math_test.cpp
    vec/function/function_string_test.cpp
    vec/function/function_time_test.cpp
    vec/function/function_ifnull_test.cpp
    vec/function/function_nullif_test.cpp
    vec/function/function_like_test.cpp
    vec/function/function_arithmetic_test.cpp
    vec/function/function_json_test.cpp
    vec/function/function_geo_test.cpp
    vec/function/function_test_util.cpp
    vec/function/table_function_test.cpp
    vec/runtime/vdata_stream_test.cpp
    vec/runtime/vdatetime_value_test.cpp
    vec/runtime/agg_state_csv_test.cpp
    vec/utils/arrow_column_to_doris_column_test.cpp
    vec/olap/char_type_padding_test.cpp
)

message(STATUS "Disable the metrics collection for orc")
add_compile_definitions(ENABLE_METRICS=0)

list(APPEND UT_FILES
    ${CONTRIB_PATH}/apache-orc/c++/test/MemoryInputStream.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/MemoryOutputStream.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestAttributes.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestBlockBuffer.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestBufferedOutputStream.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestBloomFilter.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestByteRle.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestByteRLEEncoder.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestColumnPrinter.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestColumnReader.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestColumnStatistics.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestCompression.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestDecompression.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestDecimal.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestDictionaryEncoding.cc
    #${CONTRIB_PATH}/apache-orc/c++/test/TestDriver.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestInt128.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestMurmur3.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestPredicateLeaf.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestPredicatePushdown.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestReader.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestRleDecoder.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestRleEncoder.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestRLEV2Util.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestSargsApplier.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestSearchArgument.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestStripeIndexStatistics.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestTimestampStatistics.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestTimezone.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestType.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestWriter.cc
)

include_directories(
  ${CMAKE_SOURCE_DIR}/../contrib/apache-orc/c++/src
  ${CMAKE_BINARY_DIR}/apache-orc/c++/src
  ${CMAKE_BINARY_DIR}/apache-orc/c++/include
)

# Removed test files are added back using separate compile arguments.
add_subdirectory(olap/vector_search)

add_executable(doris_be_test ${UT_FILES})

target_link_libraries(doris_be_test ${TEST_LINK_LIBS}
    -Wl,--whole-archive vector_search_test -Wl,--no-whole-archive)
set_target_properties(doris_be_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
target_compile_options(doris_be_test PRIVATE -include gtest/gtest.h -Wno-shadow -Wno-shadow-field)

if (OS_MACOSX AND ARCH_ARM)
    find_program(DSYMUTIL NAMES dsymutil)
    message(STATUS "dsymutil found: ${DSYMUTIL}")
    find_program(LLVM_STRIP NAMES llvm-strip)
    message(STATUS "llvm-strip found: ${LLVM_STRIP}")
    add_custom_command(TARGET doris_be_test POST_BUILD
        COMMAND ${DSYMUTIL} $<TARGET_FILE:doris_be_test>
        COMMAND ${LLVM_STRIP} --strip-all $<TARGET_FILE:doris_be_test>
    )
endif()

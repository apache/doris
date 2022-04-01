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
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test/util")

ADD_BE_TEST(bit_util_test)
ADD_BE_TEST(brpc_client_cache_test)
ADD_BE_TEST(path_trie_test)
ADD_BE_TEST(coding_test)
ADD_BE_TEST(crc32c_test)
ADD_BE_TEST(lru_cache_util_test)
ADD_BE_TEST(filesystem_util_test)
ADD_BE_TEST(internal_queue_test)
ADD_BE_TEST(cidr_test)
ADD_BE_TEST(new_metrics_test)
ADD_BE_TEST(doris_metrics_test)
ADD_BE_TEST(system_metrics_test)
ADD_BE_TEST(string_util_test)
ADD_BE_TEST(string_parser_test)
ADD_BE_TEST(core_local_test)
ADD_BE_TEST(json_util_test)
ADD_BE_TEST(byte_buffer2_test)
ADD_BE_TEST(uid_util_test)
ADD_BE_TEST(encryption_util_test)
ADD_BE_TEST(md5_test)
ADD_BE_TEST(sm3_test)
ADD_BE_TEST(bitmap_test)
ADD_BE_TEST(bitmap_value_test)
ADD_BE_TEST(faststring_test)
ADD_BE_TEST(rle_encoding_test)
ADD_BE_TEST(tdigest_test)
ADD_BE_TEST(block_compression_test)
ADD_BE_TEST(arrow/arrow_row_block_test)
ADD_BE_TEST(arrow/arrow_row_batch_test)
ADD_BE_TEST(arrow/arrow_work_flow_test)
ADD_BE_TEST(counter_cond_variable_test)
ADD_BE_TEST(frame_of_reference_coding_test)
ADD_BE_TEST(bit_stream_utils_test)
ADD_BE_TEST(radix_sort_test)
ADD_BE_TEST(zip_util_test)
ADD_BE_TEST(utf8_check_test)
ADD_BE_TEST(cgroup_util_test)
ADD_BE_TEST(path_util_test)
ADD_BE_TEST(file_cache_test)
ADD_BE_TEST(parse_util_test)
ADD_BE_TEST(countdown_latch_test)
ADD_BE_TEST(monotime_test)
ADD_BE_TEST(scoped_cleanup_test)
ADD_BE_TEST(thread_test)
ADD_BE_TEST(threadpool_test)
ADD_BE_TEST(mysql_row_buffer_test)
ADD_BE_TEST(trace_test)
ADD_BE_TEST(easy_json-test)
ADD_BE_TEST(http_channel_test)
ADD_BE_TEST(histogram_test)
ADD_BE_TEST(s3_uri_test)
ADD_BE_TEST(s3_storage_backend_test)
ADD_BE_TEST(broker_storage_backend_test)
ADD_BE_TEST(sort_heap_test)
ADD_BE_TEST(counts_test)
ADD_BE_TEST(date_func_test)
ADD_BE_TEST(tuple_row_zorder_compare_test)

target_link_libraries(Test_util Common Util Gutil ${Boost_LIBRARIES} glog gflags fmt protobuf)

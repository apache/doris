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

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test/runtime")

#ADD_BE_TEST(buffered_tuple_stream_test)
#ADD_BE_TEST(sorter_test)
#ADD_BE_TEST(buffer_control_block_test)
#ADD_BE_TEST(result_buffer_mgr_test)
#ADD_BE_TEST(result_sink_test)
ADD_BE_TEST(mem_pool_test)
ADD_BE_TEST(free_list_test)
ADD_BE_TEST(string_buffer_test)
#ADD_BE_TEST(data_stream_test)
#ADD_BE_TEST(parallel_executor_test)
#ADD_BE_TEST(datetime_value_test)
ADD_BE_TEST(decimalv2_value_test)
ADD_BE_TEST(large_int_value_test)
ADD_BE_TEST(string_value_test)
#ADD_BE_TEST(thread_resource_mgr_test)
#ADD_BE_TEST(qsorter_test)
ADD_BE_TEST(fragment_mgr_test)
#ADD_BE_TEST(dpp_sink_internal_test)
#ADD_BE_TEST(dpp_sink_test)
#ADD_BE_TEST(data_spliter_test)
#ADD_BE_TEST(etl_job_mgr_test)

#ADD_BE_TEST(tmp_file_mgr_test)
#ADD_BE_TEST(disk_io_mgr_test)
ADD_BE_TEST(mem_limit_test)
#ADD_BE_TEST(buffered_block_mgr2_test)
#ADD_BE_TEST(buffered_tuple_stream2_test)
ADD_BE_TEST(stream_load_pipe_test)
ADD_BE_TEST(load_channel_mgr_test)
#ADD_BE_TEST(export_task_mgr_test)
ADD_BE_TEST(snapshot_loader_test)
ADD_BE_TEST(user_function_cache_test)
ADD_BE_TEST(kafka_consumer_pipe_test)
ADD_BE_TEST(routine_load_task_executor_test)
ADD_BE_TEST(small_file_mgr_test)
ADD_BE_TEST(heartbeat_flags_test)

ADD_BE_TEST(result_queue_mgr_test)
ADD_BE_TEST(memory_scratch_sink_test test_env.cc)
ADD_BE_TEST(external_scan_context_mgr_test)

ADD_BE_TEST(memory/chunk_allocator_test)
ADD_BE_TEST(memory/system_allocator_test)
ADD_BE_TEST(cache/partition_cache_test)
ADD_BE_TEST(collection_value_test)
#ADD_BE_TEST(minidump_test)
ADD_BE_TEST(array_test)

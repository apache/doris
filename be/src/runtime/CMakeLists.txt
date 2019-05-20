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

# add_subdirectory(bufferpool)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/runtime")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/runtime")

add_library(Runtime STATIC
  broker_mgr.cpp
  buffered_block_mgr.cpp
  buffered_tuple_stream.cpp
  buffered_tuple_stream_ir.cpp
  buffer_control_block.cpp
  merge_sorter.cpp
  client_cache.cpp
  data_stream_mgr.cpp
  data_stream_sender.cpp
  datetime_value.cpp
  descriptors.cpp
  exec_env.cpp
  exec_env_init.cpp
  user_function_cache.cpp
  mem_pool.cpp
  plan_fragment_executor.cpp
  primitive_type.cpp
  pull_load_task_mgr.cpp
  raw_value.cpp
  raw_value_ir.cpp
  result_sink.cpp
  result_writer.cpp
  result_buffer_mgr.cpp
  row_batch.cpp
  runtime_state.cpp
  string_value.cpp
  thread_resource_mgr.cpp
  #  timestamp_value.cpp
  decimal_value.cpp
  decimalv2_value.cpp
  large_int_value.cpp
  tuple.cpp
  tuple_row.cpp
  vectorized_row_batch.cpp
  dpp_writer.cpp
  qsorter.cpp
  fragment_mgr.cpp
  dpp_sink_internal.cpp
  data_spliter.cpp
  dpp_sink.cpp
  etl_job_mgr.cpp
  load_path_mgr.cpp
  mysql_table_writer.cpp
  mysql_table_sink.cpp
  types.cpp
  tmp_file_mgr.cc
  disk_io_mgr.cc
  disk_io_mgr_reader_context.cc
  disk_io_mgr_scan_range.cc 
  buffered_block_mgr2.cc
  test_env.cc
  mem_tracker.cpp
  spill_sorter.cc
  sorted_run_merger.cc
  data_stream_recvr.cc
  buffered_tuple_stream2.cc
  buffered_tuple_stream2_ir.cc
  buffered_tuple_stream3.cc
  #  export_task_mgr.cpp
  export_sink.cpp
  tablet_writer_mgr.cpp
  bufferpool/buffer_allocator.cc
  bufferpool/buffer_pool.cc
  bufferpool/reservation_tracker.cc
  bufferpool/reservation_util.cc
  bufferpool/suballocator.cc
  bufferpool/system_allocator.cc
  initial_reservations.cc
  snapshot_loader.cpp
  query_statistics.cpp 
  message_body_sink.cpp
  stream_load/stream_load_context.cpp
  stream_load/stream_load_executor.cpp
  routine_load/data_consumer.cpp
  routine_load/data_consumer_group.cpp
  routine_load/data_consumer_pool.cpp
  routine_load/routine_load_task_executor.cpp
)

# This test runs forever so should not be part of 'make test'
#add_executable(disk_io_mgr_stress_test disk_io_mgr_stress_test.cpp)
#target_link_libraries(disk_io_mgr_stress_test ${IMPALA_TEST_LINK_LIBS})

#ADD_BE_TEST(sorter_test)
#ADD_BE_TEST(buffered_tuple_stream_test)
#ADD_BE_TEST(result_writer_test)
#ADD_BE_TEST(buffer_control_block_test)
#ADD_BE_TEST(result_buffer_mgr_test)
#ADD_BE_TEST(result_receiver_test)
#ADD_BE_TEST(result_sink_test)
#ADD_BE_TEST(mem_pool_test)
#ADD_BE_TEST(free_list_test)
#ADD_BE_TEST(string_buffer_test)
#ADD_BE_TEST(data_stream_test)
#ADD_BE_TEST(timestamp_test)
#ADD_BE_TEST(disk_io_mgr_test)
#ADD_BE_TEST(parallel_executor_test)
#ADD_BE_TEST(datetime_value_test)
#ADD_BE_TEST(decimal_value_test)
#ADD_BE_TEST(decimalv2_value_test)
#ADD_BE_TEST(string_value_test)
#ADD_BE_TEST(thread_resource_mgr_test)

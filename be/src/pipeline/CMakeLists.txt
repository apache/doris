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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/pipeline")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/pipeline")

set(PIPELINE_FILES
        pipeline.cpp
        pipeline_fragment_context.cpp
        pipeline_task.cpp
        task_queue.cpp
        task_scheduler.cpp
        exec/operator.cpp
        exec/scan_operator.cpp
        exec/mysql_scan_operator.cpp
        exec/schema_scan_operator.cpp
        exec/datagen_operator.cpp
        exec/empty_set_operator.cpp
        exec/exchange_source_operator.cpp
        exec/exchange_sink_operator.cpp
        exec/exchange_sink_buffer.cpp
        exec/result_sink_operator.cpp
        exec/result_file_sink_operator.cpp
        exec/aggregation_sink_operator.cpp
        exec/aggregation_source_operator.cpp
        exec/hashjoin_build_sink.cpp
        exec/hashjoin_probe_operator.cpp
        exec/analytic_sink_operator.cpp
        exec/analytic_source_operator.cpp
        exec/streaming_aggregation_source_operator.cpp
        exec/streaming_aggregation_sink_operator.cpp
        exec/sort_source_operator.cpp
        exec/sort_sink_operator.cpp
        exec/repeat_operator.cpp
        exec/table_function_operator.cpp
        exec/nested_loop_join_build_operator.cpp
        exec/nested_loop_join_probe_operator.cpp
        exec/set_sink_operator.cpp
        exec/set_source_operator.cpp
        exec/set_probe_sink_operator.cpp
        exec/union_sink_operator.cpp
        exec/union_source_operator.cpp
        exec/data_queue.cpp
        exec/select_operator.cpp
        exec/empty_source_operator.cpp)

add_library(Pipeline STATIC
        ${PIPELINE_FILES}
        )

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once
#include "runtime/runtime_profile.h"
#include "runtime/runtime_profile_counter_names.h"

namespace doris {

// Shared spill write counters, used by both PipelineXSpillLocalState (source)
// and PipelineXSpillSinkLocalState (sink) to eliminate duplicated counter definitions.
struct SpillWriteCounters {
    RuntimeProfile::Counter* spill_write_timer = nullptr;
    RuntimeProfile::Counter* spill_write_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* spill_writing_task_count = nullptr;
    RuntimeProfile::Counter* spill_write_wait_in_queue_timer = nullptr;
    RuntimeProfile::Counter* spill_write_file_timer = nullptr;
    RuntimeProfile::Counter* spill_write_serialize_block_timer = nullptr;
    RuntimeProfile::Counter* spill_write_block_count = nullptr;
    RuntimeProfile::Counter* spill_write_block_data_size = nullptr;
    RuntimeProfile::Counter* spill_write_rows_count = nullptr;

    void init(RuntimeProfile* profile) {
        spill_write_timer = ADD_TIMER_WITH_LEVEL(profile, profile::SPILL_WRITE_TIME, 1);
        spill_write_wait_in_queue_task_count = ADD_COUNTER_WITH_LEVEL(
                profile, profile::SPILL_WRITE_TASK_WAIT_IN_QUEUE_COUNT, TUnit::UNIT, 1);
        spill_writing_task_count =
                ADD_COUNTER_WITH_LEVEL(profile, profile::SPILL_WRITE_TASK_COUNT, TUnit::UNIT, 1);
        spill_write_wait_in_queue_timer =
                ADD_TIMER_WITH_LEVEL(profile, profile::SPILL_WRITE_TASK_WAIT_IN_QUEUE_TIME, 1);
        spill_write_file_timer = ADD_TIMER_WITH_LEVEL(profile, profile::SPILL_WRITE_FILE_TIME, 1);
        spill_write_serialize_block_timer =
                ADD_TIMER_WITH_LEVEL(profile, profile::SPILL_WRITE_SERIALIZE_BLOCK_TIME, 1);
        spill_write_block_count =
                ADD_COUNTER_WITH_LEVEL(profile, profile::SPILL_WRITE_BLOCK_COUNT, TUnit::UNIT, 1);
        spill_write_block_data_size =
                ADD_COUNTER_WITH_LEVEL(profile, profile::SPILL_WRITE_BLOCK_BYTES, TUnit::BYTES, 1);
        spill_write_rows_count =
                ADD_COUNTER_WITH_LEVEL(profile, profile::SPILL_WRITE_ROWS, TUnit::UNIT, 1);
    }
};

// Shared spill read counters, used only by PipelineXSpillLocalState (source).
struct SpillReadCounters {
    RuntimeProfile::Counter* spill_recover_time = nullptr;
    RuntimeProfile::Counter* spill_read_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* spill_reading_task_count = nullptr;
    RuntimeProfile::Counter* spill_read_wait_in_queue_timer = nullptr;
    RuntimeProfile::Counter* spill_read_file_time = nullptr;
    RuntimeProfile::Counter* spill_read_deserialize_block_timer = nullptr;
    RuntimeProfile::Counter* spill_read_block_count = nullptr;
    RuntimeProfile::Counter* spill_read_block_data_size = nullptr;
    RuntimeProfile::Counter* spill_read_file_size = nullptr;
    RuntimeProfile::Counter* spill_read_rows_count = nullptr;
    RuntimeProfile::Counter* spill_read_file_count = nullptr;

    void init(RuntimeProfile* profile) {
        spill_recover_time = ADD_TIMER_WITH_LEVEL(profile, profile::SPILL_RECOVER_TIME, 1);
        spill_read_wait_in_queue_task_count = ADD_COUNTER_WITH_LEVEL(
                profile, profile::SPILL_READ_TASK_WAIT_IN_QUEUE_COUNT, TUnit::UNIT, 1);
        spill_reading_task_count =
                ADD_COUNTER_WITH_LEVEL(profile, profile::SPILL_READ_TASK_COUNT, TUnit::UNIT, 1);
        spill_read_wait_in_queue_timer =
                ADD_TIMER_WITH_LEVEL(profile, profile::SPILL_READ_TASK_WAIT_IN_QUEUE_TIME, 1);
        spill_read_file_time = ADD_TIMER_WITH_LEVEL(profile, profile::SPILL_READ_FILE_TIME, 1);
        spill_read_deserialize_block_timer =
                ADD_TIMER_WITH_LEVEL(profile, profile::SPILL_READ_DESERIALIZE_BLOCK_TIME, 1);
        spill_read_block_count =
                ADD_COUNTER_WITH_LEVEL(profile, profile::SPILL_READ_BLOCK_COUNT, TUnit::UNIT, 1);
        spill_read_block_data_size =
                ADD_COUNTER_WITH_LEVEL(profile, profile::SPILL_READ_BLOCK_BYTES, TUnit::BYTES, 1);
        spill_read_file_size =
                ADD_COUNTER_WITH_LEVEL(profile, profile::SPILL_READ_FILE_BYTES, TUnit::BYTES, 1);
        spill_read_rows_count =
                ADD_COUNTER_WITH_LEVEL(profile, profile::SPILL_READ_ROWS, TUnit::UNIT, 1);
        spill_read_file_count =
                ADD_COUNTER_WITH_LEVEL(profile, profile::SPILL_READ_FILE_COUNT, TUnit::UNIT, 1);
    }
};

} // namespace doris

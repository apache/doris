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

#include <atomic>
#include <cstdint>
#include <deque>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace doris {

enum class CompactionTaskStatus : uint8_t {
    PENDING = 0,
    RUNNING = 1,
    FINISHED = 2,
    FAILED = 3,
};

const char* to_string(CompactionTaskStatus status);

enum class CompactionProfileType : uint8_t {
    BASE = 0,
    CUMULATIVE = 1,
    FULL = 2,
    SINGLE_REPLICA = 3,
    COLD_DATA = 4,
    INDEX_CHANGE = 5,
};

const char* to_string(CompactionProfileType type);

enum class TriggerMethod : uint8_t {
    MANUAL = 0,
    BACKGROUND = 1,
};

const char* to_string(TriggerMethod method);

struct CompactionTaskInfo {
    // identification
    int64_t compaction_id {0};
    int64_t backend_id {0};
    int64_t table_id {0};
    int64_t partition_id {0};
    int64_t tablet_id {0};

    // task attributes
    CompactionProfileType compaction_type {CompactionProfileType::BASE};
    CompactionTaskStatus status {CompactionTaskStatus::PENDING};
    TriggerMethod trigger_method {TriggerMethod::BACKGROUND};
    int64_t compaction_score {0};

    // timing
    int64_t scheduled_time_ms {0};
    int64_t start_time_ms {0};
    int64_t end_time_ms {0};

    // input stats (available after prepare_compact)
    int64_t input_rowsets_count {0};
    int64_t input_row_num {0};
    int64_t input_data_size {0};
    int64_t input_segments_num {0};
    std::string input_version_range;

    // output stats (available after complete/fail)
    int64_t merged_rows {0};
    int64_t filtered_rows {0};
    int64_t output_row_num {0};
    int64_t output_data_size {0};
    int64_t output_segments_num {0};
    std::string output_version;

    // IO stats
    int64_t bytes_read_from_local {0};
    int64_t bytes_read_from_remote {0};

    // resource
    int64_t peak_memory_bytes {0};
    bool is_vertical {false};
    int64_t permits {0};

    // error
    std::string status_msg;
};

struct RunningStats {
    int64_t start_time_ms {0};
    bool is_vertical {false};
    int64_t permits {0};
};

struct CompletionStats {
    // input version range (backfill for fallback path and to ensure it's always set)
    std::string input_version_range;
    int64_t end_time_ms {0};
    int64_t merged_rows {0};
    int64_t filtered_rows {0};
    int64_t output_row_num {0};
    int64_t output_data_size {0};
    int64_t output_segments_num {0};
    std::string output_version;
    int64_t bytes_read_from_local {0};
    int64_t bytes_read_from_remote {0};
    int64_t peak_memory_bytes {0};
};

class CompactionTaskTracker {
public:
    static CompactionTaskTracker* instance();

    int64_t next_compaction_id() { return _next_id.fetch_add(1, std::memory_order_relaxed); }

    void register_task(CompactionTaskInfo info);
    void update_to_running(int64_t compaction_id, const RunningStats& stats);
    void complete(int64_t compaction_id, const CompletionStats& stats);
    void fail(int64_t compaction_id, const CompletionStats& stats, const std::string& msg);
    void remove_task(int64_t compaction_id);

    // Returns active tasks + recent completed tasks snapshot.
    std::vector<CompactionTaskInfo> get_all_tasks() const;

    // Returns only completed tasks (for HTTP API compatibility).
    std::vector<CompactionTaskInfo> get_completed_tasks(int64_t tablet_id = 0,
                                                        int64_t top_n = 0) const;

    // Test only: clear all active and completed tasks.
    void clear_for_test();

private:
    CompactionTaskTracker() = default;

    // Apply completion stats to a task info, used by both complete() and fail()
    // and fallback path.
    void _apply_completion(CompactionTaskInfo& info, const CompletionStats& stats);

    void _trim_completed_locked();

    std::atomic<int64_t> _next_id {1};

    mutable std::shared_mutex _mutex;
    std::unordered_map<int64_t, CompactionTaskInfo> _active_tasks;
    std::deque<CompactionTaskInfo> _completed_tasks;
};

} // namespace doris

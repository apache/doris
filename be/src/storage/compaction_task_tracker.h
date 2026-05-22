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

// Standardized compaction type enum covering base / cumulative / full.
enum class CompactionProfileType : uint8_t {
    BASE = 0,
    CUMULATIVE = 1,
    FULL = 2,
};

const char* to_string(CompactionProfileType type);

// Task lifecycle status.
enum class CompactionTaskStatus : uint8_t {
    PENDING = 0,
    RUNNING = 1,
    FINISHED = 2,
    FAILED = 3,
};

const char* to_string(CompactionTaskStatus status);

// How the compaction was triggered.
enum class TriggerMethod : uint8_t {
    AUTO = 0,
    MANUAL = 1,
    LOAD_TRIGGERED = 2,
};

const char* to_string(TriggerMethod method);

// Incremental info when transitioning from PENDING to RUNNING.
struct RunningStats {
    int64_t start_time_ms {0};
    bool is_vertical {false};
    int64_t permits {0};
};

// Result info collected when a task completes or fails.
struct CompletionStats {
    // Input stats backfill: local compaction populates these in build_basic_info()
    // which runs inside execute_compact_impl(), after register_task().
    std::string input_version_range;
    int64_t input_rowsets_count {0};
    int64_t input_row_num {0};
    int64_t input_data_size {0};
    int64_t input_index_size {0};
    int64_t input_total_size {0};
    int64_t input_segments_num {0};
    int64_t end_time_ms {0};
    int64_t merged_rows {0};
    int64_t filtered_rows {0};
    int64_t output_rows {0}; // _stats.output_rows (Merger statistics)
    int64_t output_row_num {0};
    int64_t output_data_size {0};
    int64_t output_index_size {0}; // _output_rowset->index_disk_size()
    int64_t output_total_size {0}; // _output_rowset->total_disk_size()
    int64_t output_segments_num {0};
    std::string output_version;
    int64_t merge_latency_ms {0}; // _merge_rowsets_latency_timer (converted to ms)
    int64_t bytes_read_from_local {0};
    int64_t bytes_read_from_remote {0};
    int64_t peak_memory_bytes {0};
};

// Unified metadata describing a compaction task across its full lifecycle.
struct CompactionTaskInfo {
    // ===== Identity =====
    int64_t compaction_id {0}; // unique task ID assigned by Tracker
    int64_t backend_id {0};    // BE node ID
    int64_t table_id {0};      // table ID
    int64_t partition_id {0};  // partition ID
    int64_t tablet_id {0};     // tablet ID

    // ===== Task attributes =====
    CompactionProfileType compaction_type {CompactionProfileType::BASE};
    CompactionTaskStatus status {CompactionTaskStatus::PENDING};
    TriggerMethod trigger_method {TriggerMethod::AUTO};
    int64_t compaction_score {0}; // tablet compaction score at register time

    // ===== Timestamps =====
    int64_t scheduled_time_ms {0}; // task registration time
    int64_t start_time_ms {0};     // task execution start time (0 while PENDING)
    int64_t end_time_ms {0};       // task end time (0 while not completed)

    // ===== Input statistics (available after prepare_compact) =====
    int64_t input_rowsets_count {0};
    int64_t input_row_num {0};
    int64_t input_data_size {0};  // bytes, corresponds to _input_rowsets_data_size
    int64_t input_index_size {0}; // bytes, corresponds to _input_rowsets_index_size
    int64_t input_total_size {0}; // bytes, = data + index
    int64_t input_segments_num {0};
    std::string input_version_range; // e.g. "[0-5]"

    // ===== Output statistics (written at complete/fail) =====
    int64_t merged_rows {0};
    int64_t filtered_rows {0};
    int64_t output_rows {0};       // Merger output rows (_stats.output_rows; 0 for ordered path)
    int64_t output_row_num {0};    // from _output_rowset->num_rows()
    int64_t output_data_size {0};  // bytes, from _output_rowset->data_disk_size()
    int64_t output_index_size {0}; // bytes, from _output_rowset->index_disk_size()
    int64_t output_total_size {0}; // bytes, from _output_rowset->total_disk_size()
    int64_t output_segments_num {0};
    std::string output_version; // e.g. "[0-5]"

    // ===== Merge performance =====
    int64_t merge_latency_ms {0}; // merge rowsets latency (ms; 0 for ordered path)

    // ===== IO statistics (written at complete/fail) =====
    int64_t bytes_read_from_local {0};
    int64_t bytes_read_from_remote {0};

    // ===== Resources =====
    int64_t peak_memory_bytes {0}; // peak memory usage (bytes)
    bool is_vertical {false};      // whether vertical merge is used
    int64_t permits {0};           // compaction permits used

    // ===== Vertical compaction progress =====
    int64_t vertical_total_groups {0}; // total column groups (0 for horizontal)
    int64_t vertical_completed_groups {
            0}; // completed column groups (updated in real-time during RUNNING)

    // ===== Error =====
    std::string status_msg; // failure message (empty on success)
};

// Global singleton managing compaction task lifecycle.
// Receives push reports from compaction entries and execution layer,
// provides pull query interfaces for system table and HTTP API.
class CompactionTaskTracker {
public:
    static CompactionTaskTracker* instance();

    // ID allocation: globally unique monotonically increasing, restarts from 1 after BE restart.
    int64_t next_compaction_id() { return _next_id.fetch_add(1, std::memory_order_relaxed); }

    // ===== Push interfaces: lifecycle management (write lock) =====
    // All push interfaces are no-op when enable_compaction_task_tracker=false.
    void register_task(CompactionTaskInfo info);
    void update_to_running(int64_t compaction_id, const RunningStats& stats);
    void update_progress(int64_t compaction_id, int64_t total_groups, int64_t completed_groups);
    void complete(int64_t compaction_id, const CompletionStats& stats);
    void fail(int64_t compaction_id, const CompletionStats& stats, const std::string& msg);
    void remove_task(int64_t compaction_id);

    // ===== Pull interfaces: queries (read lock) =====
    // For system table: returns full snapshot copy of _active_tasks + _completed_tasks.
    std::vector<CompactionTaskInfo> get_all_tasks() const;

    // For HTTP API: iterates _completed_tasks only, returns filtered subset copy.
    std::vector<CompactionTaskInfo> get_completed_tasks(int64_t tablet_id = 0, int64_t top_n = 0,
                                                        const std::string& compaction_type = "",
                                                        int success_filter = -1) const;

    // Test only: clear all active and completed tasks.
    void clear_for_test();

private:
    CompactionTaskTracker() = default;

    void _apply_completion(CompactionTaskInfo& info, const CompletionStats& stats);
    void _trim_completed_locked();

    std::atomic<int64_t> _next_id {1};

    mutable std::shared_mutex _mutex;

    // Active tasks (PENDING + RUNNING), indexed by compaction_id.
    // Removed on complete/fail and moved to _completed_tasks.
    std::unordered_map<int64_t, CompactionTaskInfo> _active_tasks;

    // Completed tasks (FINISHED + FAILED), FIFO ring buffer.
    // Oldest records are evicted when exceeding compaction_task_tracker_max_records.
    std::deque<CompactionTaskInfo> _completed_tasks;
};

} // namespace doris

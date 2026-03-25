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

#include "storage/compaction_task_tracker.h"

#include "common/config.h"
#include "common/logging.h"

namespace doris {

const char* to_string(CompactionProfileType type) {
    switch (type) {
    case CompactionProfileType::BASE:
        return "base";
    case CompactionProfileType::CUMULATIVE:
        return "cumulative";
    case CompactionProfileType::FULL:
        return "full";
    }
    return "unknown";
}

const char* to_string(CompactionTaskStatus status) {
    switch (status) {
    case CompactionTaskStatus::PENDING:
        return "PENDING";
    case CompactionTaskStatus::RUNNING:
        return "RUNNING";
    case CompactionTaskStatus::FINISHED:
        return "FINISHED";
    case CompactionTaskStatus::FAILED:
        return "FAILED";
    }
    return "UNKNOWN";
}

const char* to_string(TriggerMethod method) {
    switch (method) {
    case TriggerMethod::AUTO:
        return "AUTO";
    case TriggerMethod::MANUAL:
        return "MANUAL";
    case TriggerMethod::LOAD_TRIGGERED:
        return "LOAD_TRIGGERED";
    }
    return "UNKNOWN";
}

CompactionTaskTracker* CompactionTaskTracker::instance() {
    static CompactionTaskTracker s_instance;
    return &s_instance;
}

void CompactionTaskTracker::register_task(CompactionTaskInfo info) {
    if (!config::enable_compaction_task_tracker) {
        return;
    }
    std::unique_lock wlock(_mutex);
    _active_tasks[info.compaction_id] = std::move(info);
}

void CompactionTaskTracker::update_to_running(int64_t compaction_id, const RunningStats& stats) {
    if (!config::enable_compaction_task_tracker) {
        return;
    }
    std::unique_lock wlock(_mutex);
    auto it = _active_tasks.find(compaction_id);
    if (it != _active_tasks.end()) {
        auto& task = it->second;
        task.status = CompactionTaskStatus::RUNNING;
        task.start_time_ms = stats.start_time_ms;
        task.is_vertical = stats.is_vertical;
        task.permits = stats.permits;
    }
}

void CompactionTaskTracker::update_progress(int64_t compaction_id, int64_t total_groups,
                                            int64_t completed_groups) {
    if (!config::enable_compaction_task_tracker) {
        return;
    }
    std::unique_lock wlock(_mutex);
    auto it = _active_tasks.find(compaction_id);
    if (it != _active_tasks.end()) {
        auto& task = it->second;
        task.vertical_total_groups = total_groups;
        task.vertical_completed_groups = completed_groups;
    }
}

void CompactionTaskTracker::complete(int64_t compaction_id, const CompletionStats& stats) {
    if (!config::enable_compaction_task_tracker) {
        return;
    }
    std::unique_lock wlock(_mutex);
    auto it = _active_tasks.find(compaction_id);
    if (it == _active_tasks.end()) {
        LOG(WARNING) << "compaction_id " << compaction_id << " not found in active_tasks, skip";
        return;
    }

    // Extract the task from active map.
    auto node = _active_tasks.extract(it);
    CompactionTaskInfo& info = node.mapped();
    info.status = CompactionTaskStatus::FINISHED;
    _apply_completion(info, stats);

    if (config::compaction_task_tracker_max_records > 0) {
        _completed_tasks.push_back(std::move(info));
        _trim_completed_locked();
    }
}

void CompactionTaskTracker::fail(int64_t compaction_id, const CompletionStats& stats,
                                 const std::string& msg) {
    if (!config::enable_compaction_task_tracker) {
        return;
    }
    std::unique_lock wlock(_mutex);
    auto it = _active_tasks.find(compaction_id);
    if (it == _active_tasks.end()) {
        LOG(WARNING) << "compaction_id " << compaction_id << " not found in active_tasks, skip";
        return;
    }

    // Extract the task from active map.
    auto node = _active_tasks.extract(it);
    CompactionTaskInfo& info = node.mapped();
    info.status = CompactionTaskStatus::FAILED;
    info.status_msg = msg;
    _apply_completion(info, stats);

    if (config::compaction_task_tracker_max_records > 0) {
        _completed_tasks.push_back(std::move(info));
        _trim_completed_locked();
    }
}

void CompactionTaskTracker::remove_task(int64_t compaction_id) {
    if (!config::enable_compaction_task_tracker) {
        return;
    }
    std::unique_lock wlock(_mutex);
    _active_tasks.erase(compaction_id); // idempotent: no-op if already removed
}

void CompactionTaskTracker::_apply_completion(CompactionTaskInfo& info,
                                              const CompletionStats& stats) {
    info.end_time_ms = stats.end_time_ms;
    info.merged_rows = stats.merged_rows;
    info.filtered_rows = stats.filtered_rows;
    info.output_rows = stats.output_rows;
    info.output_row_num = stats.output_row_num;
    info.output_data_size = stats.output_data_size;
    info.output_index_size = stats.output_index_size;
    info.output_total_size = stats.output_total_size;
    info.output_segments_num = stats.output_segments_num;
    info.output_version = stats.output_version;
    info.merge_latency_ms = stats.merge_latency_ms;
    info.bytes_read_from_local = stats.bytes_read_from_local;
    info.bytes_read_from_remote = stats.bytes_read_from_remote;
    info.peak_memory_bytes = stats.peak_memory_bytes;
    // Backfill input stats if they were 0 at register time.
    // Local compaction populates _input_rowsets_data_size etc. in build_basic_info()
    // which runs inside execute_compact_impl(), after register_task().
    if (info.input_version_range.empty() && !stats.input_version_range.empty()) {
        info.input_version_range = stats.input_version_range;
    }
    if (info.input_rowsets_count == 0 && stats.input_rowsets_count > 0) {
        info.input_rowsets_count = stats.input_rowsets_count;
    }
    if (info.input_row_num == 0 && stats.input_row_num > 0) {
        info.input_row_num = stats.input_row_num;
    }
    if (info.input_data_size == 0 && stats.input_data_size > 0) {
        info.input_data_size = stats.input_data_size;
    }
    if (info.input_index_size == 0 && stats.input_index_size > 0) {
        info.input_index_size = stats.input_index_size;
    }
    if (info.input_total_size == 0 && stats.input_total_size > 0) {
        info.input_total_size = stats.input_total_size;
    }
    if (info.input_segments_num == 0 && stats.input_segments_num > 0) {
        info.input_segments_num = stats.input_segments_num;
    }
}

void CompactionTaskTracker::_trim_completed_locked() {
    int32_t max = config::compaction_task_tracker_max_records;
    if (max <= 0) {
        _completed_tasks.clear();
        return;
    }
    while (static_cast<int32_t>(_completed_tasks.size()) > max) {
        _completed_tasks.pop_front();
    }
}

std::vector<CompactionTaskInfo> CompactionTaskTracker::get_all_tasks() const {
    std::shared_lock rlock(_mutex);
    std::vector<CompactionTaskInfo> result;
    result.reserve(_active_tasks.size() + _completed_tasks.size());
    for (const auto& [id, info] : _active_tasks) {
        result.push_back(info);
    }
    for (const auto& info : _completed_tasks) {
        result.push_back(info);
    }
    return result;
}

std::vector<CompactionTaskInfo> CompactionTaskTracker::get_completed_tasks(
        int64_t tablet_id, int64_t top_n, const std::string& compaction_type,
        int success_filter) const {
    int32_t max = config::compaction_task_tracker_max_records;
    if (max <= 0) {
        return {};
    }

    std::shared_lock rlock(_mutex);
    std::vector<CompactionTaskInfo> result;
    int32_t count = 0;
    // Iterate in reverse order (newest first).
    for (auto it = _completed_tasks.rbegin(); it != _completed_tasks.rend(); ++it) {
        if (count >= max) {
            break;
        }
        count++;
        const auto& record = *it;
        if (tablet_id != 0 && record.tablet_id != tablet_id) {
            continue;
        }
        if (!compaction_type.empty() && compaction_type != to_string(record.compaction_type)) {
            continue;
        }
        if (success_filter == 1 && record.status != CompactionTaskStatus::FINISHED) {
            continue;
        }
        if (success_filter == 0 && record.status != CompactionTaskStatus::FAILED) {
            continue;
        }
        result.push_back(record);
        if (top_n > 0 && static_cast<int64_t>(result.size()) >= top_n) {
            break;
        }
    }
    return result;
}

void CompactionTaskTracker::clear_for_test() {
    std::unique_lock wlock(_mutex);
    _active_tasks.clear();
    _completed_tasks.clear();
}

} // namespace doris

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

#include "storage/compaction/compaction_task_tracker.h"

#include "common/config.h"

namespace doris {

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

const char* to_string(CompactionProfileType type) {
    switch (type) {
    case CompactionProfileType::BASE:
        return "base";
    case CompactionProfileType::CUMULATIVE:
        return "cumulative";
    case CompactionProfileType::FULL:
        return "full";
    case CompactionProfileType::SINGLE_REPLICA:
        return "single_replica";
    case CompactionProfileType::COLD_DATA:
        return "cold_data";
    case CompactionProfileType::INDEX_CHANGE:
        return "index_change";
    }
    return "unknown";
}

const char* to_string(TriggerMethod method) {
    switch (method) {
    case TriggerMethod::MANUAL:
        return "MANUAL";
    case TriggerMethod::BACKGROUND:
        return "BACKGROUND";
    }
    return "UNKNOWN";
}

CompactionTaskTracker* CompactionTaskTracker::instance() {
    static CompactionTaskTracker s_instance;
    return &s_instance;
}

void CompactionTaskTracker::register_task(CompactionTaskInfo info) {
    std::unique_lock lock(_mutex);
    _active_tasks[info.compaction_id] = std::move(info);
}

void CompactionTaskTracker::update_to_running(int64_t compaction_id, const RunningStats& stats) {
    std::unique_lock lock(_mutex);
    auto it = _active_tasks.find(compaction_id);
    if (it == _active_tasks.end()) {
        return;
    }
    auto& info = it->second;
    info.status = CompactionTaskStatus::RUNNING;
    info.start_time_ms = stats.start_time_ms;
    info.is_vertical = stats.is_vertical;
    info.permits = stats.permits;
}

void CompactionTaskTracker::_apply_completion(CompactionTaskInfo& info,
                                              const CompletionStats& stats) {
    // Backfill input_version_range if not already set by entry point
    if (info.input_version_range.empty() && !stats.input_version_range.empty()) {
        info.input_version_range = stats.input_version_range;
    }
    info.end_time_ms = stats.end_time_ms;
    info.merged_rows = stats.merged_rows;
    info.filtered_rows = stats.filtered_rows;
    info.output_row_num = stats.output_row_num;
    info.output_data_size = stats.output_data_size;
    info.output_segments_num = stats.output_segments_num;
    info.output_version = stats.output_version;
    info.bytes_read_from_local = stats.bytes_read_from_local;
    info.bytes_read_from_remote = stats.bytes_read_from_remote;
    info.peak_memory_bytes = stats.peak_memory_bytes;
}

void CompactionTaskTracker::complete(int64_t compaction_id, const CompletionStats& stats) {
    std::unique_lock lock(_mutex);

    int32_t max = config::compaction_profile_max_records;
    if (max <= 0) {
        _active_tasks.erase(compaction_id);
        _trim_completed_locked();
        return;
    }

    auto it = _active_tasks.find(compaction_id);
    if (it != _active_tasks.end()) {
        auto info = std::move(it->second);
        _active_tasks.erase(it);
        info.status = CompactionTaskStatus::FINISHED;
        _apply_completion(info, stats);
        _completed_tasks.push_back(std::move(info));
    } else {
        // Fallback: entry point missed register_task(), create a degraded record
        CompactionTaskInfo info;
        info.compaction_id = compaction_id;
        info.status = CompactionTaskStatus::FINISHED;
        info.trigger_method = TriggerMethod::BACKGROUND;
        info.scheduled_time_ms = stats.end_time_ms; // best-effort
        _apply_completion(info, stats);
        _completed_tasks.push_back(std::move(info));
    }
    _trim_completed_locked();
}

void CompactionTaskTracker::fail(int64_t compaction_id, const CompletionStats& stats,
                                 const std::string& msg) {
    std::unique_lock lock(_mutex);

    int32_t max = config::compaction_profile_max_records;
    if (max <= 0) {
        _active_tasks.erase(compaction_id);
        _trim_completed_locked();
        return;
    }

    auto it = _active_tasks.find(compaction_id);
    if (it != _active_tasks.end()) {
        auto info = std::move(it->second);
        _active_tasks.erase(it);
        info.status = CompactionTaskStatus::FAILED;
        info.status_msg = msg;
        _apply_completion(info, stats);
        _completed_tasks.push_back(std::move(info));
    } else {
        // Fallback: degraded record
        CompactionTaskInfo info;
        info.compaction_id = compaction_id;
        info.status = CompactionTaskStatus::FAILED;
        info.status_msg = msg;
        info.trigger_method = TriggerMethod::BACKGROUND;
        info.scheduled_time_ms = stats.end_time_ms;
        _apply_completion(info, stats);
        _completed_tasks.push_back(std::move(info));
    }
    _trim_completed_locked();
}

void CompactionTaskTracker::remove_task(int64_t compaction_id) {
    std::unique_lock lock(_mutex);
    _active_tasks.erase(compaction_id);
}

void CompactionTaskTracker::clear_for_test() {
    std::unique_lock lock(_mutex);
    _active_tasks.clear();
    _completed_tasks.clear();
}

void CompactionTaskTracker::_trim_completed_locked() {
    int32_t max = config::compaction_profile_max_records;
    if (max <= 0) {
        _completed_tasks.clear();
        return;
    }
    while (static_cast<int32_t>(_completed_tasks.size()) > max) {
        _completed_tasks.pop_front();
    }
}

std::vector<CompactionTaskInfo> CompactionTaskTracker::get_all_tasks() const {
    std::shared_lock lock(_mutex);
    std::vector<CompactionTaskInfo> result;
    result.reserve(_active_tasks.size() + _completed_tasks.size());
    for (const auto& [_, info] : _active_tasks) {
        result.push_back(info);
    }
    for (const auto& info : _completed_tasks) {
        result.push_back(info);
    }
    return result;
}

std::vector<CompactionTaskInfo> CompactionTaskTracker::get_completed_tasks(int64_t tablet_id,
                                                                           int64_t top_n) const {
    int32_t max = config::compaction_profile_max_records;
    if (max <= 0) {
        return {};
    }

    std::shared_lock lock(_mutex);
    std::vector<CompactionTaskInfo> result;
    int32_t count = 0;
    for (auto it = _completed_tasks.rbegin(); it != _completed_tasks.rend(); ++it) {
        if (count >= max) {
            break;
        }
        count++;
        if (tablet_id != 0 && it->tablet_id != tablet_id) {
            continue;
        }
        result.push_back(*it);
        if (top_n > 0 && static_cast<int64_t>(result.size()) >= top_n) {
            break;
        }
    }
    return result;
}

} // namespace doris

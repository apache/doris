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

#include "storage/compaction/compaction_profile_mgr.h"

#include <ctime>

#include "common/config.h"

namespace doris {

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

namespace {

void add_time_string(rapidjson::Value& obj, const char* key, int64_t time_ms,
                     rapidjson::Document::AllocatorType& allocator) {
    time_t seconds = time_ms / 1000;
    struct tm tm_buf;
    localtime_r(&seconds, &tm_buf);
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_buf);
    obj.AddMember(rapidjson::Value(key, allocator), rapidjson::Value(buf, allocator), allocator);
}

} // namespace

void CompactionProfileRecord::to_json(rapidjson::Value& obj,
                                      rapidjson::Document::AllocatorType& allocator) const {
    obj.SetObject();
    obj.AddMember("compaction_id", compaction_id, allocator);
    obj.AddMember("compaction_type",
                  rapidjson::Value(doris::to_string(compaction_type), allocator), allocator);
    obj.AddMember("tablet_id", tablet_id, allocator);
    add_time_string(obj, "start_time", start_time_ms, allocator);
    add_time_string(obj, "end_time", end_time_ms, allocator);
    obj.AddMember("cost_time_ms", cost_time_ms, allocator);
    obj.AddMember("success", success, allocator);
    if (!success) {
        obj.AddMember("status_msg", rapidjson::Value(status_msg.c_str(), allocator), allocator);
    }

    obj.AddMember("input_rowsets_data_size", input_rowsets_data_size, allocator);
    obj.AddMember("input_rowsets_count", input_rowsets_count, allocator);
    obj.AddMember("input_row_num", input_row_num, allocator);
    obj.AddMember("input_segments_num", input_segments_num, allocator);
    obj.AddMember("input_rowsets_index_size", input_rowsets_index_size, allocator);
    obj.AddMember("input_rowsets_total_size", input_rowsets_total_size, allocator);

    obj.AddMember("merged_rows", merged_rows, allocator);
    obj.AddMember("filtered_rows", filtered_rows, allocator);
    obj.AddMember("output_rows", output_rows, allocator);

    obj.AddMember("output_rowset_data_size", output_rowset_data_size, allocator);
    obj.AddMember("output_row_num", output_row_num, allocator);
    obj.AddMember("output_segments_num", output_segments_num, allocator);
    obj.AddMember("output_rowset_index_size", output_rowset_index_size, allocator);
    obj.AddMember("output_rowset_total_size", output_rowset_total_size, allocator);

    obj.AddMember("merge_rowsets_latency_ms", merge_rowsets_latency_ns / 1000000, allocator);

    obj.AddMember("bytes_read_from_local", bytes_read_from_local, allocator);
    obj.AddMember("bytes_read_from_remote", bytes_read_from_remote, allocator);

    if (!output_version.empty()) {
        obj.AddMember("output_version", rapidjson::Value(output_version.c_str(), allocator),
                      allocator);
    }
}

CompactionProfileManager* CompactionProfileManager::instance() {
    static CompactionProfileManager s_instance;
    return &s_instance;
}

void CompactionProfileManager::_trim_locked() {
    int32_t max = config::compaction_profile_max_records;
    if (max <= 0) {
        _records.clear();
        return;
    }
    while (static_cast<int32_t>(_records.size()) > max) {
        _records.pop_front();
    }
}

void CompactionProfileManager::add_record(CompactionProfileRecord record) {
    std::unique_lock lock(_mutex);

    if (config::compaction_profile_max_records <= 0) {
        _trim_locked();
        return;
    }

    _records.push_back(std::move(record));
    _trim_locked();
}

std::vector<CompactionProfileRecord> CompactionProfileManager::get_records(int64_t tablet_id,
                                                                           int64_t top_n) {
    int32_t max = config::compaction_profile_max_records;
    if (max <= 0) {
        std::unique_lock lock(_mutex);
        _trim_locked();
        return {};
    }

    std::shared_lock lock(_mutex);
    std::vector<CompactionProfileRecord> result;
    int32_t count = 0;
    for (auto it = _records.rbegin(); it != _records.rend(); ++it) {
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

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

#include <rapidjson/document.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <shared_mutex>
#include <string>
#include <vector>

namespace doris {

enum class CompactionProfileType : uint8_t {
    BASE = 0,
    CUMULATIVE = 1,
    FULL = 2,
    SINGLE_REPLICA = 3,
    COLD_DATA = 4,
    INDEX_CHANGE = 5,
};

const char* to_string(CompactionProfileType type);

struct CompactionProfileRecord {
    int64_t compaction_id {0};
    CompactionProfileType compaction_type {CompactionProfileType::BASE};
    int64_t tablet_id {0};
    int64_t start_time_ms {0};
    int64_t end_time_ms {0};
    int64_t cost_time_ms {0};
    bool success {false};
    std::string status_msg;

    // input stats (from member variables, available in all paths)
    int64_t input_rowsets_data_size {0};
    int64_t input_rowsets_count {0};
    int64_t input_row_num {0};
    int64_t input_segments_num {0};
    int64_t input_rowsets_index_size {0};
    int64_t input_rowsets_total_size {0};

    // merge stats (from Merger::Statistics, updated in merge path)
    int64_t merged_rows {0};
    int64_t filtered_rows {0};
    int64_t output_rows {0};

    // output rowset stats (available when _output_rowset is built)
    int64_t output_rowset_data_size {0};
    int64_t output_row_num {0};
    int64_t output_segments_num {0};
    int64_t output_rowset_index_size {0};
    int64_t output_rowset_total_size {0};

    // timer (from RuntimeProfile, updated in merge path, 0 for ordered path)
    int64_t merge_rowsets_latency_ns {0};

    // IO stats
    int64_t bytes_read_from_local {0};
    int64_t bytes_read_from_remote {0};

    // version info
    std::string output_version;

    void to_json(rapidjson::Value& obj, rapidjson::Document::AllocatorType& allocator) const;
};

class CompactionProfileManager {
public:
    static CompactionProfileManager* instance();

    int64_t next_compaction_id() { return _next_id.fetch_add(1, std::memory_order_relaxed); }

    void add_record(CompactionProfileRecord record);

    // Non-const: when config=0, upgrades to write lock to clear stale records.
    std::vector<CompactionProfileRecord> get_records(int64_t tablet_id = 0, int64_t top_n = 0);

private:
    CompactionProfileManager() = default;

    void _trim_locked();

    std::atomic<int64_t> _next_id {1};

    std::shared_mutex _mutex;
    std::deque<CompactionProfileRecord> _records;
};

} // namespace doris

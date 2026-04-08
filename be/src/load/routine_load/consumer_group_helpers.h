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

#include <map>
#include <string>
#include <vector>

namespace doris {

// Helper class for partitioning work items (partitions/shards) across consumers
template <typename KeyType, typename ValueType>
class WorkPartitioner {
public:
    // Divide work items equally across N consumers using round-robin
    static std::vector<std::map<KeyType, ValueType>> partition_round_robin(
            const std::map<KeyType, ValueType>& work_items, int consumer_count) {
        std::vector<std::map<KeyType, ValueType>> result(consumer_count);
        int i = 0;
        for (const auto& [key, value] : work_items) {
            int idx = i % consumer_count;
            result[idx].emplace(key, value);
            i++;
        }
        return result;
    }
};

// Helper class for managing format-based append operations
class FormatAppender {
public:
    // Get the appropriate append function pointer based on format type
    template <typename PipeType>
    static auto get_append_function(TFileFormatType::type format)
            -> Status (PipeType::*)(const char*, size_t) {
        return (format == TFileFormatType::FORMAT_JSON) ? &PipeType::append_json
                                                        : &PipeType::append_with_line_delimiter;
    }
};

// Helper class for tracking consumption progress
class ConsumptionProgress {
public:
    ConsumptionProgress(int64_t max_time_ms, int64_t max_rows, int64_t max_bytes)
            : _initial_time(max_time_ms),
              _initial_rows(max_rows),
              _initial_bytes(max_bytes),
              _left_time(max_time_ms),
              _left_rows(max_rows),
              _left_bytes(max_bytes) {}

    // Check if any limit is reached
    bool is_limit_reached() const {
        return _left_time <= 0 || _left_rows <= 0 || _left_bytes <= 0;
    }

    // Update progress after consuming one item
    void consume_item(int64_t bytes) {
        _left_rows--;
        _left_bytes -= bytes;
    }

    // Update time progress
    void update_time(int64_t elapsed_us) { _left_time = _initial_time - elapsed_us / 1000; }

    // Getters
    int64_t left_time() const { return _left_time; }
    int64_t left_rows() const { return _left_rows; }
    int64_t left_bytes() const { return _left_bytes; }
    int64_t consumed_rows() const { return _initial_rows - _left_rows; }
    int64_t consumed_bytes() const { return _initial_bytes - _left_bytes; }
    int64_t consumed_time() const { return _initial_time - _left_time; }

private:
    int64_t _initial_time;
    int64_t _initial_rows;
    int64_t _initial_bytes;
    int64_t _left_time;
    int64_t _left_rows;
    int64_t _left_bytes;
};

} // namespace doris

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

#include <unordered_map>
#include <vector>

#include "olap/olap_common.h"
#include "olap/rowid_spill_manager.h"

namespace doris {
namespace detail {

// Abstract storage interface
class RowIdConversionStorage {
public:
    virtual ~RowIdConversionStorage() = default;
    virtual Status init(const std::vector<uint32_t>& segment_row_counts) = 0;
    virtual Status init_segment(uint32_t segment_id, uint32_t num_rows) = 0;
    virtual Status add(uint32_t segment_id, uint32_t row_id,
                       const std::pair<uint32_t, uint32_t>& value) = 0;
    virtual Status get(uint32_t segment_id, uint32_t row_id,
                       std::pair<uint32_t, uint32_t>* value) const = 0;
    virtual size_t memory_usage() const = 0;
    virtual const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>&
    get_rowid_conversion_map() const = 0;
};

// Vector-based storage implementation, memory only
class MemoryStorage final : public RowIdConversionStorage {
public:
    Status init(const std::vector<uint32_t>& segment_row_counts) override {
        _segments.reserve(segment_row_counts.size());
        return Status::OK();
    }

    Status init_segment(uint32_t segment_id, uint32_t num_rows) override {
        if (segment_id >= _segments.size()) {
            _segments.resize(segment_id + 1);
        }
        _segments[segment_id].resize(num_rows, {UINT32_MAX, UINT32_MAX});
        return Status::OK();
    }

    Status add(uint32_t segment_id, uint32_t row_id,
               const std::pair<uint32_t, uint32_t>& value) override {
        auto& vec = _segments[segment_id];
        if (row_id >= vec.size()) {
            vec.resize(row_id + 1, {UINT32_MAX, UINT32_MAX});
        }
        vec[row_id] = value;
        return Status::OK();
    }

    Status get(uint32_t segment_id, uint32_t row_id,
               std::pair<uint32_t, uint32_t>* value) const override {
        if (segment_id >= _segments.size()) {
            return Status::NotFound("segment_id out of range");
        }
        const auto& vec = _segments[segment_id];
        if (row_id >= vec.size()) {
            return Status::NotFound("row_id out of range");
        }
        const auto& pair = vec[row_id];
        if (pair.first == UINT32_MAX && pair.second == UINT32_MAX) {
            return Status::NotFound("row not found");
        }
        *value = pair;
        return Status::OK();
    }

    size_t memory_usage() const override {
        size_t total = 0;
        for (const auto& vec : _segments) {
            total += vec.capacity() * sizeof(std::pair<uint32_t, uint32_t>);
        }
        return total + _segments.capacity() * sizeof(std::vector<std::pair<uint32_t, uint32_t>>);
    }

    const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& get_rowid_conversion_map()
            const override {
        return _segments;
    }

private:
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> _segments;
};

// Map-based storage with spilling implementation
class SpillableStorage final : public RowIdConversionStorage {
public:
    SpillableStorage() {
        _spill_manager = std::make_unique<RowIdSpillManager>("/tmp/doris_rowid_spill");
        // _spill_dir = "/tmp/doris_rowid_spill_" + std::to_string(std::time(nullptr));
        // auto st = FileUtils::create_dir(_spill_dir);
        // LOG_IF(WARNING, !st.ok()) << "Failed to create spill directory: " << st.to_string();
    }

    Status init(const std::vector<uint32_t>& segment_row_counts) override {
        RETURN_IF_ERROR(_spill_manager->init(segment_row_counts));
        _segments.reserve(segment_row_counts.size());
        return Status::OK();
    }

    Status init_segment(uint32_t segment_id, uint32_t num_rows) override {
        if (segment_id >= _segments.size()) {
            _segments.resize(segment_id + 1);
        }
        _segments[segment_id].reserve(std::min(num_rows, static_cast<uint32_t>(16384)));
        _segment_sizes.resize(segment_id + 1);
        _segment_sizes[segment_id] = num_rows;
        return Status::OK();
    }

    Status add(uint32_t segment_id, uint32_t row_id,
               const std::pair<uint32_t, uint32_t>& value) override {
        auto& segment = _segments[segment_id];
        segment[row_id] = value;

        // Check if need to spill
        if (segment.size() >= _spill_threshold) {
            RETURN_IF_ERROR(_spill_manager->spill_segment(segment_id, segment));
        }
        return Status::OK();
    }

    // TODO: fix me!!
    Status get(uint32_t segment_id, uint32_t row_id,
               std::pair<uint32_t, uint32_t>* value) const override {
        // Check memory first
        const auto& segment = _segments[segment_id];
        auto it = segment.find(row_id);
        if (it != segment.end()) {
            *value = it->second;
            return Status::OK();
        }

        // Check spilled data
        if (_is_spilled[segment_id]) {
            std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>> mappings;
            RETURN_IF_ERROR(_spill_manager->read_segment(segment_id, &mappings));
            it = mappings.find(row_id);
            if (it != mappings.end()) {
                *value = it->second;
                return Status::OK();
            }
        }

        return Status::OK();
    }

    size_t memory_usage() const override {
        size_t total = 0;
        for (const auto& map : _segments) {
            total +=
                    map.bucket_count() * (sizeof(uint32_t) + sizeof(std::pair<uint32_t, uint32_t>));
        }
        return total + _segments.capacity() *
                               sizeof(std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>);
    }

    const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& get_rowid_conversion_map()
            const override {
        throw Exception(Status::FatalError("Unreachable"));
    }

private:
    static constexpr size_t _spill_threshold = 1000000; // 1M entries
    std::string _spill_dir;
    std::vector<std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>> _segments;
    std::vector<uint32_t> _segment_sizes;
    std::vector<bool> _is_spilled;

    std::unique_ptr<RowIdSpillManager> _spill_manager;
};

} // namespace detail
} // namespace doris
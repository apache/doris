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

#include <memory_resource>
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
    virtual Status add(uint32_t segment_id, uint32_t row_id,
                       const std::pair<uint32_t, uint32_t>& value) = 0;
    virtual Status get(uint32_t segment_id, uint32_t row_id,
                       std::pair<uint32_t, uint32_t>* value) = 0;
    virtual void prune_segment_mapping(uint32_t segment_id) = 0;
    virtual std::size_t memory_usage() const = 0;
    virtual Status spill_if_eligible() { return Status::OK(); }
    virtual const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>&
    get_rowid_conversion_map() const = 0;
};

// Vector-based storage implementation, memory only
class RowIdMemoryStorage final : public RowIdConversionStorage {
public:
    Status init(const std::vector<uint32_t>& segment_row_counts) override {
        _segments.resize(segment_row_counts.size());
        for (std::size_t i = 0; i < segment_row_counts.size(); ++i) {
            _segments[i].resize(segment_row_counts[i], {UINT32_MAX, UINT32_MAX});
        }
        return Status::OK();
    }

    Status add(uint32_t segment_id, uint32_t row_id,
               const std::pair<uint32_t, uint32_t>& value) override {
        auto& vec = _segments[segment_id];
        vec[row_id] = value;
        return Status::OK();
    }

    Status get(uint32_t segment_id, uint32_t row_id,
               std::pair<uint32_t, uint32_t>* value) override {
        if (segment_id >= _segments.size()) {
            return Status::NotFound<false>("segment_id out of range");
        }
        const auto& vec = _segments[segment_id];
        if (row_id >= vec.size()) {
            return Status::NotFound<false>("row_id out of range");
        }
        const auto& pair = vec[row_id];
        if (pair.first == UINT32_MAX && pair.second == UINT32_MAX) {
            return Status::NotFound<false>("row not found");
        }
        *value = pair;
        return Status::OK();
    }

    void prune_segment_mapping(uint32_t segment_id) override {
        if (segment_id < _segments.size()) {
            _segments[segment_id].clear();
        }
    }

    std::size_t memory_usage() const override {
        std::size_t total = 0;
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
class RowIdSpillableStorage final : public RowIdConversionStorage {
public:
    struct SegmentData {
        bool is_spilled {false};
        TrackableResource resource;
        RowIdMappingType mapping {&resource};
    };

    RowIdSpillableStorage(const std::string& path)
            : _spill_manager {std::make_unique<RowIdSpillManager>(path)} {}

    Status init(const std::vector<uint32_t>& segment_row_counts) override {
        RETURN_IF_ERROR(_spill_manager->init(segment_row_counts));
        _segments.resize(segment_row_counts.size());
        return Status::OK();
    }

    Status add(uint32_t segment_id, uint32_t row_id,
               const std::pair<uint32_t, uint32_t>& value) override {
        auto& segment_mapping = _segments[segment_id].mapping;
        segment_mapping[row_id] = value;
        return Status::OK();
    }

    Status spill_if_eligible() override {
        // First check if current segment needs spilling
        for (std::size_t i = 0; i < _segments.size(); ++i) {
            RETURN_IF_ERROR(check_and_spill_segment(i));
        }

        // Then check if total memory exceeds limit
        RETURN_IF_ERROR(check_and_spill_all());
        return Status::OK();
    }

    Status get(uint32_t segment_id, uint32_t row_id,
               std::pair<uint32_t, uint32_t>* value) override {
        auto& mappings = _segments[segment_id].mapping;
        if (_segments[segment_id].is_spilled) {
            RETURN_IF_ERROR(_spill_manager->read_segment_mapping(segment_id, &mappings));
        }

        if (auto it = mappings.find(row_id); it != mappings.end()) {
            *value = it->second;
            return Status::OK();
        }
        return Status::NotFound("row_id not found in spilled data");
    }

    void prune_segment_mapping(uint32_t segment_id) override {
        if (segment_id < _segments.size()) {
            _segments[segment_id].mapping.clear();
            _segments[segment_id].resource.reset();
            _segments[segment_id].is_spilled = false;
        }
    }

    const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& get_rowid_conversion_map()
            const override {
        throw Exception(Status::FatalError("Unreachable"));
    }

    std::size_t memory_usage() const override {
        std::size_t total = 0;
        for (const auto& segment : _segments) {
            total += segment.resource.bytes_allocated();
        }
        return total;
    }

private:
    static constexpr std::size_t _spill_threshold = 1000000;               // 1M entries per segment
    static constexpr std::size_t _total_memory_limit = 1024 * 1024 * 1024; // 1GB total

    Status _spill_segment(uint32_t segment_id) {
        auto& segment = _segments[segment_id];
        RETURN_IF_ERROR(_spill_manager->spill_segment_mapping(segment_id, segment.mapping));
        segment.is_spilled = true;
        segment.mapping.clear();
        segment.resource.reset();
        return Status::OK();
    }

    Status check_and_spill_segment(uint32_t segment_id) {
        if (_segments[segment_id].resource.bytes_allocated() >=
            config::rowid_conversion_max_mb * 1024 * 1024) {
            RETURN_IF_ERROR(_spill_segment(segment_id));
        }
        return Status::OK();
    }

    Status check_and_spill_all() {
        if (memory_usage() >= config::rowid_conversion_max_mb * 1024 * 1024) {
            for (std::size_t i = 0; i < _segments.size(); ++i) {
                if (!_segments[i].mapping.empty()) {
                    RETURN_IF_ERROR(_spill_segment(i));
                }
            }
        }
        return Status::OK();
    }

    std::vector<SegmentData> _segments;
    std::unique_ptr<RowIdSpillManager> _spill_manager;
};

} // namespace detail
} // namespace doris
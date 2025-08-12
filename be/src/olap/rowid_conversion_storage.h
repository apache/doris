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
#include "runtime/memory/global_memory_arbitrator.h"

namespace doris {
namespace detail {

// Abstract storage interface
class RowIdConversionStorage {
public:
    virtual ~RowIdConversionStorage() = default;
    virtual Status init() { return Status::OK(); }
    virtual Status init_new_segments(const RowsetId& rowset_id,
                                     const std::vector<uint32_t>& segment_row_counts) = 0;
    virtual Status add(const RowsetId& rowset_id, uint32_t segment_id, uint32_t row_id,
                       const std::pair<uint32_t, uint32_t>& value) = 0;
    virtual Status get(const RowsetId& rowset_id, uint32_t segment_id, uint32_t row_id,
                       std::pair<uint32_t, uint32_t>* value) = 0;
    virtual void prune_segment_mapping(const RowsetId& rowset_id, uint32_t segment_id) = 0;
    virtual std::size_t memory_usage() const = 0;
    virtual Status spill_if_eligible() { return Status::OK(); }
    virtual const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>&
    get_rowid_conversion_map() const = 0;

    const std::map<std::pair<RowsetId, uint32_t>, uint32_t>& get_src_segment_to_id_map() const {
        return _segment_to_id_map;
    }

protected:
    std::map<std::pair<RowsetId, uint32_t>, uint32_t> _segment_to_id_map;
    std::vector<std::pair<RowsetId, uint32_t>> _id_to_segment_map;
};

// Vector-based storage implementation, memory only
class RowIdMemoryStorage final : public RowIdConversionStorage {
public:
    Status init_new_segments(const RowsetId& src_rowset_id,
                             const std::vector<uint32_t>& segment_row_counts) override {
        for (size_t i = 0; i < segment_row_counts.size(); i++) {
            constexpr size_t RESERVED_MEMORY = 10 * 1024 * 1024;
            if (doris::GlobalMemoryArbitrator::is_exceed_hard_mem_limit(RESERVED_MEMORY)) {
                return Status::MemoryLimitExceeded("Memory limit exceeded during init");
            }

            uint32_t id = cast_set<uint32_t>(_segment_to_id_map.size());
            _segment_to_id_map.emplace(std::pair<RowsetId, uint32_t> {src_rowset_id, i}, id);
            _id_to_segment_map.emplace_back(src_rowset_id, i);

            std::vector<std::pair<uint32_t, uint32_t>> vec(
                    segment_row_counts[i], std::pair<uint32_t, uint32_t>(UINT32_MAX, UINT32_MAX));
            _segments.emplace_back(std::move(vec));
        }

        return Status::OK();
    }

    Status add(const RowsetId& rowset_id, uint32_t segment_id, uint32_t row_id,
               const std::pair<uint32_t, uint32_t>& value) override {
        uint32_t internal_id =
                _segment_to_id_map.at(std::pair<RowsetId, uint32_t> {rowset_id, segment_id});
        auto& vec = _segments[internal_id];
        vec[row_id] = value;
        return Status::OK();
    }

    Status get(const RowsetId& rowset_id, uint32_t segment_id, uint32_t row_id,
               std::pair<uint32_t, uint32_t>* value) override {
        auto it = _segment_to_id_map.find({rowset_id, segment_id});
        if (it == _segment_to_id_map.end()) {
            return Status::NotFound("segment mapping not found");
        }
        uint32_t internal_id = it->second;
        if (internal_id >= _segments.size()) {
            return Status::NotFound<false>("internal_id out of range");
        }
        const auto& vec = _segments[internal_id];
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

    void prune_segment_mapping(const RowsetId& rowset_id, uint32_t segment_id) override {
        if (auto it = _segment_to_id_map.find({rowset_id, segment_id});
            it != _segment_to_id_map.end()) {
            uint32_t internal_id = it->second;
            _segments[internal_id].clear();
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
        std::unique_ptr<TrackableResource> resource {std::make_unique<TrackableResource>()};
        RowIdMappingType mapping {resource.get()};
    };

    RowIdSpillableStorage(int64_t memory_limit, const std::string& path)
            : _memory_limit(memory_limit),
              _spill_manager {std::make_unique<RowIdSpillManager>(path)} {}

    Status init() override { return _spill_manager->init(); }

    Status init_new_segments(const RowsetId& rowset_id,
                             const std::vector<uint32_t>& segment_row_counts) override {
        for (size_t i = 0; i < segment_row_counts.size(); i++) {
            uint32_t id = cast_set<uint32_t>(_segment_to_id_map.size());
            _segment_to_id_map.emplace(std::pair<RowsetId, uint32_t> {rowset_id, i}, id);
            _id_to_segment_map.emplace_back(rowset_id, i);
            _segments.emplace_back();
            RETURN_IF_ERROR(_spill_manager->init_new_segment(id, segment_row_counts[i]));
        }

        return Status::OK();
    }

    Status add(const RowsetId& rowset_id, uint32_t segment_id, uint32_t row_id,
               const std::pair<uint32_t, uint32_t>& value) override {
        uint32_t internal_id =
                _segment_to_id_map.at(std::pair<RowsetId, uint32_t> {rowset_id, segment_id});
        auto& segment_mapping = _segments[internal_id].mapping;
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

    Status get(const RowsetId& rowset_id, uint32_t segment_id, uint32_t row_id,
               std::pair<uint32_t, uint32_t>* value) override {
        auto it = _segment_to_id_map.find({rowset_id, segment_id});
        if (it == _segment_to_id_map.end()) {
            return Status::NotFound("segment mapping not found");
        }
        uint32_t internal_id = it->second;
        auto& mappings = _segments[internal_id].mapping;
        if (_segments[internal_id].is_spilled) {
            RETURN_IF_ERROR(_spill_manager->read_segment_mapping(internal_id, &mappings));
        }

        if (auto it = mappings.find(row_id); it != mappings.end()) {
            *value = it->second;
            return Status::OK();
        }
        return Status::NotFound("row_id not found in spilled data");
    }

    void prune_segment_mapping(const RowsetId& rowset_id, uint32_t segment_id) override {
        if (auto it = _segment_to_id_map.find({rowset_id, segment_id});
            it != _segment_to_id_map.end()) {
            uint32_t internal_id = it->second;
            _segments[internal_id].mapping.clear();
            _segments[internal_id].resource->reset();
            _segments[internal_id].is_spilled = false;
        }
    }

    // for inverted index compaction
    // TODO: fix me
    const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& get_rowid_conversion_map()
            const override {
        throw Exception(Status::FatalError("Unreachable"));
    }

    std::size_t memory_usage() const override {
        std::size_t total = 0;
        for (const auto& segment : _segments) {
            total += segment.resource->bytes_allocated();
        }
        return total;
    }

private:
    Status _spill_segment(uint32_t internal_id) {
        auto& segment = _segments[internal_id];
        LOG_INFO(
                "[verbose] begin to spill segment mapping, internal_id={}, current memory "
                "usage={}, totoal memory usage={}, segment info={}",
                internal_id, segment.resource->bytes_allocated(), memory_usage(),
                _spill_manager->dump_segment_info(internal_id));
        RETURN_IF_ERROR(_spill_manager->spill_segment_mapping(internal_id, segment.mapping));
        segment.is_spilled = true;
        segment.mapping.clear();
        segment.resource->reset();
        LOG_INFO(
                "[verbose] after spilling segment mapping, internal_id={}, current memory "
                "usage={}, totoal memory usage={}, segment info={}",
                internal_id, segment.resource->bytes_allocated(), memory_usage(),
                _spill_manager->dump_segment_info(internal_id));
        return Status::OK();
    }

    Status check_and_spill_segment(uint32_t internal_id) {
        if (_segments[internal_id].resource->bytes_allocated() >= memory_limit()) {
            RETURN_IF_ERROR(_spill_segment(internal_id));
        }
        return Status::OK();
    }

    Status check_and_spill_all() {
        if (memory_usage() >= memory_limit()) {
            for (std::size_t i = 0; i < _segments.size(); ++i) {
                if (!_segments[i].mapping.empty()) {
                    RETURN_IF_ERROR(_spill_segment(i));
                }
            }
        }
        return Status::OK();
    }

    int64_t memory_limit() const { return _memory_limit; }

    int64_t _memory_limit;
    std::vector<SegmentData> _segments;
    std::unique_ptr<RowIdSpillManager> _spill_manager;
};

} // namespace detail
} // namespace doris
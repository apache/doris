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

namespace doris::detail {
#include "common/compile_check_begin.h"

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

    // for inverted index compaction
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
                             const std::vector<uint32_t>& segment_row_counts) override;

    Status add(const RowsetId& rowset_id, uint32_t segment_id, uint32_t row_id,
               const std::pair<uint32_t, uint32_t>& value) override;

    Status get(const RowsetId& rowset_id, uint32_t segment_id, uint32_t row_id,
               std::pair<uint32_t, uint32_t>* value) override;

    void prune_segment_mapping(const RowsetId& rowset_id, uint32_t segment_id) override;

    std::size_t memory_usage() const override;

    const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& get_rowid_conversion_map()
            const override;

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

    Status init() override;

    Status init_new_segments(const RowsetId& rowset_id,
                             const std::vector<uint32_t>& segment_row_counts) override;

    Status add(const RowsetId& rowset_id, uint32_t segment_id, uint32_t row_id,
               const std::pair<uint32_t, uint32_t>& value) override;

    Status spill_if_eligible() override;

    Status get(const RowsetId& rowset_id, uint32_t segment_id, uint32_t row_id,
               std::pair<uint32_t, uint32_t>* value) override;

    void prune_segment_mapping(const RowsetId& rowset_id, uint32_t segment_id) override;

    const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& get_rowid_conversion_map()
            const override;

    std::size_t memory_usage() const override;

private:
    Status _spill_segment(uint32_t internal_id);

    Status check_and_spill_segment(uint32_t internal_id);

    Status check_and_spill_all();

    int64_t memory_limit() const;

    int64_t _memory_limit;
    std::vector<SegmentData> _segments;
    std::unique_ptr<RowIdSpillManager> _spill_manager;
};
#include "common/compile_check_end.h"

} // namespace doris::detail
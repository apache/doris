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
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "common/check.h"
#include "core/custom_allocator.h"
#include "storage/compressed_rowid_map.h"
#include "storage/olap_common.h"
#include "storage/utils.h"

namespace doris {

// Compaction's source-to-destination mapping, consumed by delete bitmap conversion
// and inverted-index merging. Source identity/visibility are independent of storage.
class RowIdConversion {
public:
    enum class Mode { DENSE, LAZY_CHUNKED, COMPRESSED };
    using RowIdPair = std::pair<uint32_t, uint32_t>;
    using DenseMap = std::vector<std::vector<RowIdPair>>;
    using SourceIndex = DorisMap<std::pair<RowsetId, uint32_t>, uint32_t>;
    using VisibilityFilteredRows =
            std::set<RowLocation, std::less<>, CustomStdAllocator<RowLocation>>;

    struct DestinationRowId {
        uint32_t segment_pos;
        uint32_t row_id;
    };

    explicit RowIdConversion(Mode mode = Mode::DENSE);
    ~RowIdConversion();
    void set_mode(Mode mode);
    Mode mode() const { return _mode; }
    Status finalize(const std::vector<uint32_t>& destination_rows);
    RowIdConversionSource source(uint32_t source_id) const;
    RowIdConversionSource find_source(const RowsetId& rowset, uint32_t segment) const;
    size_t source_count() const { return _segment_num_rows.size(); }
    CompressedRowIdMap::MemoryUsage compressed_memory_usage() const;

    Status init_segment_map(const RowsetId& src_rowset_id, const std::vector<uint32_t>& segment_ids,
                            const std::vector<uint32_t>& num_rows);
    void set_dst_rowset_id(const RowsetId& dst_rowset_id) { _dst_rowst_id = dst_rowset_id; }
    const RowsetId& get_dst_rowset_id() const { return _dst_rowst_id; }

    // Debug correctness checks distinguish rows discarded by visibility filtering from
    // missing mappings. Such rows can acquire delete-bitmap entries during concurrent loads.
    void enable_visibility_filter_tracking() {
        _visibility_filtered_rows = std::make_unique<VisibilityFilteredRows>();
    }
    VisibilityFilteredRows* visibility_filtered_rows() { return _visibility_filtered_rows.get(); }
    bool is_visibility_filtered(const RowLocation& src) const {
        return _visibility_filtered_rows != nullptr && _visibility_filtered_rows->contains(src);
    }

    Status add(const std::vector<RowLocation>& rss_row_ids,
               const std::vector<uint32_t>& dst_segments_num_row);

    // Legacy, infallible lookup for DENSE/LAZY_CHUNKED. Compressed consumers use source().
    // The physical destination segment ID is resolved after the output rowset is built.
    int get(const RowLocation& src, DestinationRowId* dst) const;
    const DenseMap& get_rowid_conversion_map() const;
    size_t memory_usage() const;

    const SourceIndex& get_src_segment_to_id_map() const { return _segment_to_id_map; }
    std::pair<RowsetId, uint32_t> get_segment_by_id(uint32_t id) const {
        DCHECK_GT(_id_to_segment_map.size(), id);
        return _id_to_segment_map.at(id);
    }
    uint32_t get_id_by_segment(const std::pair<RowsetId, uint32_t>& segment) const {
        return _segment_to_id_map.at(segment);
    }

private:
    friend class RowIdConversionSource;
    Status lookup_source(uint32_t source_id, uint32_t row, CompressedRowIdMap::Cursor& cursor,
                         CompressedRowIdMap::DecodeWorkspace& workspace,
                         RowIdConversionSource::Pair* destination) const;

    // Concrete storage alternatives live in the .cpp. Dispatch once per add() batch,
    // so its row loop has neither mode checks nor per-row virtual calls.
    struct Storage;
    std::unique_ptr<Storage> _storage;
    Mode _mode;
    DorisVector<uint32_t> _segment_num_rows;
    std::unique_ptr<VisibilityFilteredRows> _visibility_filtered_rows;
    SourceIndex _segment_to_id_map;
    DorisVector<std::pair<RowsetId, uint32_t>> _id_to_segment_map;
    RowsetId _dst_rowst_id;
};

} // namespace doris

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

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "common/status.h"
#include "storage/partial_update_info.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/segment/historical_row_retriever.h"

namespace doris {
class Block;
class TabletSchema;

// Reads old-row column values for merge-on-write loads. One instance for each
// segment-writer flush; it owns the rowset pins and the read plans that the
// MowKeyProbe outcomes feed.
class HistoricalRowFetcher {
public:
    explicit HistoricalRowFetcher(segment_v2::HistoricalRowRetrieverContext context);

    // Keep `rowset` alive until fill/read is done.
    void pin_rowset(const RowsetSharedPtr& rowset);

    // ---- plan building; the destination position (segment_pos / delta_pos /
    //      block_pos) is not interpreted by the fetcher ----
    void plan_fixed_read(const RowLocation& loc, size_t dst_pos);
    void plan_flexible_read(const RowLocation& loc, size_t dst_pos, const BitmapValue& skip_bitmap);

    // ---- fixed partial update / binlog AFTER fill ----
    // Same behavior as FixedReadPlan::fill_missing_columns: forces reading
    // old delete signs, fills in this order: default -> null -> auto-inc ->
    // any value, keeps the sequence column non-decreasing over delete-signed
    // old rows.
    Status fill_missing_columns(const TabletSchema& tablet_schema, Block& full_block,
                                const std::vector<bool>& use_default_or_null_flag,
                                bool has_default_or_nullable, uint32_t segment_start_pos,
                                const Block* block) const;

    // ---- flexible partial update fill ----
    // Same behavior as FlexibleReadPlan::fill_non_primary_key_columns:
    // driven by the skip bitmap, re-checks the delete sign with the seq map
    // column in mind, on_update_current_timestamp forces default, auto-inc
    // copied in place from the current block.
    Status fill_non_primary_key_columns(const TabletSchema& tablet_schema, Block& full_block,
                                        const std::vector<bool>& use_default_or_null_flag,
                                        bool has_default_or_nullable, uint32_t segment_start_pos,
                                        uint32_t block_start_pos, const Block* block,
                                        std::vector<BitmapValue>* skip_bitmaps) const;

    // ---- raw column read on the fixed plan ----
    // No default/null fill steps, no auto-inc, no seq handling. Used by the
    // binlog BEFORE image (cids = visible value columns; rows not in
    // *read_index are left as NULL) and BlockAggregator::fill_sequence_column.
    Status read_columns(const TabletSchema& tablet_schema, std::vector<uint32_t> cids_to_read,
                        Block& dst_block, std::map<uint32_t, uint32_t>* read_index,
                        bool force_read_old_delete_signs,
                        const signed char* __restrict cur_delete_signs = nullptr) const;

    const std::map<RowsetId, RowsetSharedPtr>& pinned_rowsets() const { return _rsid_to_rowset; }

private:
    segment_v2::HistoricalRowRetrieverContext _context;
    FixedReadPlan _fixed_plan;
    FlexibleReadPlan _flexible_plan;
    std::map<RowsetId, RowsetSharedPtr> _rsid_to_rowset;
};

} // namespace doris

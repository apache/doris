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

// Reads old-row column values for merge-on-write loads. One instance for each block appended to a
// segment writer; it owns the rowset pins and the read plans that the MowKeyProbe outcomes feed.
class HistoricalRowFetcher {
public:
    // The per-method `tablet_schema` below is the schema of the block being filled; `context` also
    // carries one, used only to decide up front whether the flexible plan reads the row store.
    explicit HistoricalRowFetcher(segment_v2::HistoricalRowRetrieverContext context);

    // Keep `rowset` alive until fill/read is done.
    void pin_rowset(const RowsetSharedPtr& rowset);

    // Plan building. `dst_pos` is where the old row lands: read_columns() only uses it as the key
    // of *read_index, but both fill methods below index the block by it, so they require
    // `segment_start_pos + i` for the i-th row of the block.
    void plan_fixed_read(const RowLocation& loc, size_t dst_pos);
    void plan_flexible_read(const RowLocation& loc, size_t dst_pos, const BitmapValue& skip_bitmap);

    // Fixed partial update and the binlog AFTER image. Same behavior as
    // FixedReadPlan::fill_missing_columns, including its default / null / auto-inc order and its
    // handling of delete-signed old rows.
    Status fill_missing_columns(const TabletSchema& tablet_schema, Block& full_block,
                                const std::vector<bool>& use_default_or_null_flag,
                                bool has_default_or_nullable, uint32_t segment_start_pos,
                                const Block* block,
                                std::vector<signed char>* old_delete_signs = nullptr) const;

    // Collects the old rows' delete signs out of a block read by read_columns(), indexed by the
    // caller's row position. Rows with no old row keep a zero sign.
    Status fill_old_delete_signs(const Block& old_value_block,
                                 const std::map<uint32_t, uint32_t>& read_index, size_t num_rows,
                                 std::vector<signed char>* old_delete_signs) const;

    // Flexible partial update. Same behavior as FlexibleReadPlan::fill_non_primary_key_columns,
    // which fills per cell, driven by the skip bitmap.
    Status fill_non_primary_key_columns(const TabletSchema& tablet_schema, Block& full_block,
                                        const std::vector<bool>& use_default_or_null_flag,
                                        bool has_default_or_nullable, uint32_t segment_start_pos,
                                        uint32_t block_start_pos, const Block* block,
                                        std::vector<BitmapValue>* skip_bitmaps) const;

    // A raw read on the fixed plan: no fill steps at all. Used by the binlog BEFORE image, where
    // rows missing from *read_index stay NULL.
    Status read_columns(const TabletSchema& tablet_schema, std::vector<uint32_t> cids_to_read,
                        Block& dst_block, std::map<uint32_t, uint32_t>* read_index,
                        bool force_read_old_delete_signs,
                        const signed char* __restrict cur_delete_signs = nullptr) const;

    const std::map<RowsetId, RowsetSharedPtr>& pinned_rowsets() const { return _rsid_to_rowset; }

    // True when no fixed read was planned, i.e. no row needs an old value.
    bool empty() const { return _fixed_plan.empty(); }

private:
    // _context must stay declared before _flexible_plan, whose constructor argument reads
    // _context.tablet_schema.
    segment_v2::HistoricalRowRetrieverContext _context;
    FixedReadPlan _fixed_plan;
    FlexibleReadPlan _flexible_plan;
    std::map<RowsetId, RowsetSharedPtr> _rsid_to_rowset;
};

} // namespace doris

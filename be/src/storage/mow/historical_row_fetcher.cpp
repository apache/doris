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

#include "storage/mow/historical_row_fetcher.h"

#include "storage/rowset/rowset.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

HistoricalRowFetcher::HistoricalRowFetcher(segment_v2::HistoricalRowRetrieverContext context)
        : _context(std::move(context)),
          _flexible_plan(_context.tablet_schema->has_row_store_for_all_columns()) {}

void HistoricalRowFetcher::pin_rowset(const RowsetSharedPtr& rowset) {
    _rsid_to_rowset.emplace(rowset->rowset_id(), rowset);
}

void HistoricalRowFetcher::plan_fixed_read(const RowLocation& loc, size_t dst_pos) {
    _fixed_plan.prepare_to_read(loc, dst_pos);
}

void HistoricalRowFetcher::plan_flexible_read(const RowLocation& loc, size_t dst_pos,
                                              const BitmapValue& skip_bitmap) {
    _flexible_plan.prepare_to_read(loc, dst_pos, skip_bitmap);
}

Status HistoricalRowFetcher::fill_missing_columns(const TabletSchema& tablet_schema,
                                                  Block& full_block,
                                                  const std::vector<bool>& use_default_or_null_flag,
                                                  bool has_default_or_nullable,
                                                  uint32_t segment_start_pos,
                                                  const Block* block) const {
    return _fixed_plan.fill_missing_columns(_context, _rsid_to_rowset, tablet_schema, full_block,
                                            use_default_or_null_flag, has_default_or_nullable,
                                            segment_start_pos, block);
}

Status HistoricalRowFetcher::fill_non_primary_key_columns(
        const TabletSchema& tablet_schema, Block& full_block,
        const std::vector<bool>& use_default_or_null_flag, bool has_default_or_nullable,
        uint32_t segment_start_pos, uint32_t block_start_pos, const Block* block,
        std::vector<BitmapValue>* skip_bitmaps) const {
    return _flexible_plan.fill_non_primary_key_columns(
            _context, _rsid_to_rowset, tablet_schema, full_block, use_default_or_null_flag,
            has_default_or_nullable, segment_start_pos, block_start_pos, block, skip_bitmaps);
}

Status HistoricalRowFetcher::read_columns(const TabletSchema& tablet_schema,
                                          std::vector<uint32_t> cids_to_read, Block& dst_block,
                                          std::map<uint32_t, uint32_t>* read_index,
                                          bool force_read_old_delete_signs,
                                          const signed char* __restrict cur_delete_signs) const {
    return _fixed_plan.read_columns_by_plan(tablet_schema, std::move(cids_to_read), _rsid_to_rowset,
                                            dst_block, read_index, force_read_old_delete_signs,
                                            cur_delete_signs);
}

} // namespace doris

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

#include "olap/iterators.h"
#include "olap/row_block.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/segment_loader.h"

namespace doris {

class BetaRowsetReader : public RowsetReader {
public:
    BetaRowsetReader(BetaRowsetSharedPtr rowset);

    ~BetaRowsetReader() override { _rowset->release(); }

    Status init(RowsetReaderContext* read_context) override;

    Status get_segment_iterators(RowsetReaderContext* read_context,
                                 std::vector<RowwiseIterator*>* out_iters) override;
    void reset_read_options() override;

    // It's ok, because we only get ref here, the block's owner is this reader.
    Status next_block(RowBlock** block) override;
    Status next_block(vectorized::Block* block) override;
    Status next_block_view(vectorized::BlockView* block_view) override;
    bool support_return_data_by_ref() override { return _iterator->support_return_data_by_ref(); }

    bool delete_flag() override { return _rowset->delete_flag(); }

    Version version() override { return _rowset->version(); }

    int64_t oldest_write_timestamp() override { return _rowset->oldest_write_timestamp(); }
    int64_t newest_write_timestamp() override { return _rowset->newest_write_timestamp(); }

    RowsetSharedPtr rowset() override { return std::dynamic_pointer_cast<Rowset>(_rowset); }

    // Return the total number of filtered rows, will be used for validation of schema change
    int64_t filtered_rows() override {
        return _stats->rows_del_filtered + _stats->rows_del_by_bitmap +
               _stats->rows_conditions_filtered + _stats->rows_vec_del_cond_filtered +
               _stats->rows_vec_cond_filtered;
    }

    RowsetTypePB type() const override { return RowsetTypePB::BETA_ROWSET; }

    Status current_block_row_locations(std::vector<RowLocation>* locations) override {
        return _iterator->current_block_row_locations(locations);
    }

    Status get_segment_num_rows(std::vector<uint32_t>* segment_num_rows) override;

    bool update_profile(RuntimeProfile* profile) override {
        if (_iterator != nullptr) {
            return _iterator->update_profile(profile);
        }
        return false;
    }

private:
    bool _should_push_down_value_predicates() const;

    std::shared_ptr<Schema> _input_schema;
    RowsetReaderContext* _context;
    BetaRowsetSharedPtr _rowset;

    OlapReaderStatistics _owned_stats;
    OlapReaderStatistics* _stats;

    std::unique_ptr<RowwiseIterator> _iterator;

    std::shared_ptr<RowBlockV2> _input_block;
    std::unique_ptr<RowBlock> _output_block;
    std::unique_ptr<RowCursor> _row;

    // make sure this handle is initialized and valid before
    // reading data.
    SegmentCacheHandle _segment_cache_handle;

    StorageReadOptions _read_options;
    bool _can_reuse_schema = true;
};

} // namespace doris

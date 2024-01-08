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

#include <gen_cpp/olap_file.pb.h>
#include <stdint.h>

#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/iterators.h"
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/schema.h"
#include "olap/segment_loader.h"
#include "util/once.h"
#include "vec/core/block.h"

namespace doris {
class RuntimeProfile;
class Schema;
struct RowLocation;
struct RowsetReaderContext;

class BetaRowsetReader : public RowsetReader {
public:
    BetaRowsetReader(BetaRowsetSharedPtr rowset);

    ~BetaRowsetReader() override { _rowset->release(); }

    Status init(RowsetReaderContext* read_context, const RowSetSplits& rs_splits) override;

    Status get_segment_iterators(RowsetReaderContext* read_context,
                                 std::vector<RowwiseIteratorUPtr>* out_iters,
                                 bool use_cache = false) override;
    void reset_read_options() override;
    Status next_block(vectorized::Block* block) override;
    Status next_block_view(vectorized::BlockView* block_view) override;
    bool support_return_data_by_ref() override { return _is_merge_iterator(); }

    bool delete_flag() override { return _rowset->delete_flag(); }

    Version version() override { return _rowset->version(); }

    int64_t newest_write_timestamp() override { return _rowset->newest_write_timestamp(); }

    RowsetSharedPtr rowset() override { return std::dynamic_pointer_cast<Rowset>(_rowset); }

    // Return the total number of filtered rows, will be used for validation of schema change
    int64_t filtered_rows() override {
        return _stats->rows_del_filtered + _stats->rows_del_by_bitmap +
               _stats->rows_conditions_filtered + _stats->rows_vec_del_cond_filtered +
               _stats->rows_vec_cond_filtered + _stats->rows_short_circuit_cond_filtered;
    }

    RowsetTypePB type() const override { return RowsetTypePB::BETA_ROWSET; }

    Status current_block_row_locations(std::vector<RowLocation>* locations) override {
        return _iterator->current_block_row_locations(locations);
    }

    Status get_segment_num_rows(std::vector<uint32_t>* segment_num_rows) override;

    bool update_profile(RuntimeProfile* profile) override;

    RowsetReaderSharedPtr clone() override;

private:
    [[nodiscard]] Status _init_iterator_once();
    [[nodiscard]] Status _init_iterator();
    bool _should_push_down_value_predicates() const;
    bool _is_merge_iterator() const {
        return _read_context->need_ordered_result &&
               _rowset->rowset_meta()->is_segments_overlapping();
    }

    DorisCallOnce<Status> _init_iter_once;

    std::pair<int, int> _segment_offsets;
    std::vector<RowRanges> _segment_row_ranges;

    SchemaSPtr _input_schema;
    RowsetReaderContext* _read_context = nullptr;
    BetaRowsetSharedPtr _rowset;

    OlapReaderStatistics _owned_stats;
    OlapReaderStatistics* _stats = nullptr;

    std::unique_ptr<RowwiseIterator> _iterator;

    // make sure this handle is initialized and valid before
    // reading data.
    SegmentCacheHandle _segment_cache_handle;

    StorageReadOptions _read_options;

    bool _empty = false;
};

} // namespace doris

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
#include "core/block/block.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/segment/row_ranges.h"
#include "util/once.h"

namespace doris {
class RuntimeProfile;
class ReadSchema;
struct RowsetReaderContext;

class BetaRowsetReader : public RowsetReader {
public:
    BetaRowsetReader(BetaRowsetSharedPtr rowset);

    ~BetaRowsetReader() override { _rowset->release(); }

    Status init(const RowsetReaderContext* read_context, const RowSetSplits& rs_splits) override;

    Status get_segment_iterators(const RowsetReaderContext* read_context,
                                 std::vector<RowwiseIteratorUPtr>* out_iters,
                                 bool use_cache = false) override;

    Status new_iterator(const RowsetReaderContext* read_context,
                        RowwiseIteratorUPtr* out_iter) override;

    void reset_read_options() override;

    Status next_batch(Block* block) override;

    Version version() override { return _rowset->version(); }

    int64_t newest_write_timestamp() override { return _rowset->newest_write_timestamp(); }

    RowsetSharedPtr rowset() override { return std::dynamic_pointer_cast<Rowset>(_rowset); }

    const ReadSchema& read_schema() const override { return *_read_context->read_schema; }

    // Return the total number of filtered rows, will be used for validation of schema change
    int64_t filtered_rows() override {
        return _stats->rows_del_filtered + _stats->rows_del_by_bitmap +
               _stats->rows_conditions_filtered + _stats->rows_vec_del_cond_filtered +
               _stats->rows_vec_cond_filtered + _stats->rows_short_circuit_cond_filtered;
    }

    // Rows dropped by the segment merge of next_batch(): the older versions of a key.
    uint64_t merged_rows() override { return _iterator != nullptr ? _iterator->merged_rows() : 0; }

    RowsetTypePB type() const override { return RowsetTypePB::BETA_ROWSET; }

    void update_profile(RuntimeProfile* profile) override;

    RowsetReaderSharedPtr clone() override;

    OlapReaderStatistics* get_stats() { return _stats; }

private:
    // Whether new_iterator() must merge the segments: the read wants key order and the
    // segments of the read's range overlap on keys.
    bool _needs_segment_merge() const;

    bool _should_push_down_value_predicates() const;

    int64_t _get_segment_num() const {
        auto [seg_start, seg_end] = _segment_offsets;
        if (seg_start == seg_end) {
            seg_start = 0;
            seg_end = _rowset->num_segments();
        }
        return seg_end - seg_start;
    }

    DorisCallOnce<Status> _init_iter_once;

    std::pair<int64_t, int64_t> _segment_offsets;
    std::vector<RowRanges> _segment_row_ranges;

    const RowsetReaderContext* _read_context = nullptr;
    BetaRowsetSharedPtr _rowset;

    // The statistics the segment reads add to: the context's when the read supplies one (a
    // query, a compaction), these otherwise (a schema change, which checks the row counts of
    // every rowset on its own).
    OlapReaderStatistics _owned_stats;
    OlapReaderStatistics* _stats = nullptr;

    // The iterator of next_batch(), created on the first call.
    std::unique_ptr<RowwiseIterator> _iterator;

    // The read options of this rowset's segments, built by get_segment_iterators() and shared
    // by every segment iterator it creates; `_read_options` is the working copy it is built in.
    StorageReadOptions _read_options;
    std::shared_ptr<const StorageReadOptions> _segment_options;
};

} // namespace doris

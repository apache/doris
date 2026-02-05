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
    Status next_batch(vectorized::Block* block) override { return _next_batch(block); }
    Status next_batch(vectorized::BlockView* block_view) override {
        return _next_batch(block_view);
    }
    Status next_batch(BlockWithSameBit* block_with_same_bit) override {
        return _next_batch(block_with_same_bit);
    }

    bool is_merge_iterator() const override {
        return _read_context->need_ordered_result &&
               _rowset->rowset_meta()->is_segments_overlapping() && _get_segment_num() > 1;
    }

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

    uint64_t merged_rows() override { return *(_read_context->merged_rows); }

    RowsetTypePB type() const override { return RowsetTypePB::BETA_ROWSET; }

    Status current_block_row_locations(std::vector<RowLocation>* locations) override {
        return _iterator->current_block_row_locations(locations);
    }

    void update_profile(RuntimeProfile* profile) override;

    RowsetReaderSharedPtr clone() override;

    void set_topn_limit(size_t topn_limit) override { _topn_limit = topn_limit; }

    OlapReaderStatistics* get_stats() { return _stats; }

private:
    template <typename T>
    Status _next_batch(T* block) {
        RETURN_IF_ERROR(_init_iterator_once());
        SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
        if (_empty) {
            return Status::Error<ErrorCode::END_OF_FILE>("BetaRowsetReader is empty");
        }

        RuntimeState* runtime_state = nullptr;
        if (_read_context != nullptr) {
            runtime_state = _read_context->runtime_state;
        }

        do {
            Status s = _iterator->next_batch(block);
            if (!s.ok()) {
                if (!s.is<ErrorCode::END_OF_FILE>()) {
                    LOG(WARNING) << "failed to read next block: " << s.to_string();
                }
                return s;
            }

            if (runtime_state != nullptr && runtime_state->is_cancelled()) [[unlikely]] {
                return runtime_state->cancel_reason();
            }
        } while (block->empty());

        return Status::OK();
    }

    [[nodiscard]] Status _init_iterator_once();
    [[nodiscard]] Status _init_iterator();
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

    SchemaSPtr _input_schema;
    RowsetReaderContext* _read_context = nullptr;
    BetaRowsetSharedPtr _rowset;

    OlapReaderStatistics _owned_stats;
    OlapReaderStatistics* _stats = nullptr;

    std::unique_ptr<RowwiseIterator> _iterator;

    StorageReadOptions _read_options;

    bool _empty = false;
    size_t _topn_limit = 0;
    uint64_t _merged_rows = 0;
};

} // namespace doris

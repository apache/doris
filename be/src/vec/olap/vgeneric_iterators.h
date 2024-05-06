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

#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <list>
#include <map>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "olap/iterators.h"
#include "olap/schema.h"
#include "olap/utils.h"
#include "vec/core/block.h"

namespace doris {
class RuntimeProfile;

namespace segment_v2 {
class Segment;
class ColumnIterator;
} // namespace segment_v2

namespace vectorized {

class VStatisticsIterator : public RowwiseIterator {
public:
    // Will generate num_rows rows in total
    VStatisticsIterator(std::shared_ptr<Segment> segment, const Schema& schema)
            : _segment(std::move(segment)), _schema(schema) {}

    ~VStatisticsIterator() override = default;

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override;

    const Schema& schema() const override { return _schema; }

private:
    std::shared_ptr<Segment> _segment;
    const Schema& _schema;
    size_t _target_rows = 0;
    size_t _output_rows = 0;
    bool _init = false;
    TPushAggOp::type _push_down_agg_type_opt;
    std::map<int32_t, std::unique_ptr<ColumnIterator>> _column_iterators_map;
    std::vector<ColumnIterator*> _column_iterators;

    static constexpr size_t MAX_ROW_SIZE_IN_COUNT = 65535;
};

// Used to store merge state for a VMergeIterator input.
// This class will iterate all data from internal iterator
// through client call advance().
// Usage:
//      VMergeIteratorContext ctx(iter);
//      RETURN_IF_ERROR(ctx.init());
//      while (ctx.valid()) {
//          visit(ctx.current_row());
//          RETURN_IF_ERROR(ctx.advance());
//      }
class VMergeIteratorContext {
public:
    VMergeIteratorContext(RowwiseIteratorUPtr&& iter, int sequence_id_idx, bool is_unique,
                          bool is_reverse, std::vector<uint32_t>* read_orderby_key_columns)
            : _iter(std::move(iter)),
              _sequence_id_idx(sequence_id_idx),
              _is_unique(is_unique),
              _is_reverse(is_reverse),
              _num_columns(_iter->schema().num_column_ids()),
              _num_key_columns(_iter->schema().num_key_columns()),
              _compare_columns(read_orderby_key_columns) {}

    VMergeIteratorContext(const VMergeIteratorContext&) = delete;
    VMergeIteratorContext(VMergeIteratorContext&&) = delete;
    VMergeIteratorContext& operator=(const VMergeIteratorContext&) = delete;
    VMergeIteratorContext& operator=(VMergeIteratorContext&&) = delete;

    ~VMergeIteratorContext() = default;

    Status block_reset(const std::shared_ptr<Block>& block);

    // Initialize this context and will prepare data for current_row()
    Status init(const StorageReadOptions& opts);

    bool compare(const VMergeIteratorContext& rhs) const;

    // `advanced = false` when current block finished
    Status copy_rows(Block* block, bool advanced = true);

    Status copy_rows(BlockView* view, bool advanced = true);

    RowLocation current_row_location() {
        DCHECK(_record_rowids);
        return _block_row_locations[_index_in_block];
    }

    // Advance internal row index to next valid row
    // Return error if error happens
    // Don't call this when valid() is false, action is undefined
    Status advance();

    // Return if it has remaining data in this context.
    // Only when this function return true, current_row()
    // will return a valid row
    bool valid() const { return _valid; }

    uint64_t data_id() const { return _iter->data_id(); }

    bool need_skip() const { return _skip; }

    void set_skip(bool skip) const { _skip = skip; }

    bool is_same() const { return _same; }

    void set_same(bool same) const { _same = same; }

    const std::vector<bool>& get_pre_ctx_same() const { return _pre_ctx_same_bit; }

    void set_pre_ctx_same(VMergeIteratorContext* ctx) const {
        int64_t index = ctx->get_cur_batch() - 1;
        DCHECK(index >= 0);
        DCHECK_LT(index, _pre_ctx_same_bit.size());
        _pre_ctx_same_bit[index] = ctx->is_same();
    }

    size_t get_cur_batch() const { return _cur_batch_num; }

    void add_cur_batch() { _cur_batch_num++; }

    void reset_cur_batch() { _cur_batch_num = 0; }

    bool is_cur_block_finished() { return _index_in_block == _block->rows() - 1; }

private:
    // Load next block into _block
    Status _load_next_block();

    RowwiseIteratorUPtr _iter;

    int _sequence_id_idx = -1;
    bool _is_unique = false;
    bool _is_reverse = false;
    bool _valid = false;
    mutable bool _skip = false;
    mutable bool _same = false;
    size_t _index_in_block = -1;
    // 4096 minus 16 + 16 bytes padding that in padding pod array
    int _block_row_max = 4064;
    int _num_columns;
    int _num_key_columns;
    std::vector<uint32_t>* _compare_columns;
    std::vector<RowLocation> _block_row_locations;
    bool _record_rowids = false;
    size_t _cur_batch_num = 0;

    // used to store data load from iterator->next_batch(Block*)
    std::shared_ptr<Block> _block;
    // used to store data still on block view
    std::list<std::shared_ptr<Block>> _block_list;
    mutable std::vector<bool> _pre_ctx_same_bit;
};

class VMergeIterator : public RowwiseIterator {
public:
    // VMergeIterator takes the ownership of input iterators
    VMergeIterator(std::vector<RowwiseIteratorUPtr>&& iters, int sequence_id_idx, bool is_unique,
                   bool is_reverse, uint64_t* merged_rows)
            : _origin_iters(std::move(iters)),
              _sequence_id_idx(sequence_id_idx),
              _is_unique(is_unique),
              _is_reverse(is_reverse),
              _merged_rows(merged_rows) {}

    ~VMergeIterator() override = default;

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override { return _next_batch(block); }
    Status next_block_view(BlockView* block_view) override { return _next_batch(block_view); }

    const Schema& schema() const override { return *_schema; }

    Status current_block_row_locations(std::vector<RowLocation>* block_row_locations) override {
        DCHECK(_record_rowids);
        *block_row_locations = _block_row_locations;
        return Status::OK();
    }

    bool update_profile(RuntimeProfile* profile) override {
        if (!_origin_iters.empty()) {
            return _origin_iters[0]->update_profile(profile);
        }
        return false;
    }

private:
    int _get_size(Block* block) { return block->rows(); }
    int _get_size(BlockView* block_view) { return block_view->size(); }

    template <typename T>
    Status _next_batch(T* block) {
        if (UNLIKELY(_record_rowids)) {
            _block_row_locations.resize(_block_row_max);
        }
        size_t row_idx = 0;
        std::shared_ptr<VMergeIteratorContext> pre_ctx;
        while (_get_size(block) < _block_row_max) {
            if (_merge_heap.empty()) {
                break;
            }

            auto ctx = _merge_heap.top();
            _merge_heap.pop();

            if (!ctx->need_skip()) {
                ctx->add_cur_batch();
                if (pre_ctx != ctx) {
                    if (pre_ctx) {
                        RETURN_IF_ERROR(pre_ctx->copy_rows(block));
                    }
                    pre_ctx = ctx;
                }
                pre_ctx->set_pre_ctx_same(ctx.get());
                if (UNLIKELY(_record_rowids)) {
                    _block_row_locations[row_idx] = ctx->current_row_location();
                }
                row_idx++;
                if (ctx->is_cur_block_finished() || row_idx >= _block_row_max) {
                    // current block finished, ctx not advance
                    // so copy start_idx = (_index_in_block - _cur_batch_num + 1)
                    RETURN_IF_ERROR(ctx->copy_rows(block, false));
                    pre_ctx = nullptr;
                }
            } else if (_merged_rows != nullptr) {
                (*_merged_rows)++;
                // need skip cur row, so flush rows in pre_ctx
                if (pre_ctx) {
                    RETURN_IF_ERROR(pre_ctx->copy_rows(block));
                    pre_ctx = nullptr;
                }
            }

            RETURN_IF_ERROR(ctx->advance());
            if (ctx->valid()) {
                _merge_heap.push(ctx);
            }
        }
        if (!_merge_heap.empty()) {
            return Status::OK();
        }
        // Still last batch needs to be processed

        if (UNLIKELY(_record_rowids)) {
            _block_row_locations.resize(row_idx);
        }

        return Status::EndOfFile("no more data in segment");
    }

    // It will be released after '_merge_heap' has been built.
    std::vector<RowwiseIteratorUPtr> _origin_iters;

    const Schema* _schema = nullptr;

    struct VMergeContextComparator {
        bool operator()(const std::shared_ptr<VMergeIteratorContext>& lhs,
                        const std::shared_ptr<VMergeIteratorContext>& rhs) const {
            return lhs->compare(*rhs);
        }
    };

    using VMergeHeap = std::priority_queue<std::shared_ptr<VMergeIteratorContext>,
                                           std::vector<std::shared_ptr<VMergeIteratorContext>>,
                                           VMergeContextComparator>;

    VMergeHeap _merge_heap;

    int _block_row_max = 0;
    int _sequence_id_idx = -1;
    bool _is_unique = false;
    bool _is_reverse = false;
    uint64_t* _merged_rows = nullptr;
    bool _record_rowids = false;
    std::vector<RowLocation> _block_row_locations;
};

// Create a merge iterator for input iterators. Merge iterator will merge
// ordered input iterator to one ordered iterator. So client should ensure
// that every input iterator is ordered, otherwise result is undefined.
//
// Inputs iterators' ownership is taken by created merge iterator. And client
// should delete returned iterator after usage.
RowwiseIteratorUPtr new_merge_iterator(std::vector<RowwiseIteratorUPtr>&& inputs,
                                       int sequence_id_idx, bool is_unique, bool is_reverse,
                                       uint64_t* merged_rows);

// Create a union iterator for input iterators. Union iterator will read
// input iterators one by one.
//
// Inputs iterators' ownership is taken by created union iterator.
RowwiseIteratorUPtr new_union_iterator(std::vector<RowwiseIteratorUPtr>&& inputs);

// Create an auto increment iterator which returns num_rows data in format of schema.
// This class aims to be used in unit test.
//
// Client should delete returned iterator.
RowwiseIteratorUPtr new_auto_increment_iterator(const Schema& schema, size_t num_rows);

RowwiseIterator* new_vstatistics_iterator(std::shared_ptr<Segment> segment, const Schema& schema);

} // namespace vectorized

} // namespace doris

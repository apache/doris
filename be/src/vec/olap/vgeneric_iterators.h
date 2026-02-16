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

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "olap/iterators.h"
#include "olap/rowset/segment_v2/column_reader.h"
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
#include "common/compile_check_begin.h"

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
                          bool is_reverse, std::vector<uint32_t>* read_orderby_key_columns,
                          SchemaSPtr output_schema)
            : _iter(std::move(iter)),
              _sequence_id_idx(sequence_id_idx),
              _is_unique(is_unique),
              _is_reverse(is_reverse),
              _output_schema(std::move(output_schema)),
              _num_key_columns(cast_set<int>(_output_schema->num_key_columns())),
              _compare_columns(read_orderby_key_columns) {}

    VMergeIteratorContext(const VMergeIteratorContext&) = delete;
    VMergeIteratorContext(VMergeIteratorContext&&) = delete;
    VMergeIteratorContext& operator=(const VMergeIteratorContext&) = delete;
    VMergeIteratorContext& operator=(VMergeIteratorContext&&) = delete;

    ~VMergeIteratorContext() = default;

    // Reset (or initialize) the internal _block using the output schema.
    //
    // The output schema contains only the columns the caller requested (return_columns),
    // excluding delete predicate columns. For example, if the query reads columns {c1, c2}
    // but there is a delete predicate on column c3 (e.g., "DELETE FROM t WHERE c3 = 'foo'"):
    //   - input schema  (iter->schema) = {c1, c2, c3}   (3 columns)
    //   - output schema                = {c1, c2}       (2 columns)
    //
    // It is safe to build the block with only the output schema because SegmentIterator
    // handles delete predicate columns independently of the block structure:
    //   - _init_current_block() skips predicate columns (including delete predicates)
    //     via the _is_pred_column[cid] check, never accessing the block for them.
    //   - _output_non_pred_columns() checks loc < block->columns() before filling any
    //     column, so delete predicate columns are simply skipped when the block is smaller.
    //   - Delete predicate evaluation uses _current_return_columns and
    //     _evaluate_short_circuit_predicate(), independent of the block.
    Status block_reset(const std::shared_ptr<Block>& block);

    // Initialize this context and will prepare data for current_row()
    Status init(const StorageReadOptions& opts);

    bool compare(const VMergeIteratorContext& rhs) const;

    // Copy rows from internal _block to the destination block.
    // Both blocks have _output_schema columns (return_columns only).
    // Only _output_schema->num_column_ids() columns are copied.
    //
    // `advanced = false` when current block finished
    // when input argument type is block, we do not process same_bit,
    // this case we only need merge and return ordered data (VCollectIterator::_topn_next), data mode is dup/mow can guarantee all rows are different
    // todo: we can reduce same_bit processing in this case to improve performance
    Status copy_rows(Block* block, bool advanced = true);
    Status copy_rows(BlockWithSameBit* block, bool advanced = true);
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
    // The output schema defines which columns are in _block and in the caller's dst block.
    // It contains only the requested return_columns, excluding delete predicate columns.
    // For example:
    //   - _iter->schema() (input)  = {c1, c2, c3}  — c3 for "DELETE WHERE c3='foo'"
    //   - _output_schema           = {c1, c2}      — only the requested columns
    // block_reset() uses _output_schema to build _block, and copy_rows() iterates over
    // _output_schema->num_column_ids() columns to copy from _block to the destination.
    const SchemaSPtr _output_schema;
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
                   bool is_reverse, uint64_t* merged_rows, SchemaSPtr output_schema)
            : _origin_iters(std::move(iters)),
              _output_schema(std::move(output_schema)),
              _sequence_id_idx(sequence_id_idx),
              _is_unique(is_unique),
              _is_reverse(is_reverse),
              _merged_rows(merged_rows) {}

    ~VMergeIterator() override = default;

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override { return _next_batch(block); }
    Status next_batch(BlockWithSameBit* block_with_same_bit) override {
        return _next_batch(block_with_same_bit);
    }
    Status next_batch(BlockView* block_view) override { return _next_batch(block_view); }

    const Schema& schema() const override { return *_output_schema; }

    Status current_block_row_locations(std::vector<RowLocation>* block_row_locations) override {
        DCHECK(_record_rowids);
        *block_row_locations = _block_row_locations;
        return Status::OK();
    }

    void update_profile(RuntimeProfile* profile) override {
        if (!_origin_iters.empty()) {
            _origin_iters[0]->update_profile(profile);
        }
    }

private:
    int _get_size(const BlockWithSameBit* block_with_same_bit) {
        return cast_set<int>(block_with_same_bit->block->rows());
    }
    int _get_size(BlockView* block_view) { return cast_set<int>(block_view->size()); }
    int _get_size(Block* block) { return cast_set<int>(block->rows()); }

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

    // The output schema (excludes delete predicate columns). Passed down to each
    // VMergeIteratorContext to control how many columns copy_rows() copies.
    const SchemaSPtr _output_schema;

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
                                       uint64_t* merged_rows, SchemaSPtr output_schema);

// Create a union iterator for input iterators. Union iterator will read
// input iterators one by one.
//
// Inputs iterators' ownership is taken by created union iterator.
RowwiseIteratorUPtr new_union_iterator(std::vector<RowwiseIteratorUPtr>&& inputs,
                                       SchemaSPtr output_schema);

// Create an auto increment iterator which returns num_rows data in format of schema.
// This class aims to be used in unit test.
//
// Client should delete returned iterator.
RowwiseIteratorUPtr new_auto_increment_iterator(const Schema& schema, size_t num_rows);

RowwiseIterator* new_vstatistics_iterator(std::shared_ptr<Segment> segment, const Schema& schema);

#include "common/compile_check_end.h"
} // namespace vectorized

} // namespace doris

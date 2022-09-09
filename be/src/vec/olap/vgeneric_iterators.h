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

#include "olap/iterators.h"
#include "olap/row.h"
#include "olap/row_block2.h"

namespace doris {

namespace vectorized {

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
    VMergeIteratorContext(RowwiseIterator* iter, int sequence_id_idx, bool is_unique,
                          bool is_reverse, std::vector<uint32_t>* read_orderby_key_columns)
            : _iter(iter),
              _sequence_id_idx(sequence_id_idx),
              _is_unique(is_unique),
              _is_reverse(is_reverse),
              _num_columns(iter->schema().num_column_ids()),
              _num_key_columns(iter->schema().num_key_columns()),
              _compare_columns(read_orderby_key_columns) {}

    VMergeIteratorContext(const VMergeIteratorContext&) = delete;
    VMergeIteratorContext(VMergeIteratorContext&&) = delete;
    VMergeIteratorContext& operator=(const VMergeIteratorContext&) = delete;
    VMergeIteratorContext& operator=(VMergeIteratorContext&&) = delete;

    ~VMergeIteratorContext() {
        delete _iter;
        _iter = nullptr;
    }

    Status block_reset(const std::shared_ptr<Block>& block) {
        if (!*block) {
            const Schema& schema = _iter->schema();
            const auto& column_ids = schema.column_ids();
            for (size_t i = 0; i < schema.num_column_ids(); ++i) {
                auto column_desc = schema.column(column_ids[i]);
                auto data_type = Schema::get_data_type_ptr(*column_desc);
                if (data_type == nullptr) {
                    return Status::RuntimeError("invalid data type");
                }
                auto column = data_type->create_column();
                column->reserve(_block_row_max);
                block->insert(
                        ColumnWithTypeAndName(std::move(column), data_type, column_desc->name()));
            }
        } else {
            block->clear_column_data();
        }
        return Status::OK();
    }

    // Initialize this context and will prepare data for current_row()
    Status init(const StorageReadOptions& opts);

    bool compare(const VMergeIteratorContext& rhs) const {
        int cmp_res = UNLIKELY(_compare_columns)
                              ? _block->compare_at(_index_in_block, rhs._index_in_block,
                                                   _compare_columns, *rhs._block, -1)
                              : _block->compare_at(_index_in_block, rhs._index_in_block,
                                                   _num_key_columns, *rhs._block, -1);

        if (cmp_res != 0) {
            return UNLIKELY(_is_reverse) ? cmp_res < 0 : cmp_res > 0;
        }

        auto col_cmp_res = 0;
        if (_sequence_id_idx != -1) {
            col_cmp_res = _block->compare_column_at(_index_in_block, rhs._index_in_block,
                                                    _sequence_id_idx, *rhs._block, -1);
        }
        auto result = col_cmp_res == 0 ? data_id() < rhs.data_id() : col_cmp_res < 0;

        if (_is_unique) {
            result ? set_skip(true) : rhs.set_skip(true);
        }
        return result;
    }

    // `advanced = false` when current block finished
    void copy_rows(Block* block, bool advanced = true) {
        Block& src = *_block;
        Block& dst = *block;
        if (_cur_batch_num == 0) {
            return;
        }

        // copy a row to dst block column by column
        size_t start = _index_in_block - _cur_batch_num + 1 - advanced;
        DCHECK(start >= 0);

        for (size_t i = 0; i < _num_columns; ++i) {
            auto& s_col = src.get_by_position(i);
            auto& d_col = dst.get_by_position(i);

            ColumnPtr& s_cp = s_col.column;
            ColumnPtr& d_cp = d_col.column;

            d_cp->assume_mutable()->insert_range_from(*s_cp, start, _cur_batch_num);
        }
        _cur_batch_num = 0;
    }

    void copy_rows(BlockView* view, bool advanced = true) {
        if (_cur_batch_num == 0) {
            return;
        }
        size_t start = _index_in_block - _cur_batch_num + 1 - advanced;
        DCHECK(start >= 0);

        for (size_t i = 0; i < _cur_batch_num; ++i) {
            view->push_back({_block, static_cast<int>(start + i), false});
        }

        _cur_batch_num = 0;
    }

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

    void add_cur_batch() { _cur_batch_num++; }

    void reset_cur_batch() { _cur_batch_num = 0; }

    bool is_cur_block_finished() { return _index_in_block == _block->rows() - 1; }

private:
    // Load next block into _block
    Status _load_next_block();

    RowwiseIterator* _iter;

    int _sequence_id_idx = -1;
    bool _is_unique = false;
    bool _is_reverse = false;
    bool _valid = false;
    mutable bool _skip = false;
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
};

class VMergeIterator : public RowwiseIterator {
public:
    // VMergeIterator takes the ownership of input iterators
    VMergeIterator(std::vector<RowwiseIterator*>& iters, int sequence_id_idx, bool is_unique,
                   bool is_reverse, uint64_t* merged_rows)
            : _origin_iters(iters),
              _sequence_id_idx(sequence_id_idx),
              _is_unique(is_unique),
              _is_reverse(is_reverse),
              _merged_rows(merged_rows) {}

    ~VMergeIterator() override {
        while (!_merge_heap.empty()) {
            auto ctx = _merge_heap.top();
            _merge_heap.pop();
            delete ctx;
        }
    }

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override { return _next_batch(block); }
    Status next_block_view(BlockView* block_view) override { return _next_batch(block_view); }

    bool support_return_data_by_ref() override { return true; }

    const Schema& schema() const override { return *_schema; }

    Status current_block_row_locations(std::vector<RowLocation>* block_row_locations) override {
        DCHECK(_record_rowids);
        *block_row_locations = _block_row_locations;
        return Status::OK();
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
        VMergeIteratorContext* pre_ctx = nullptr;
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
                        pre_ctx->copy_rows(block);
                    }
                    pre_ctx = ctx;
                }
                if (UNLIKELY(_record_rowids)) {
                    _block_row_locations[row_idx] = ctx->current_row_location();
                }
                row_idx++;
                if (ctx->is_cur_block_finished() || row_idx >= _block_row_max) {
                    // current block finished, ctx not advance
                    // so copy start_idx = (_index_in_block - _cur_batch_num + 1)
                    ctx->copy_rows(block, false);
                    pre_ctx = nullptr;
                }
            } else if (_merged_rows != nullptr) {
                (*_merged_rows)++;
                // need skip cur row, so flush rows in pre_ctx
                if (pre_ctx) {
                    pre_ctx->copy_rows(block);
                    pre_ctx = nullptr;
                }
            }

            RETURN_IF_ERROR(ctx->advance());
            if (ctx->valid()) {
                _merge_heap.push(ctx);
            } else {
                // Release ctx earlier to reduce resource consumed
                delete ctx;
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
    std::vector<RowwiseIterator*> _origin_iters;

    const Schema* _schema = nullptr;

    struct VMergeContextComparator {
        bool operator()(const VMergeIteratorContext* lhs, const VMergeIteratorContext* rhs) const {
            return lhs->compare(*rhs);
        }
    };

    using VMergeHeap =
            std::priority_queue<VMergeIteratorContext*, std::vector<VMergeIteratorContext*>,
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
RowwiseIterator* new_merge_iterator(std::vector<RowwiseIterator*>& inputs, int sequence_id_idx,
                                    bool is_unique, bool is_reverse, uint64_t* merged_rows);

// Create a union iterator for input iterators. Union iterator will read
// input iterators one by one.
//
// Inputs iterators' ownership is taken by created union iterator. And client
// should delete returned iterator after usage.
RowwiseIterator* new_union_iterator(std::vector<RowwiseIterator*>& inputs);

// Create an auto increment iterator which returns num_rows data in format of schema.
// This class aims to be used in unit test.
//
// Client should delete returned iterator.
RowwiseIterator* new_auto_increment_iterator(const Schema& schema, size_t num_rows);

} // namespace vectorized

} // namespace doris

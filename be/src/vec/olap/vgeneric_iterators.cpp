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

#include <queue>
#include <utility>

#include "olap/iterators.h"
#include "olap/row.h"
#include "olap/row_block2.h"

namespace doris {

namespace vectorized {

// This iterator will generate ordered data. For example for schema
// (int, int) this iterator will generator data like
// (0, 1), (1, 2), (2, 3), (3, 4)...
//
// Usage:
//      Schema schema;
//      VAutoIncrementIterator iter(schema, 1000);
//      StorageReadOptions opts;
//      RETURN_IF_ERROR(iter.init(opts));
//      RowBlockV2 block;
//      do {
//          st = iter.next_batch(&block);
//      } while (st.ok());
class VAutoIncrementIterator : public RowwiseIterator {
public:
    // Will generate num_rows rows in total
    VAutoIncrementIterator(const Schema& schema, size_t num_rows)
            : _schema(schema), _num_rows(num_rows), _rows_returned() {}
    ~VAutoIncrementIterator() override {}

    // NOTE: Currently, this function will ignore StorageReadOptions
    Status init(const StorageReadOptions& opts) override;

    Status next_batch(vectorized::Block* block) override {
        int row_idx = 0;
        while (_rows_returned < _num_rows) {
            for (int j = 0; j < _schema.num_columns(); ++j) {
                vectorized::ColumnWithTypeAndName& vc = block->get_by_position(j);
                vectorized::IColumn& vi = (vectorized::IColumn&)(*vc.column);

                char data[16] = {};
                size_t data_len = 0;
                const auto* col_schema = _schema.column(j);
                switch (col_schema->type()) {
                case OLAP_FIELD_TYPE_SMALLINT:
                    *(int16_t*)data = _rows_returned + j;
                    data_len = sizeof(int16_t);
                    break;
                case OLAP_FIELD_TYPE_INT:
                    *(int32_t*)data = _rows_returned + j;
                    data_len = sizeof(int32_t);
                    break;
                case OLAP_FIELD_TYPE_BIGINT:
                    *(int64_t*)data = _rows_returned + j;
                    data_len = sizeof(int64_t);
                    break;
                case OLAP_FIELD_TYPE_FLOAT:
                    *(float*)data = _rows_returned + j;
                    data_len = sizeof(float);
                    break;
                case OLAP_FIELD_TYPE_DOUBLE:
                    *(double*)data = _rows_returned + j;
                    data_len = sizeof(double);
                    break;
                default:
                    break;
                }

                vi.insert_data(data, data_len);
            }

            ++row_idx;
            ++_rows_returned;
        }

        if (row_idx > 0) return Status::OK();
        return Status::EndOfFile("End of VAutoIncrementIterator");
    }

    const Schema& schema() const override { return _schema; }

private:
    const Schema& _schema;
    size_t _num_rows;
    size_t _rows_returned;
};

Status VAutoIncrementIterator::init(const StorageReadOptions& opts) {
    return Status::OK();
}

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

    Status block_reset() {
        if (!_block) {
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
                _block.insert(
                        ColumnWithTypeAndName(std::move(column), data_type, column_desc->name()));
            }
        } else {
            _block.clear_column_data();
        }
        return Status::OK();
    }

    // Initialize this context and will prepare data for current_row()
    Status init(const StorageReadOptions& opts);

    bool compare(const VMergeIteratorContext& rhs) const {
        int cmp_res = UNLIKELY(_compare_columns)
                              ? this->_block.compare_at(_index_in_block, rhs._index_in_block,
                                                        _compare_columns, rhs._block, -1)
                              : this->_block.compare_at(_index_in_block, rhs._index_in_block,
                                                        _num_key_columns, rhs._block, -1);

        if (cmp_res != 0) {
            return UNLIKELY(_is_reverse) ? cmp_res < 0 : cmp_res > 0;
        }

        auto col_cmp_res = 0;
        if (_sequence_id_idx != -1) {
            col_cmp_res = this->_block.compare_column_at(_index_in_block, rhs._index_in_block,
                                                         _sequence_id_idx, rhs._block, -1);
        }
        auto result = col_cmp_res == 0 ? this->data_id() < rhs.data_id() : col_cmp_res < 0;

        if (_is_unique) {
            result ? this->set_skip(true) : rhs.set_skip(true);
        }
        return result;
    }

    void copy_row(vectorized::Block* block) {
        vectorized::Block& src = _block;
        vectorized::Block& dst = *block;

        for (size_t i = 0; i < _num_columns; ++i) {
            auto& s_col = src.get_by_position(i);
            auto& d_col = dst.get_by_position(i);

            vectorized::ColumnPtr& s_cp = s_col.column;
            vectorized::ColumnPtr& d_cp = d_col.column;

            //copy a row to dst block column by column
            ((vectorized::IColumn&)(*d_cp)).insert_from(*s_cp, _index_in_block);
        }
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

private:
    // Load next block into _block
    Status _load_next_block();

    RowwiseIterator* _iter;

    // used to store data load from iterator->next_batch(Vectorized::Block*)
    vectorized::Block _block;

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
};

Status VMergeIteratorContext::init(const StorageReadOptions& opts) {
    _block_row_max = opts.block_row_max;
    _record_rowids = opts.record_rowids;
    RETURN_IF_ERROR(_iter->init(opts));
    RETURN_IF_ERROR(block_reset());
    RETURN_IF_ERROR(_load_next_block());
    if (valid()) {
        RETURN_IF_ERROR(advance());
    }
    return Status::OK();
}

Status VMergeIteratorContext::advance() {
    _skip = false;
    // NOTE: we increase _index_in_block directly to valid one check
    do {
        _index_in_block++;
        if (LIKELY(_index_in_block < _block.rows())) {
            return Status::OK();
        }
        // current batch has no data, load next batch
        RETURN_IF_ERROR(_load_next_block());
    } while (_valid);
    return Status::OK();
}

Status VMergeIteratorContext::_load_next_block() {
    do {
        block_reset();
        Status st = _iter->next_batch(&_block);
        if (!st.ok()) {
            _valid = false;
            if (st.is_end_of_file()) {
                return Status::OK();
            } else {
                return st;
            }
        }
        if (UNLIKELY(_record_rowids)) {
            RETURN_IF_ERROR(_iter->current_block_row_locations(&_block_row_locations));
        }
    } while (_block.rows() == 0);
    _index_in_block = -1;
    _valid = true;
    return Status::OK();
}

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

    Status next_batch(vectorized::Block* block) override;

    const Schema& schema() const override { return *_schema; }

    Status current_block_row_locations(std::vector<RowLocation>* block_row_locations) override {
        DCHECK(_record_rowids);
        *block_row_locations = _block_row_locations;
        return Status::OK();
    }

private:
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

    int block_row_max = 0;
    int _sequence_id_idx = -1;
    bool _is_unique = false;
    bool _is_reverse = false;
    uint64_t* _merged_rows = nullptr;
    bool _record_rowids = false;
    std::vector<RowLocation> _block_row_locations;
};

Status VMergeIterator::init(const StorageReadOptions& opts) {
    if (_origin_iters.empty()) {
        return Status::OK();
    }
    _schema = &(*_origin_iters.begin())->schema();
    _record_rowids = opts.record_rowids;

    for (auto iter : _origin_iters) {
        auto ctx = std::make_unique<VMergeIteratorContext>(
                iter, _sequence_id_idx, _is_unique, _is_reverse, opts.read_orderby_key_columns);
        RETURN_IF_ERROR(ctx->init(opts));
        if (!ctx->valid()) {
            continue;
        }
        _merge_heap.push(ctx.release());
    }

    _origin_iters.clear();

    block_row_max = opts.block_row_max;

    return Status::OK();
}

Status VMergeIterator::next_batch(vectorized::Block* block) {
    if (UNLIKELY(_record_rowids)) {
        _block_row_locations.resize(block_row_max);
    }
    size_t row_idx = 0;
    while (block->rows() < block_row_max) {
        if (_merge_heap.empty()) break;

        auto ctx = _merge_heap.top();
        _merge_heap.pop();

        if (!ctx->need_skip()) {
            // copy current row to block
            ctx->copy_row(block);
            if (UNLIKELY(_record_rowids)) {
                _block_row_locations[row_idx] = ctx->current_row_location();
            }
            row_idx++;
        } else if (_merged_rows != nullptr) {
            (*_merged_rows)++;
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

// VUnionIterator will read data from input iterator one by one.
class VUnionIterator : public RowwiseIterator {
public:
    // Iterators' ownership it transferred to this class.
    // This class will delete all iterators when destructs
    // Client should not use iterators anymore.
    VUnionIterator(std::vector<RowwiseIterator*>& v) : _origin_iters(v.begin(), v.end()) {}

    ~VUnionIterator() override {
        std::for_each(_origin_iters.begin(), _origin_iters.end(),
                      std::default_delete<RowwiseIterator>());
    }

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(vectorized::Block* block) override;

    const Schema& schema() const override { return *_schema; }

    Status current_block_row_locations(std::vector<RowLocation>* locations) override;

private:
    const Schema* _schema = nullptr;
    RowwiseIterator* _cur_iter = nullptr;
    std::deque<RowwiseIterator*> _origin_iters;
};

Status VUnionIterator::init(const StorageReadOptions& opts) {
    if (_origin_iters.empty()) {
        return Status::OK();
    }

    for (auto iter : _origin_iters) {
        RETURN_IF_ERROR(iter->init(opts));
    }
    _cur_iter = *(_origin_iters.begin());
    _schema = &_cur_iter->schema();
    return Status::OK();
}

Status VUnionIterator::next_batch(vectorized::Block* block) {
    while (_cur_iter != nullptr) {
        auto st = _cur_iter->next_batch(block);
        if (st.is_end_of_file()) {
            delete _cur_iter;
            _origin_iters.pop_front();
            if (!_origin_iters.empty()) {
                _cur_iter = *(_origin_iters.begin());
            } else {
                _cur_iter = nullptr;
            }
        } else {
            return st;
        }
    }
    return Status::EndOfFile("End of VUnionIterator");
}

Status VUnionIterator::current_block_row_locations(std::vector<RowLocation>* locations) {
    if (!_cur_iter) {
        locations->clear();
        return Status::EndOfFile("End of VUnionIterator");
    }
    return _cur_iter->current_block_row_locations(locations);
}

RowwiseIterator* new_merge_iterator(std::vector<RowwiseIterator*>& inputs, int sequence_id_idx,
                                    bool is_unique, bool is_reverse, uint64_t* merged_rows) {
    if (inputs.size() == 1) {
        return *(inputs.begin());
    }
    return new VMergeIterator(inputs, sequence_id_idx, is_unique, is_reverse, merged_rows);
}

RowwiseIterator* new_union_iterator(std::vector<RowwiseIterator*>& inputs) {
    if (inputs.size() == 1) {
        return *(inputs.begin());
    }
    return new VUnionIterator(inputs);
}

RowwiseIterator* new_auto_increment_iterator(const Schema& schema, size_t num_rows) {
    return new VAutoIncrementIterator(schema, num_rows);
}

} // namespace vectorized

} // namespace doris

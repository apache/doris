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

#include "vec/olap/vgeneric_iterators.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "common/status.h"
#include "olap/field.h"
#include "olap/iterators.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/schema_cache.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"

namespace doris {
class RuntimeProfile;

using namespace ErrorCode;

namespace vectorized {

Status VStatisticsIterator::init(const StorageReadOptions& opts) {
    if (!_init) {
        _push_down_agg_type_opt = opts.push_down_agg_type_opt;

        for (size_t i = 0; i < _schema.num_column_ids(); i++) {
            auto cid = _schema.column_id(i);
            auto unique_id = _schema.column(cid)->unique_id();
            if (_column_iterators_map.count(unique_id) < 1) {
                RETURN_IF_ERROR(_segment->new_column_iterator(opts.tablet_schema->column(cid),
                                                              &_column_iterators_map[unique_id]));
            }
            _column_iterators.push_back(_column_iterators_map[unique_id].get());
        }

        _target_rows = _push_down_agg_type_opt == TPushAggOp::MINMAX ? 2 : _segment->num_rows();
        _init = true;
    }

    return Status::OK();
}

Status VStatisticsIterator::next_batch(Block* block) {
    DCHECK(block->columns() == _column_iterators.size());
    if (_output_rows < _target_rows) {
        block->clear_column_data();
        auto columns = block->mutate_columns();

        size_t size = _push_down_agg_type_opt == TPushAggOp::MINMAX
                              ? 2
                              : std::min(_target_rows - _output_rows, MAX_ROW_SIZE_IN_COUNT);
        if (_push_down_agg_type_opt == TPushAggOp::COUNT) {
            size = std::min(_target_rows - _output_rows, MAX_ROW_SIZE_IN_COUNT);
            for (int i = 0; i < block->columns(); ++i) {
                columns[i]->resize(size);
            }
        } else {
            for (int i = 0; i < block->columns(); ++i) {
                static_cast<void>(_column_iterators[i]->next_batch_of_zone_map(&size, columns[i]));
            }
        }
        _output_rows += size;
        return Status::OK();
    }
    return Status::EndOfFile("End of VStatisticsIterator");
}

Status VMergeIteratorContext::block_reset(const std::shared_ptr<Block>& block) {
    if (!block->columns()) {
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
            block->insert(ColumnWithTypeAndName(std::move(column), data_type, column_desc->name()));
        }
    } else {
        block->clear_column_data();
    }
    return Status::OK();
}

bool VMergeIteratorContext::compare(const VMergeIteratorContext& rhs) const {
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
    result ? set_same(true) : rhs.set_same(true);
    return result;
}

// `advanced = false` when current block finished
void VMergeIteratorContext::copy_rows(Block* block, bool advanced) {
    Block& src = *_block;
    Block& dst = *block;
    if (_cur_batch_num == 0) {
        return;
    }

    // copy a row to dst block column by column
    size_t start = _index_in_block - _cur_batch_num + 1 - advanced;

    for (size_t i = 0; i < _num_columns; ++i) {
        auto& s_col = src.get_by_position(i);
        auto& d_col = dst.get_by_position(i);

        ColumnPtr& s_cp = s_col.column;
        ColumnPtr& d_cp = d_col.column;

        d_cp->assume_mutable()->insert_range_from(*s_cp, start, _cur_batch_num);
    }
    const auto& tmp_pre_ctx_same_bit = get_pre_ctx_same();
    dst.set_same_bit(tmp_pre_ctx_same_bit.begin(), tmp_pre_ctx_same_bit.begin() + _cur_batch_num);
    _cur_batch_num = 0;
}

void VMergeIteratorContext::copy_rows(BlockView* view, bool advanced) {
    if (_cur_batch_num == 0) {
        return;
    }
    size_t start = _index_in_block - _cur_batch_num + 1 - advanced;

    const auto& tmp_pre_ctx_same_bit = get_pre_ctx_same();
    for (size_t i = 0; i < _cur_batch_num; ++i) {
        view->push_back({_block, static_cast<int>(start + i), tmp_pre_ctx_same_bit[i]});
    }

    _cur_batch_num = 0;
}

// This iterator will generate ordered data. For example for schema
// (int, int) this iterator will generator data like
// (0, 1), (1, 2), (2, 3), (3, 4)...
//
// Usage:
//      Schema schema;
//      VAutoIncrementIterator iter(schema, 1000);
//      StorageReadOptions opts;
//      RETURN_IF_ERROR(iter.init(opts));
//      Block block;
//      do {
//          st = iter.next_batch(&block);
//      } while (st.ok());
class VAutoIncrementIterator : public RowwiseIterator {
public:
    // Will generate num_rows rows in total
    VAutoIncrementIterator(const Schema& schema, size_t num_rows)
            : _schema(schema), _num_rows(num_rows), _rows_returned() {}
    ~VAutoIncrementIterator() override = default;

    // NOTE: Currently, this function will ignore StorageReadOptions
    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override {
        int row_idx = 0;
        while (_rows_returned < _num_rows) {
            for (int j = 0; j < _schema.num_columns(); ++j) {
                ColumnWithTypeAndName& vc = block->get_by_position(j);
                IColumn& vi = (IColumn&)(*vc.column);

                char data[16] = {};
                size_t data_len = 0;
                const auto* col_schema = _schema.column(j);
                switch (col_schema->type()) {
                case FieldType::OLAP_FIELD_TYPE_SMALLINT:
                    *(int16_t*)data = _rows_returned + j;
                    data_len = sizeof(int16_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_INT:
                    *(int32_t*)data = _rows_returned + j;
                    data_len = sizeof(int32_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_BIGINT:
                    *(int64_t*)data = _rows_returned + j;
                    data_len = sizeof(int64_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_FLOAT:
                    *(float*)data = _rows_returned + j;
                    data_len = sizeof(float);
                    break;
                case FieldType::OLAP_FIELD_TYPE_DOUBLE:
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

        if (row_idx > 0) {
            return Status::OK();
        }
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

Status VMergeIteratorContext::init(const StorageReadOptions& opts) {
    _block_row_max = opts.block_row_max;
    _record_rowids = opts.record_rowids;
    RETURN_IF_ERROR(_load_next_block());
    if (valid()) {
        RETURN_IF_ERROR(advance());
    }
    _pre_ctx_same_bit.reserve(_block_row_max);
    _pre_ctx_same_bit.assign(_block_row_max, false);
    return Status::OK();
}

Status VMergeIteratorContext::advance() {
    _skip = false;
    _same = false;
    // NOTE: we increase _index_in_block directly to valid one check
    do {
        _index_in_block++;
        if (LIKELY(_index_in_block < _block->rows())) {
            return Status::OK();
        }
        // current batch has no data, load next batch
        RETURN_IF_ERROR(_load_next_block());
    } while (_valid);
    return Status::OK();
}

Status VMergeIteratorContext::_load_next_block() {
    do {
        if (_block != nullptr) {
            _block_list.push_back(_block);
            _block = nullptr;
        }
        for (auto it = _block_list.begin(); it != _block_list.end(); it++) {
            if (it->use_count() == 1) {
                static_cast<void>(block_reset(*it));
                _block = *it;
                _block_list.erase(it);
                break;
            }
        }
        if (_block == nullptr) {
            _block = std::make_shared<Block>();
            static_cast<void>(block_reset(_block));
        }
        Status st = _iter->next_batch(_block.get());
        if (!st.ok()) {
            _valid = false;
            if (st.is<END_OF_FILE>()) {
                return Status::OK();
            } else {
                return st;
            }
        }
        if (UNLIKELY(_record_rowids)) {
            RETURN_IF_ERROR(_iter->current_block_row_locations(&_block_row_locations));
        }
    } while (_block->rows() == 0);
    _index_in_block = -1;
    _valid = true;
    return Status::OK();
}

Status VMergeIterator::init(const StorageReadOptions& opts) {
    if (_origin_iters.empty()) {
        return Status::OK();
    }
    _schema = &(_origin_iters[0]->schema());
    _record_rowids = opts.record_rowids;

    for (auto& iter : _origin_iters) {
        auto ctx = std::make_unique<VMergeIteratorContext>(std::move(iter), _sequence_id_idx,
                                                           _is_unique, _is_reverse,
                                                           opts.read_orderby_key_columns);
        RETURN_IF_ERROR(ctx->init(opts));
        if (!ctx->valid()) {
            continue;
        }
        _merge_heap.push(ctx.release());
    }

    _origin_iters.clear();

    _block_row_max = opts.block_row_max;

    return Status::OK();
}

// VUnionIterator will read data from input iterator one by one.
class VUnionIterator : public RowwiseIterator {
public:
    // Iterators' ownership it transferred to this class.
    // This class will delete all iterators when destructs
    // Client should not use iterators anymore.
    VUnionIterator(std::vector<RowwiseIteratorUPtr>&& v) : _origin_iters(std::move(v)) {}

    ~VUnionIterator() override = default;

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override;

    const Schema& schema() const override { return *_schema; }

    Status current_block_row_locations(std::vector<RowLocation>* locations) override;

    bool update_profile(RuntimeProfile* profile) override {
        if (_cur_iter != nullptr) {
            return _cur_iter->update_profile(profile);
        }
        return false;
    }

private:
    const Schema* _schema = nullptr;
    RowwiseIteratorUPtr _cur_iter = nullptr;
    std::vector<RowwiseIteratorUPtr> _origin_iters;
};

Status VUnionIterator::init(const StorageReadOptions& opts) {
    if (_origin_iters.empty()) {
        return Status::OK();
    }

    // we use back() and pop_back() of std::vector to handle each iterator,
    // so reverse the vector here to keep result block of next_batch to be
    // in the same order as the original segments.
    std::reverse(_origin_iters.begin(), _origin_iters.end());

    for (auto& iter : _origin_iters) {
        RETURN_IF_ERROR(iter->init(opts));
    }
    _cur_iter = std::move(_origin_iters.back());
    _schema = &_cur_iter->schema();
    return Status::OK();
}

Status VUnionIterator::next_batch(Block* block) {
    while (_cur_iter != nullptr) {
        auto st = _cur_iter->next_batch(block);
        if (st.is<END_OF_FILE>()) {
            _origin_iters.pop_back();
            if (!_origin_iters.empty()) {
                _cur_iter = std::move(_origin_iters.back());
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

RowwiseIteratorUPtr new_merge_iterator(std::vector<RowwiseIteratorUPtr>&& inputs,
                                       int sequence_id_idx, bool is_unique, bool is_reverse,
                                       uint64_t* merged_rows) {
    if (inputs.size() == 1) {
        return std::move(inputs[0]);
    }
    return std::make_unique<VMergeIterator>(std::move(inputs), sequence_id_idx, is_unique,
                                            is_reverse, merged_rows);
}

RowwiseIteratorUPtr new_union_iterator(std::vector<RowwiseIteratorUPtr>&& inputs) {
    if (inputs.size() == 1) {
        return std::move(inputs[0]);
    }
    return std::make_unique<VUnionIterator>(std::move(inputs));
}

RowwiseIterator* new_vstatistics_iterator(std::shared_ptr<Segment> segment, const Schema& schema) {
    return new VStatisticsIterator(segment, schema);
}

RowwiseIteratorUPtr new_auto_increment_iterator(const Schema& schema, size_t num_rows) {
    return std::make_unique<VAutoIncrementIterator>(schema, num_rows);
}

} // namespace vectorized

} // namespace doris

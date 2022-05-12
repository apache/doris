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

#include "vec/olap/block_aggregator.h"

#include "util/simd/bits.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris::vectorized {

template <typename ColumnType>
inline void compare_previous(const IColumn& col, uint8_t* flags) {
    if (col.is_nullable()) {
        auto col_ptr = reinterpret_cast<const ColumnNullable*>(&col);
        for (int i = 1; i < col_ptr->size(); ++i) {
            flags[i] &= (col_ptr->compare_at(i, i - 1, col, -1) == 0);
        }
    } else {
        auto col_ptr = reinterpret_cast<const ColumnType*>(&col);
        for (int i = 1; i < col_ptr->size(); ++i) {
            flags[i] &= (col_ptr->compare_at(i, i - 1, col, -1) == 0);
        }
    }
}

BlockAggregator::BlockAggregator(const TabletSchema* tablet_schema,
                                 const std::vector<uint32_t>& output_columns,
                                 const std::vector<uint32_t>& return_columns,
                                 const std::unordered_set<uint32_t>* null_set, const int batch_size)
        : _tablet_schema(tablet_schema),
          _output_columns(output_columns),
          _sorted_output_columns(output_columns),
          _return_columns(return_columns),
          _null_set(null_set),
          _batch_size(batch_size) {
    _has_agg_data = false;
    _do_aggregate = true;
    _agg_ratio = config::block_aggregate_ratio;
    _num_columns = _output_columns.size();
    _num_key_columns = 0;

    // todo(zeno) Make sure column prune done
    std::sort(_sorted_output_columns.begin(), _sorted_output_columns.end());
    _output_columns_loc.resize(_num_columns);
    for (int i = 0; i < _num_columns; ++i) {
        for (int j = 0; j < _num_columns; ++j) {
            if (_sorted_output_columns[i] == output_columns[j]) {
                _output_columns_loc[i] = j;
                break;
            }
        }
    }

    for (auto index : _sorted_output_columns) {
        if (_tablet_schema->column(index).is_key()) {
            _num_key_columns++;
        }
    }

    _current_row = 0;
    _source_size = 0;
    _eq_previous.reserve(_batch_size);
    _agg_index.reserve(_batch_size);
    _agg_range.reserve(_batch_size);

    // init _key_comparator
    for (int i = 0; i < _num_key_columns; ++i) {
        _key_comparator.emplace_back(
                _get_comparator(_tablet_schema->column(_sorted_output_columns[i])));
    }

    // init _column_aggregator
    for (int i = 0; i < _num_key_columns; ++i) {
        _column_aggregator.emplace_back(std::make_unique<KeyColumnAggregator>());
    }

    for (int i = _num_key_columns; i < _num_columns; ++i) {
        bool is_nullable = (_null_set != nullptr &&
                            _null_set->find(_sorted_output_columns[i]) != _null_set->end());
        auto col = _tablet_schema->column(_sorted_output_columns[i]);
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(col, is_nullable);
        _column_aggregator.emplace_back(std::make_unique<ValueColumnAggregator>(col, data_type));
    }

    aggregate_reset();
}

CompareFunc BlockAggregator::_get_comparator(const TabletColumn& col) {
    switch (col.type()) {
    case OLAP_FIELD_TYPE_TINYINT:
        return &compare_previous<ColumnInt8>;
    case OLAP_FIELD_TYPE_SMALLINT:
        return &compare_previous<ColumnInt16>;
    case OLAP_FIELD_TYPE_INT:
        return &compare_previous<ColumnInt32>;
    case OLAP_FIELD_TYPE_BIGINT:
        return &compare_previous<ColumnInt64>;
    case OLAP_FIELD_TYPE_LARGEINT:
        return &compare_previous<ColumnInt128>;
    case OLAP_FIELD_TYPE_BOOL:
        return &compare_previous<ColumnUInt8>;
    case OLAP_FIELD_TYPE_FLOAT:
        return &compare_previous<ColumnFloat32>;
    case OLAP_FIELD_TYPE_DOUBLE:
        return &compare_previous<ColumnFloat64>;
    case OLAP_FIELD_TYPE_DECIMAL:
        return &compare_previous<ColumnDecimal<Decimal128>>;
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_STRING:
        return &compare_previous<ColumnString>;
    case OLAP_FIELD_TYPE_DATE:
        return &compare_previous<ColumnDate>;
    case OLAP_FIELD_TYPE_DATETIME:
        return &compare_previous<ColumnDateTime>;
    default:
        DCHECK(false) << "unhandled key column type: " << col.type();
        return nullptr;
    }
}

void BlockAggregator::update_source(const Block* block) {
    _eq_previous.assign(block->rows(), 1);
    _source_size = 0;
    _current_row = 0;
    _do_aggregate = true;

    for (int i = _num_key_columns - 1; i >= 0; --i) {
        _key_comparator[i](*block->get_column_by_position(_output_columns_loc[i]),
                           _eq_previous.data());
        size_t eq_count = simd::count_not_zero(_eq_previous);
        if (_agg_ratio != 0 && eq_count + 1 < block->rows() * _agg_ratio) {
            _do_aggregate = false;
            return;
        }
    }

    if (_result_block->rows() > 0 && _num_key_columns > 0) {
        _eq_previous[0] = _result_block->compare_at(_result_block->rows() - 1, 0, _num_key_columns,
                                                    *block, -1, _output_columns_loc) == 0;
    } else {
        _eq_previous[0] = 0;
    }

    // update source column
    for (int i = 0; i < _num_columns; ++i) {
        _column_aggregator[i]->update_source(block->get_column_by_position(_output_columns_loc[i]));
    }
    _source_size = block->rows();
}

void BlockAggregator::aggregate() {
    if (source_exhausted()) {
        return;
    }

    _agg_index.clear();
    _agg_range.clear();

    if (_eq_previous[_current_row] == 1) {
        _agg_range.emplace_back(0);
    }

    uint32_t cur_row = _current_row;
    uint32_t res_rows = _result_block->rows();
    for (; cur_row < _source_size; ++cur_row) {
        if (_eq_previous[cur_row] == 0) {
            if (res_rows >= _batch_size) {
                break;
            }
            res_rows++;
            _agg_index.emplace_back(cur_row);
            _agg_range.emplace_back(1);
        } else {
            _agg_range[_agg_range.size() - 1] += 1;
        }
    }

    bool neq_previous = !_eq_previous[_current_row] && (_result_block->rows() != 0);

    for (int i = 0; i < _num_key_columns; ++i) {
        _column_aggregator[i]->aggregate_keys(_agg_index.size(), _agg_index.data());
    }
    for (int i = _num_key_columns; i < _num_columns; ++i) {
        _column_aggregator[i]->aggregate_values(_current_row, _agg_range.size(), _agg_range.data(),
                                                neq_previous);
    }
    _current_row = cur_row;
    _has_agg_data = true;
}

std::shared_ptr<Block> BlockAggregator::aggregate_result() {
    for (int i = 0; i < _num_columns; ++i) {
        _column_aggregator[i]->finalize();
    }
    _has_agg_data = false;
    return _result_block;
}

void BlockAggregator::aggregate_reset() {
    _result_block =
            std::make_shared<Block>(_tablet_schema->create_block(_return_columns, _null_set));
    auto cols = _result_block->mutate_columns();
    for (int i = 0; i < _num_columns; ++i) {
        _column_aggregator[i]->update_aggregate(cols[_output_columns_loc[i]]);
    }
    _has_agg_data = false;
}

} // namespace doris::vectorized
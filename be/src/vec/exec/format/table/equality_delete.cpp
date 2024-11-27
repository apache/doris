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

#include "vec/exec/format/table/equality_delete.h"

namespace doris::vectorized {

std::unique_ptr<EqualityDeleteBase> EqualityDeleteBase::get_delete_impl(Block* delete_block) {
    if (delete_block->columns() == 1) {
        return std::make_unique<SimpleEqualityDelete>(delete_block);
    } else {
        return std::make_unique<MultiEqualityDelete>(delete_block);
    }
}

Status SimpleEqualityDelete::_build_set() {
    COUNTER_UPDATE(num_delete_rows, _delete_block->rows());
    if (_delete_block->columns() != 1) {
        return Status::InternalError("Simple equality delete can be only applied with one column");
    }
    auto& column_and_type = _delete_block->get_by_position(0);
    _delete_column_name = column_and_type.name;
    _delete_column_type = remove_nullable(column_and_type.type)->get_type_as_type_descriptor().type;
    _hybrid_set.reset(create_set(_delete_column_type, _delete_block->rows()));
    _hybrid_set->insert_fixed_len(column_and_type.column, 0);
    return Status::OK();
}

Status SimpleEqualityDelete::filter_data_block(Block* data_block) {
    SCOPED_TIMER(equality_delete_time);
    auto* column_and_type = data_block->try_get_by_name(_delete_column_name);
    if (column_and_type == nullptr) {
        return Status::InternalError("Can't find the delete column '{}' in data file",
                                     _delete_column_name);
    }
    if (remove_nullable(column_and_type->type)->get_type_as_type_descriptor().type !=
        _delete_column_type) {
        return Status::InternalError("Not support type change in column '{}'", _delete_column_name);
    }
    size_t rows = data_block->rows();
    // _filter: 1 => in _hybrid_set; 0 => not in _hybrid_set
    if (_filter == nullptr) {
        _filter = std::make_unique<IColumn::Filter>(rows, 0);
    } else {
        _filter->resize_fill(rows, 0);
    }

    if (column_and_type->column->is_nullable()) {
        const NullMap& null_map =
                reinterpret_cast<const ColumnNullable*>(column_and_type->column.get())
                        ->get_null_map_data();
        _hybrid_set->find_batch_nullable(
                remove_nullable(column_and_type->column)->assume_mutable_ref(), rows, null_map,
                *_filter.get());
        if (_hybrid_set->contain_null()) {
            auto* filter_data = _filter->data();
            for (size_t i = 0; i < rows; ++i) {
                filter_data[i] = filter_data[i] || null_map[i];
            }
        }
    } else {
        _hybrid_set->find_batch(column_and_type->column->assume_mutable_ref(), rows,
                                *_filter.get());
    }
    // should reverse _filter
    auto* filter_data = _filter->data();
    for (size_t i = 0; i < rows; ++i) {
        filter_data[i] = !filter_data[i];
    }

    Block::filter_block_internal(data_block, *_filter.get(), data_block->columns());
    return Status::OK();
}

Status MultiEqualityDelete::_build_set() {
    COUNTER_UPDATE(num_delete_rows, _delete_block->rows());
    size_t rows = _delete_block->rows();
    _delete_hashes.clear();
    _delete_hashes.resize(rows, 0);
    for (ColumnPtr column : _delete_block->get_columns()) {
        column->update_hashes_with_value(_delete_hashes.data(), nullptr);
    }
    for (size_t i = 0; i < rows; ++i) {
        _delete_hash_map.insert({_delete_hashes[i], i});
    }
    _data_column_index.resize(_delete_block->columns());
    return Status::OK();
}

Status MultiEqualityDelete::filter_data_block(Block* data_block) {
    SCOPED_TIMER(equality_delete_time);
    size_t column_index = 0;
    for (string column_name : _delete_block->get_names()) {
        auto* column_and_type = data_block->try_get_by_name(column_name);
        if (column_and_type == nullptr) {
            return Status::InternalError("Can't find the delete column '{}' in data file",
                                         column_name);
        }
        if (!_delete_block->get_by_name(column_name).type->equals(*column_and_type->type)) {
            return Status::InternalError("Not support type change in column '{}'", column_name);
        }
        _data_column_index[column_index++] = data_block->get_position_by_name(column_name);
    }
    size_t rows = data_block->rows();
    _data_hashes.clear();
    _data_hashes.resize(rows, 0);
    for (size_t index : _data_column_index) {
        data_block->get_by_position(index).column->update_hashes_with_value(_data_hashes.data(),
                                                                            nullptr);
    }

    if (_filter == nullptr) {
        _filter = std::make_unique<IColumn::Filter>(rows, 1);
    } else {
        _filter->resize_fill(rows, 1);
    }
    auto* filter_data = _filter->data();
    for (size_t i = 0; i < rows; ++i) {
        for (auto beg = _delete_hash_map.lower_bound(_data_hashes[i]),
                  end = _delete_hash_map.upper_bound(_data_hashes[i]);
             beg != end; ++beg) {
            if (_equal(data_block, i, beg->second)) {
                filter_data[i] = 0;
                break;
            }
        }
    }

    Block::filter_block_internal(data_block, *_filter.get(), data_block->columns());
    return Status::OK();
}

bool MultiEqualityDelete::_equal(Block* data_block, size_t data_row_index,
                                 size_t delete_row_index) {
    for (size_t i = 0; i < _delete_block->columns(); ++i) {
        ColumnPtr data_col = data_block->get_by_position(_data_column_index[i]).column;
        ColumnPtr delete_col = _delete_block->get_by_position(i).column;
        if (data_col->compare_at(data_row_index, delete_row_index, delete_col->assume_mutable_ref(),
                                 -1) != 0) {
            return false;
        }
    }
    return true;
}

} // namespace doris::vectorized

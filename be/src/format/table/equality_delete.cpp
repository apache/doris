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

#include "format/table/equality_delete.h"

#include "core/column/column_nullable.h"
#include "exprs/create_predicate_function.h"
#include "util/hash_util.hpp"

namespace doris {
#include "common/compile_check_begin.h"

namespace {

bool is_equality_delete_byte_type(const DataTypePtr& type) {
    const auto primitive_type = remove_nullable(type)->get_primitive_type();
    return is_string_type(primitive_type) || is_varbinary(primitive_type);
}

void insert_byte_values(const ColumnWithTypeAndName& column_and_type, HybridSetBase* byte_set) {
    DORIS_CHECK(byte_set != nullptr);
    const IColumn* values = column_and_type.column.get();
    const uint8_t* null_data = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(values); nullable != nullptr) {
        null_data = nullable->get_null_map_data().data();
        values = &nullable->get_nested_column();
    }
    for (size_t row = 0; row < column_and_type.column->size(); ++row) {
        if (null_data != nullptr && null_data[row] != 0) {
            byte_set->insert(static_cast<const void*>(nullptr));
            continue;
        }
        const StringRef value = values->get_data_at(row);
        byte_set->insert(&value);
    }
}

void find_byte_values(const ColumnWithTypeAndName& column_and_type, const HybridSetBase& byte_set,
                      IColumn::Filter* matches) {
    DORIS_CHECK(matches != nullptr);
    const IColumn* values = column_and_type.column.get();
    const uint8_t* null_data = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(values); nullable != nullptr) {
        null_data = nullable->get_null_map_data().data();
        values = &nullable->get_nested_column();
    }
    for (size_t row = 0; row < column_and_type.column->size(); ++row) {
        if (null_data != nullptr && null_data[row] != 0) {
            (*matches)[row] = byte_set.contain_null();
            continue;
        }
        const StringRef value = values->get_data_at(row);
        (*matches)[row] = byte_set.find(&value);
    }
}

void update_byte_hashes(const ColumnWithTypeAndName& column_and_type,
                        std::vector<uint64_t>* hashes) {
    DORIS_CHECK(hashes != nullptr);
    const IColumn* values = column_and_type.column.get();
    const uint8_t* null_data = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(values); nullable != nullptr) {
        null_data = nullable->get_null_map_data().data();
        for (size_t row = 0; row < nullable->size(); ++row) {
            if (null_data[row] != 0) {
                (*hashes)[row] = HashUtil::xxHash64NullWithSeed((*hashes)[row]);
            }
        }
        values = &nullable->get_nested_column();
    }

    for (size_t row = 0; row < column_and_type.column->size(); ++row) {
        if (null_data == nullptr || null_data[row] == 0) {
            const StringRef value = values->get_data_at(row);
            (*hashes)[row] = HashUtil::xxHash64WithSeed(value.data, value.size, (*hashes)[row]);
        }
    }
}

void update_equality_delete_hashes(const ColumnWithTypeAndName& column_and_type,
                                   std::vector<uint64_t>* hashes) {
    DORIS_CHECK(hashes != nullptr);
    if (is_equality_delete_byte_type(column_and_type.type)) {
        update_byte_hashes(column_and_type, hashes);
        return;
    }
    column_and_type.column->update_hashes_with_value(hashes->data(), nullptr);
}

bool equality_delete_values_equal(const ColumnWithTypeAndName& data_column, size_t data_row,
                                  const ColumnWithTypeAndName& delete_column, size_t delete_row) {
    if (!is_equality_delete_byte_type(data_column.type) ||
        !is_equality_delete_byte_type(delete_column.type)) {
        return data_column.column->compare_at(data_row, delete_row, *delete_column.column, -1) == 0;
    }

    const IColumn* data_values = data_column.column.get();
    const IColumn* delete_values = delete_column.column.get();
    bool data_is_null = false;
    bool delete_is_null = false;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(data_values);
        nullable != nullptr) {
        data_is_null = nullable->is_null_at(data_row);
        data_values = &nullable->get_nested_column();
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(delete_values);
        nullable != nullptr) {
        delete_is_null = nullable->is_null_at(delete_row);
        delete_values = &nullable->get_nested_column();
    }
    if (data_is_null || delete_is_null) {
        return data_is_null && delete_is_null;
    }
    return data_values->get_data_at(data_row) == delete_values->get_data_at(delete_row);
}

} // namespace

std::unique_ptr<EqualityDeleteBase> EqualityDeleteBase::get_delete_impl(
        const Block* delete_block, const std::vector<int>& delete_col_ids) {
    DCHECK_EQ(delete_block->columns(), delete_col_ids.size());
    if (delete_block->columns() == 1) {
        return std::make_unique<SimpleEqualityDelete>(delete_block, delete_col_ids);
    } else {
        return std::make_unique<MultiEqualityDelete>(delete_block, delete_col_ids);
    }
}

Status SimpleEqualityDelete::_build_set() {
    COUNTER_UPDATE(num_delete_rows, _delete_block->rows());
    if (_delete_block->columns() != 1) [[unlikely]] {
        return Status::InternalError("Simple equality delete can be only applied with one column");
    }
    const auto& column_and_type = _delete_block->get_by_position(0);
    auto delete_column_type = remove_nullable(column_and_type.type)->get_primitive_type();
    size_t non_null_rows = _delete_block->rows();
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column_and_type.column.get());
        nullable != nullptr) {
        non_null_rows = std::ranges::count(nullable->get_null_map_data(), UInt8(0));
    }
    if (is_equality_delete_byte_type(column_and_type.type)) {
        // VARBINARY has no generic set dispatch. Store all Doris string carriers in the same byte
        // set so Iceberg FIXED/BINARY values compare independently of their physical column class.
        _hybrid_set.reset(create_set(TYPE_STRING, non_null_rows, true));
        insert_byte_values(column_and_type, _hybrid_set.get());
    } else {
        _hybrid_set.reset(create_set(delete_column_type, non_null_rows, true));
        _hybrid_set->insert_fixed_len(column_and_type.column, 0);
    }
    return Status::OK();
}

Status SimpleEqualityDelete::filter_data_block(
        Block* data_block, const std::unordered_map<std::string, uint32_t>* col_name_to_block_idx,
        const std::unordered_map<int, std::string>& id_to_block_column_name,
        IColumn::Filter& filter) {
    SCOPED_TIMER(equality_delete_time);
    DCHECK(_delete_col_ids.size() == 1);
    auto column_field_id = _delete_col_ids[0];

    auto column_and_type = data_block->get_by_position(
            col_name_to_block_idx->at(id_to_block_column_name.at(column_field_id)));
    const auto& delete_column = _delete_block->get_by_position(0);
    const bool delete_is_byte = is_equality_delete_byte_type(delete_column.type);
    const bool byte_compatible =
            delete_is_byte && is_equality_delete_byte_type(column_and_type.type);
    if (delete_is_byte && !byte_compatible) [[unlikely]] {
        return Status::InternalError(
                "Not support type change in column '{}', src type: {}, target type: {}",
                column_and_type.name, delete_column.type->get_name(),
                column_and_type.type->get_name());
    }

    size_t rows = data_block->rows();
    //     _filter: 1 => in _hybrid_set; 0 => not in _hybrid_set
    if (_single_filter == nullptr) {
        _single_filter = std::make_unique<IColumn::Filter>(rows, 0);
    } else {
        // reset the array capacity and fill all elements using the 0
        _single_filter->assign(rows, UInt8(0));
    }
    if (byte_compatible) {
        find_byte_values(column_and_type, *_hybrid_set, _single_filter.get());
    } else if (column_and_type.column->is_nullable()) {
        const NullMap& null_map =
                reinterpret_cast<const ColumnNullable*>(column_and_type.column.get())
                        ->get_null_map_data();
        _hybrid_set->find_batch_nullable(*remove_nullable(column_and_type.column), rows, null_map,
                                         *_single_filter);
        if (_hybrid_set->contain_null()) {
            auto* filter_data = _single_filter->data();
            for (size_t i = 0; i < rows; ++i) {
                filter_data[i] = filter_data[i] || null_map[i];
            }
        }
    } else {
        _hybrid_set->find_batch(*column_and_type.column, rows, *_single_filter);
    }
    // should reverse _filter
    auto* filter_data = filter.data();
    for (size_t i = 0; i < rows; ++i) {
        filter_data[i] &= !_single_filter->data()[i];
    }
    return Status::OK();
}

Status MultiEqualityDelete::_build_set() {
    COUNTER_UPDATE(num_delete_rows, _delete_block->rows());
    size_t rows = _delete_block->rows();
    _delete_hashes.clear();
    _delete_hashes.resize(rows, 0);
    for (const auto& column : _delete_block->get_columns_with_type_and_name()) {
        update_equality_delete_hashes(column, &_delete_hashes);
    }
    for (size_t i = 0; i < rows; ++i) {
        _delete_hash_map.insert({_delete_hashes[i], i});
    }
    _data_column_index.resize(_delete_block->columns());
    return Status::OK();
}

Status MultiEqualityDelete::filter_data_block(
        Block* data_block, const std::unordered_map<std::string, uint32_t>* col_name_to_block_idx,
        const std::unordered_map<int, std::string>& id_to_block_column_name,
        IColumn::Filter& filter) {
    SCOPED_TIMER(equality_delete_time);
    DCHECK_EQ(_delete_block->get_columns_with_type_and_name().size(), _delete_col_ids.size());
    size_t column_index = 0;

    for (size_t idx = 0; idx < _delete_block->get_columns_with_type_and_name().size(); ++idx) {
        auto delete_col = _delete_block->get_columns_with_type_and_name()[idx];
        auto delete_col_id = _delete_col_ids[idx];

        DCHECK(id_to_block_column_name.contains(delete_col_id));
        const auto& block_column_name = id_to_block_column_name.at(delete_col_id);
        if (!col_name_to_block_idx->contains(block_column_name)) [[unlikely]] {
            return Status::InternalError("Column '{}' not found in data block: {}",
                                         block_column_name, data_block->dump_structure());
        }
        auto column_and_type =
                data_block->safe_get_by_position(col_name_to_block_idx->at(block_column_name));
        const bool byte_compatible = is_equality_delete_byte_type(delete_col.type) &&
                                     is_equality_delete_byte_type(column_and_type.type);
        if (!delete_col.type->equals(*column_and_type.type) && !byte_compatible) [[unlikely]] {
            return Status::InternalError(
                    "Not support type change in column '{}', src type: {}, target type: {}",
                    block_column_name, delete_col.type->get_name(),
                    column_and_type.type->get_name());
        }
        _data_column_index[column_index++] = col_name_to_block_idx->at(block_column_name);
    }
    size_t rows = data_block->rows();
    _data_hashes.clear();
    _data_hashes.resize(rows, 0);
    for (size_t index : _data_column_index) {
        update_equality_delete_hashes(data_block->get_by_position(index), &_data_hashes);
    }
    auto* filter_data = filter.data();
    for (size_t i = 0; i < rows; ++i) {
        for (auto beg = _delete_hash_map.lower_bound(_data_hashes[i]),
                  end = _delete_hash_map.upper_bound(_data_hashes[i]);
             beg != end; ++beg) {
            if (filter[i] && _equal(data_block, i, beg->second)) {
                filter_data[i] = 0;
                break;
            }
        }
    }

    return Status::OK();
}

bool MultiEqualityDelete::_equal(Block* data_block, size_t data_row_index,
                                 size_t delete_row_index) {
    for (size_t i = 0; i < _delete_block->columns(); ++i) {
        const auto& data_col = data_block->get_by_position(_data_column_index[i]);
        const auto& delete_col = _delete_block->get_by_position(i);
        if (!equality_delete_values_equal(data_col, data_row_index, delete_col, delete_row_index)) {
            return false;
        }
    }
    return true;
}

#include "common/compile_check_end.h"
} // namespace doris

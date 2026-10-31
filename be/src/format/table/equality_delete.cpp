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

#include <gen_cpp/ExternalTableSchema_types.h>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_factory.hpp"
#include "exprs/create_predicate_function.h"
#include "format/column_type_convert.h"
#include "util/hash_util.hpp"

namespace doris {

DataTypePtr equality_delete_key_type(const schema::external::TField* field,
                                     const DataTypePtr& delete_key_type) {
    DORIS_CHECK(delete_key_type->is_nullable());
    // A plan from an FE that sends no scan schema, or no type for the field, leaves only the delete
    // file's own type, which is how keys were compared before.
    if (field == nullptr || !field->__isset.type || is_equality_delete_byte_type(delete_key_type)) {
        return delete_key_type;
    }
    const auto primitive_type = thrift_to_type(field->type.type);
    DORIS_CHECK(!is_complex_type(primitive_type));
    return DataTypeFactory::instance().create_data_type(
            primitive_type, true, field->type.__isset.precision ? field->type.precision : 0,
            field->type.__isset.scale ? field->type.scale : 0,
            field->type.__isset.len ? field->type.len : -1);
}

bool is_equality_delete_byte_type(const DataTypePtr& type) {
    const auto primitive_type = remove_nullable(type)->get_primitive_type();
    return is_string_type(primitive_type) || is_varbinary(primitive_type);
}

Status widen_equality_delete_key(ColumnPtr column, const DataTypePtr& column_type,
                                 const DataTypePtr& key_type, ColumnPtr* result) {
    DORIS_CHECK(result != nullptr);
    DORIS_CHECK(column_type->is_nullable());
    DORIS_CHECK(key_type->is_nullable());
    if (column_type->equals(*key_type)) {
        *result = std::move(column);
        return Status::OK();
    }
    // Byte keys keep the type they were read in and match by their bytes, and the converters below
    // would turn VARBINARY into its text form.
    if (is_equality_delete_byte_type(column_type) || is_equality_delete_byte_type(key_type))
            [[unlikely]] {
        return Status::InternalError("Unexpected type change of equality delete key from {} to {}",
                                     column_type->get_name(), key_type->get_name());
    }
    // The Parquet and ORC readers widen the data column of a promoted field with the same
    // converters, so a widened delete key holds exactly the value of the data key it must match.
    auto converter =
            converter::ColumnTypeConverter::get_converter(column_type, key_type, converter::COMMON);
    if (!converter->support()) {
        return Status::InternalError(
                "Not support type change of equality delete key from {} to {}: {}",
                column_type->get_name(), key_type->get_name(), converter->get_error_msg());
    }
    if (converter->is_consistent()) {
        // The same column layout, for example DATETIMEV2 of another scale.
        *result = std::move(column);
        return Status::OK();
    }
    MutableColumnPtr widened = key_type->create_column();
    // The converters fill only the nested values; as in OrcReader::_orc_column_to_doris_column, the
    // null map is copied before converting.
    const auto& source_null_map = assert_cast<const ColumnNullable&>(*column).get_null_map_data();
    assert_cast<ColumnNullable&>(*widened).get_null_map_data().insert(source_null_map.begin(),
                                                                      source_null_map.end());
    RETURN_IF_ERROR(converter->convert(column, widened));
    *result = std::move(widened);
    return Status::OK();
}

namespace {

// The rows of a byte-key column. A key that the data file lacks arrives as a full column of NULLs
// (IcebergReaderMixin::_create_missing_equality_delete_value); a Const(Nullable(String)) would skip
// the ColumnNullable unwrapping below and read its NULL as an empty string.
class ByteKeyColumn {
public:
    explicit ByteKeyColumn(const IColumn& column) : _values(&column) {
        DORIS_CHECK(!is_column_const(column));
        if (const auto* nullable = check_and_get_column<ColumnNullable>(_values)) {
            _null_map = &nullable->get_null_map_data();
            _values = &nullable->get_nested_column();
        }
    }

    bool is_null(size_t row) const { return _null_map != nullptr && (*_null_map)[row] != 0; }

    StringRef value(size_t row) const { return _values->get_data_at(row); }

private:
    const IColumn* _values;
    const NullMap* _null_map = nullptr;
};

void insert_byte_values(const IColumn& column, HybridSetBase* byte_set) {
    const ByteKeyColumn keys(column);
    for (size_t row = 0; row < column.size(); ++row) {
        if (keys.is_null(row)) {
            byte_set->insert(static_cast<const void*>(nullptr));
            continue;
        }
        const StringRef value = keys.value(row);
        byte_set->insert(&value);
    }
}

void find_byte_values(const IColumn& column, const HybridSetBase& byte_set,
                      IColumn::Filter* matches) {
    const ByteKeyColumn keys(column);
    for (size_t row = 0; row < column.size(); ++row) {
        if (keys.is_null(row)) {
            (*matches)[row] = byte_set.contain_null();
            continue;
        }
        const StringRef value = keys.value(row);
        (*matches)[row] = byte_set.find(&value);
    }
}

void update_equality_delete_hashes(const ColumnWithTypeAndName& column_and_type,
                                   std::vector<uint64_t>* hashes) {
    if (!is_equality_delete_byte_type(column_and_type.type)) {
        column_and_type.column->update_hashes_with_value(hashes->data(), nullptr);
        return;
    }
    // Hash the bytes rather than call update_hashes_with_value: VARBINARY does not implement it, and a
    // STRING delete key must land on the same hash as the equal VARBINARY data key.
    const ByteKeyColumn keys(*column_and_type.column);
    for (size_t row = 0; row < column_and_type.column->size(); ++row) {
        if (keys.is_null(row)) {
            (*hashes)[row] = HashUtil::xxHash64NullWithSeed((*hashes)[row]);
            continue;
        }
        const StringRef value = keys.value(row);
        (*hashes)[row] = HashUtil::xxHash64WithSeed(value.data, value.size, (*hashes)[row]);
    }
}

bool equality_delete_values_equal(const ColumnWithTypeAndName& data_column, size_t data_row,
                                  const ColumnWithTypeAndName& delete_column, size_t delete_row) {
    if (!is_equality_delete_byte_type(data_column.type) ||
        !is_equality_delete_byte_type(delete_column.type)) {
        return data_column.column->compare_at(data_row, delete_row, *delete_column.column, -1) == 0;
    }
    const ByteKeyColumn data_keys(*data_column.column);
    const ByteKeyColumn delete_keys(*delete_column.column);
    if (data_keys.is_null(data_row) || delete_keys.is_null(delete_row)) {
        return data_keys.is_null(data_row) && delete_keys.is_null(delete_row);
    }
    return data_keys.value(data_row) == delete_keys.value(delete_row);
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
        // create_set has no VARBINARY set (it throws NOT_IMPLEMENTED), so every byte key goes into a
        // STRING set as bytes, whichever of STRING and VARBINARY the delete file was read as.
        _hybrid_set.reset(create_set(TYPE_STRING, non_null_rows, true));
        insert_byte_values(*column_and_type.column, _hybrid_set.get());
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

    const auto& block_col_name = id_to_block_column_name.at(column_field_id);
    auto block_idx = col_name_to_block_idx->at(block_col_name);

    auto column_and_type = data_block->get_by_position(block_idx);
    const bool delete_is_byte =
            is_equality_delete_byte_type(_delete_block->get_by_position(0).type);
    const bool byte_compatible =
            delete_is_byte && is_equality_delete_byte_type(column_and_type.type);
    // The byte set can only be probed with bytes: an INT data column would be read as StringRef values.
    if (delete_is_byte && !byte_compatible) [[unlikely]] {
        return Status::InternalError(
                "Not support type change in column '{}', src type: {}, target type: {}",
                column_and_type.name, _delete_block->get_by_position(0).type->get_name(),
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
    auto column = column_and_type.column->convert_to_full_column_if_const();
    if (byte_compatible) {
        find_byte_values(*column, *_hybrid_set, _single_filter.get());
    } else if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
        const NullMap& null_map = nullable->get_null_map_data();
        _hybrid_set->find_batch_nullable(*remove_nullable(column), rows, null_map, *_single_filter);
        if (_hybrid_set->contain_null()) {
            auto* filter_data = _single_filter->data();
            for (size_t i = 0; i < rows; ++i) {
                filter_data[i] = filter_data[i] || null_map[i];
            }
        }
    } else {
        _hybrid_set->find_batch(*column, rows, *_single_filter);
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
        // A STRING delete key may meet a VARBINARY data column for the same Iceberg BINARY field: both are
        // hashed and compared as bytes, so only a change to or from a non-byte type is rejected.
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
        if (!equality_delete_values_equal(data_block->get_by_position(_data_column_index[i]),
                                          data_row_index, _delete_block->get_by_position(i),
                                          delete_row_index)) {
            return false;
        }
    }
    return true;
}

} // namespace doris

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

#include <parallel_hashmap/phmap.h>

#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_map.h"
#include "core/string_ref.h"
#include "exec/common/hash_table/hash.h"
#include "exec/common/util.hpp"
#include "exprs/function/array/function_array_distance.h"

namespace doris {

namespace detail {

template <PrimitiveType KeyType>
struct InnerProductMapKeyTraits {
    using ColumnType = PrimitiveTypeTraits<KeyType>::ColumnType;
    using Key = PrimitiveTypeTraits<KeyType>::CppType;
    using KeyAccessor = const Key*;
    using Hash = HashCRC32<Key>;

    static KeyAccessor get_key_accessor(const ColumnType& column) {
        return column.get_data().data();
    }

    static Key get_key(KeyAccessor keys, size_t index) { return keys[index]; }
};

template <>
struct InnerProductMapKeyTraits<TYPE_STRING> {
    using ColumnType = ColumnString;
    using Key = StringRef;
    using KeyAccessor = const ColumnType*;
    using Hash = StringRefHash;

    static KeyAccessor get_key_accessor(const ColumnType& column) { return &column; }

    static Key get_key(KeyAccessor keys, size_t index) { return keys->get_data_at(index); }
};

} // namespace detail

class FunctionInnerProduct final : public FunctionArrayDistance<InnerProduct> {
public:
    static FunctionPtr create() { return std::make_shared<FunctionInnerProduct>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() != 2) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Invalid number of arguments");
        }

        const bool both_arrays = arguments[0]->get_primitive_type() == TYPE_ARRAY &&
                                 arguments[1]->get_primitive_type() == TYPE_ARRAY;
        if (both_arrays) {
            return FunctionArrayDistance<InnerProduct>::get_return_type_impl(arguments);
        }

        const bool both_maps = arguments[0]->get_primitive_type() == TYPE_MAP &&
                               arguments[1]->get_primitive_type() == TYPE_MAP;
        if (!both_maps) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Arguments for function {} must be arrays or maps", get_name());
        }

        const auto& left_type = assert_cast<const DataTypeMap&>(*remove_nullable(arguments[0]));
        const auto& right_type = assert_cast<const DataTypeMap&>(*remove_nullable(arguments[1]));
        if (!left_type.get_key_type()->equals(*right_type.get_key_type())) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Map keys for function {} must have the same type", get_name());
        }
        const auto key_type = remove_nullable(left_type.get_key_type())->get_primitive_type();
        if (!_is_supported_map_key_type(key_type)) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Function {} only supports integer or string map keys",
                                   get_name());
        }
        if (remove_nullable(left_type.get_value_type())->get_primitive_type() != TYPE_FLOAT ||
            remove_nullable(right_type.get_value_type())->get_primitive_type() != TYPE_FLOAT) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Map values for function {} must be FLOAT", get_name());
        }
        return std::make_shared<DataTypeFloat32>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        if (block.get_by_position(arguments[0]).type->get_primitive_type() == TYPE_MAP) {
            return _execute_map(block, arguments, result, input_rows_count);
        }
        return FunctionArrayDistance<InnerProduct>::execute_impl(context, block, arguments, result,
                                                                 input_rows_count);
    }

private:
    using ColumnType = PrimitiveTypeTraits<TYPE_FLOAT>::ColumnType;

    struct MapRange {
        size_t begin;
        size_t size;
    };

    static ALWAYS_INLINE MapRange _get_map_range(const ColumnMap& map, bool is_const, size_t row) {
        const size_t actual_row = index_check_const(row, is_const);
        return {map.offset_at(actual_row), map.size_at(actual_row)};
    }

    static bool _is_supported_map_key_type(PrimitiveType type) {
        switch (type) {
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING:
            return true;
        default:
            return false;
        }
    }

    static const ColumnMap& _get_map_column(const ColumnPtr& column, const char* argument_name,
                                            const String& function_name, bool& is_const) {
        const IColumn* raw_column = column.get();
        is_const = is_column_const(*raw_column);
        if (is_const) {
            raw_column = assert_cast<const ColumnConst*>(raw_column)->get_data_column_ptr().get();
        }

        if (const auto* nullable = check_and_get_column<ColumnNullable>(raw_column)) {
            if (raw_column->has_null()) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "{} for function {} cannot be null", argument_name,
                                       function_name);
            }
            raw_column = nullable->get_nested_column_ptr().get();
        }

        const auto& map = assert_cast<const ColumnMap&>(*raw_column);
        if (map.get_values().has_null()) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "{} for function {} cannot have null", argument_name,
                                   function_name);
        }
        return map;
    }

    static const IColumn& _get_key_column(const IColumn& column, const UInt8*& null_map) {
        null_map = nullptr;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(&column)) {
            null_map = nullable->get_null_map_data().data();
            return nullable->get_nested_column();
        }
        return column;
    }

    template <PrimitiveType KeyType>
    static void _execute_map_typed(const ColumnMap& left, bool left_is_const,
                                   const ColumnMap& right, bool right_is_const,
                                   ColumnType::Container& destination_data,
                                   size_t input_rows_count) {
        using KeyTraits = detail::InnerProductMapKeyTraits<KeyType>;
        using Key = typename KeyTraits::Key;
        using KeyAccessor = typename KeyTraits::KeyAccessor;
        using KeyColumn = typename KeyTraits::ColumnType;

        const UInt8* left_key_null_map = nullptr;
        const UInt8* right_key_null_map = nullptr;
        const auto& left_keys =
                assert_cast<const KeyColumn&>(_get_key_column(left.get_keys(), left_key_null_map));
        const auto& right_keys = assert_cast<const KeyColumn&>(
                _get_key_column(right.get_keys(), right_key_null_map));
        const IColumn* left_values_column = &left.get_values();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(left_values_column)) {
            left_values_column = &nullable->get_nested_column();
        }
        const IColumn* right_values_column = &right.get_values();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(right_values_column)) {
            right_values_column = &nullable->get_nested_column();
        }
        const auto& left_values = assert_cast<const ColumnType&>(*left_values_column).get_data();
        const auto& right_values = assert_cast<const ColumnType&>(*right_values_column).get_data();

        struct MapData {
            KeyAccessor keys;
            const UInt8* key_null_map;
            const float* values;
        };

        const MapData left_data {KeyTraits::get_key_accessor(left_keys), left_key_null_map,
                                 left_values.data()};
        const MapData right_data {KeyTraits::get_key_accessor(right_keys), right_key_null_map,
                                  right_values.data()};

        // Build the hash table from the smaller map row to minimize temporary memory.
        phmap::flat_hash_map<Key, float, typename KeyTraits::Hash> values_by_key;
        for (size_t row = 0; row < input_rows_count; ++row) {
            const MapRange left_range = _get_map_range(left, left_is_const, row);
            const MapRange right_range = _get_map_range(right, right_is_const, row);
            const bool build_left = left_range.size <= right_range.size;
            const MapData build = build_left ? left_data : right_data;
            const MapData probe = build_left ? right_data : left_data;
            const MapRange build_range = build_left ? left_range : right_range;
            const MapRange probe_range = build_left ? right_range : left_range;

            values_by_key.clear();
            values_by_key.reserve(build_range.size);
            bool has_null_key = false;
            float null_key_value = 0.0F;
            for (size_t i = build_range.begin; i < build_range.begin + build_range.size; ++i) {
                if (build.key_null_map != nullptr && build.key_null_map[i]) {
                    has_null_key = true;
                    null_key_value = build.values[i];
                } else {
                    values_by_key[KeyTraits::get_key(build.keys, i)] = build.values[i];
                }
            }

            float inner_product = 0.0F;
            for (size_t i = probe_range.begin; i < probe_range.begin + probe_range.size; ++i) {
                if (probe.key_null_map != nullptr && probe.key_null_map[i]) {
                    if (has_null_key) {
                        inner_product += null_key_value * probe.values[i];
                    }
                    continue;
                }
                const auto it = values_by_key.find(KeyTraits::get_key(probe.keys, i));
                if (it != values_by_key.end()) {
                    inner_product += it->second * probe.values[i];
                }
            }
            destination_data[row] = inner_product;
        }
    }

    Status _execute_map(Block& block, const ColumnNumbers& arguments, uint32_t result,
                        size_t input_rows_count) const {
        bool left_is_const = false;
        bool right_is_const = false;
        const auto& left = _get_map_column(block.get_by_position(arguments[0]).column,
                                           "First argument", get_name(), left_is_const);
        const auto& right = _get_map_column(block.get_by_position(arguments[1]).column,
                                            "Second argument", get_name(), right_is_const);

        auto destination = ColumnType::create(input_rows_count);
        auto& destination_data = destination->get_data();
        const auto& map_type = assert_cast<const DataTypeMap&>(
                *remove_nullable(block.get_by_position(arguments[0]).type));
        switch (remove_nullable(map_type.get_key_type())->get_primitive_type()) {
        case TYPE_TINYINT:
            _execute_map_typed<TYPE_TINYINT>(left, left_is_const, right, right_is_const,
                                             destination_data, input_rows_count);
            break;
        case TYPE_SMALLINT:
            _execute_map_typed<TYPE_SMALLINT>(left, left_is_const, right, right_is_const,
                                              destination_data, input_rows_count);
            break;
        case TYPE_INT:
            _execute_map_typed<TYPE_INT>(left, left_is_const, right, right_is_const,
                                         destination_data, input_rows_count);
            break;
        case TYPE_BIGINT:
            _execute_map_typed<TYPE_BIGINT>(left, left_is_const, right, right_is_const,
                                            destination_data, input_rows_count);
            break;
        case TYPE_LARGEINT:
            _execute_map_typed<TYPE_LARGEINT>(left, left_is_const, right, right_is_const,
                                              destination_data, input_rows_count);
            break;
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING:
            _execute_map_typed<TYPE_STRING>(left, left_is_const, right, right_is_const,
                                            destination_data, input_rows_count);
            break;
        default:
            return Status::InvalidArgument("Function {} only supports integer or string map keys",
                                           get_name());
        }

        block.replace_by_position(result, std::move(destination));
        return Status::OK();
    }
};

} // namespace doris

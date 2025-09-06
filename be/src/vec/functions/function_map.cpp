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

#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include "common/status.h"
#include "runtime/primitive_type.h"
#include "util/simd/vstring_function.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/functions/array/function_array_index.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

// construct a map
// map(key1, value2, key2, value2) -> {key1: value2, key2: value2}
class FunctionMap : public IFunction {
public:
    static constexpr auto name = "map";
    static FunctionPtr create() { return std::make_shared<FunctionMap>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments.size() % 2 == 0)
                << "function: " << get_name() << ", arguments should not be even number";
        return std::make_shared<DataTypeMap>(make_nullable(arguments[0]),
                                             make_nullable(arguments[1]));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK(arguments.size() % 2 == 0)
                << "function: " << get_name() << ", arguments should not be even number";

        size_t num_element = arguments.size();

        auto result_col = block.get_by_position(result).type->create_column();
        auto* map_column = assert_cast<ColumnMap*>(result_col.get());

        // map keys column
        auto& result_col_map_keys_data = map_column->get_keys();
        result_col_map_keys_data.reserve(input_rows_count * num_element / 2);
        // map values column
        auto& result_col_map_vals_data = map_column->get_values();
        result_col_map_vals_data.reserve(input_rows_count * num_element / 2);
        // map offsets column
        auto& result_col_map_offsets = map_column->get_offsets();
        result_col_map_offsets.resize(input_rows_count);

        std::unique_ptr<bool[]> col_const = std::make_unique_for_overwrite<bool[]>(num_element);
        std::vector<ColumnPtr> arg(num_element);
        for (size_t i = 0; i < num_element; ++i) {
            auto& col = block.get_by_position(arguments[i]).column;
            std::tie(col, col_const[i]) = unpack_if_const(col);
            bool is_nullable = i % 2 == 0 ? result_col_map_keys_data.is_nullable()
                                          : result_col_map_vals_data.is_nullable();
            // convert to nullable column
            arg[i] = col;
            if (is_nullable && !col->is_nullable()) {
                arg[i] = ColumnNullable::create(col, ColumnUInt8::create(col->size(), 0));
            }
        }

        // insert value into map
        ColumnArray::Offset64 offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row) {
            for (size_t i = 0; i < num_element; i += 2) {
                result_col_map_keys_data.insert_from(*arg[i], index_check_const(row, col_const[i]));
                result_col_map_vals_data.insert_from(*arg[i + 1],
                                                     index_check_const(row, col_const[i + 1]));
            }
            offset += num_element / 2;
            result_col_map_offsets[row] = offset;
        }
        block.replace_by_position(result, std::move(result_col));
        return Status::OK();
    }
};

template <bool is_key, bool OldVersion = false>
class FunctionMapContains : public IFunction {
public:
    static constexpr auto name = is_key ? "map_contains_key" : "map_contains_value";
    static FunctionPtr create() { return std::make_shared<FunctionMapContains>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override {
        return array_contains.use_default_implementation_for_nulls();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr datatype = arguments[0];
        if (datatype->is_nullable()) {
            datatype = assert_cast<const DataTypeNullable*>(datatype.get())->get_nested_type();
        }
        DCHECK(datatype->get_primitive_type() == TYPE_MAP)
                << "first argument for function: " << name << " should be DataTypeMap";

        if constexpr (OldVersion) {
            return make_nullable(std::make_shared<DataTypeBool>());
        } else {
            if (arguments[0]->is_nullable()) {
                return make_nullable(std::make_shared<DataTypeBool>());
            } else {
                return std::make_shared<DataTypeBool>();
            }
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        // backup original argument 0
        auto orig_arg0 = block.get_by_position(arguments[0]);
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnMap* map_column = nullptr;
        ColumnPtr nullmap_column = nullptr;
        if (left_column->is_nullable()) {
            auto nullable_column = reinterpret_cast<const ColumnNullable*>(left_column.get());
            map_column = check_and_get_column<ColumnMap>(nullable_column->get_nested_column());
            nullmap_column = nullable_column->get_null_map_column_ptr();
        } else {
            map_column = check_and_get_column<ColumnMap>(*left_column.get());
        }
        if (!map_column) {
            return Status::RuntimeError("unsupported types for function {}({})", get_name(),
                                        block.get_by_position(arguments[0]).type->get_name());
        }

        DataTypePtr datatype = block.get_by_position(arguments[0]).type;
        if (datatype->is_nullable()) {
            datatype = assert_cast<const DataTypeNullable*>(datatype.get())->get_nested_type();
        }
        const auto datatype_map = static_cast<const DataTypeMap*>(datatype.get());
        if constexpr (is_key) {
            const auto& array_column = map_column->get_keys_array_ptr();
            const auto datatype_array =
                    std::make_shared<DataTypeArray>(datatype_map->get_key_type());
            if (nullmap_column) {
                block.get_by_position(arguments[0]) = {
                        ColumnNullable::create(array_column, nullmap_column),
                        make_nullable(datatype_array),
                        block.get_by_position(arguments[0]).name + ".keys"};
            } else {
                block.get_by_position(arguments[0]) = {
                        array_column, datatype_array,
                        block.get_by_position(arguments[0]).name + ".keys"};
            }
        } else {
            const auto& array_column = map_column->get_values_array_ptr();
            const auto datatype_array =
                    std::make_shared<DataTypeArray>(datatype_map->get_value_type());
            if (nullmap_column) {
                block.get_by_position(arguments[0]) = {
                        ColumnNullable::create(array_column, nullmap_column),
                        make_nullable(datatype_array),
                        block.get_by_position(arguments[0]).name + ".values"};
            } else {
                block.get_by_position(arguments[0]) = {
                        array_column, datatype_array,
                        block.get_by_position(arguments[0]).name + ".values"};
            }
        }

        RETURN_IF_ERROR(
                array_contains.execute_impl(context, block, arguments, result, input_rows_count));

        // restore original argument 0
        block.get_by_position(arguments[0]) = orig_arg0;
        return Status::OK();
    }

private:
    FunctionArrayIndex<ArrayContainsAction> array_contains;
};

template <bool is_key>
class FunctionMapKeysOrValues : public IFunction {
public:
    static constexpr auto name = is_key ? "map_keys" : "map_values";
    static FunctionPtr create() { return std::make_shared<FunctionMapKeysOrValues>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr datatype = arguments[0];
        if (datatype->is_nullable()) {
            datatype = assert_cast<const DataTypeNullable*>(datatype.get())->get_nested_type();
        }
        DCHECK(datatype->get_primitive_type() == TYPE_MAP)
                << "first argument for function: " << name << " should be DataTypeMap";
        const auto datatype_map = static_cast<const DataTypeMap*>(datatype.get());
        if (is_key) {
            return std::make_shared<DataTypeArray>(datatype_map->get_key_type());
        } else {
            return std::make_shared<DataTypeArray>(datatype_map->get_value_type());
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnMap* map_column = nullptr;
        if (left_column->is_nullable()) {
            auto nullable_column = reinterpret_cast<const ColumnNullable*>(left_column.get());
            map_column = check_and_get_column<ColumnMap>(nullable_column->get_nested_column());
        } else {
            map_column = check_and_get_column<ColumnMap>(*left_column.get());
        }
        if (!map_column) {
            return Status::RuntimeError("unsupported types for function {}({})", get_name(),
                                        block.get_by_position(arguments[0]).type->get_name());
        }

        if constexpr (is_key) {
            block.replace_by_position(result, map_column->get_keys_array_ptr());
        } else {
            block.replace_by_position(result, map_column->get_values_array_ptr());
        }

        return Status::OK();
    }
};

class FunctionMapEntries : public IFunction {
public:
    static constexpr auto name = "map_entries";
    static FunctionPtr create() { return std::make_shared<FunctionMapEntries>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        const auto* const datatype_map = assert_cast<const DataTypeMap*>(arguments[0].get());

        // Create struct type with named fields "key" and "value"
        // key and value are always nullable
        auto struct_type = std::make_shared<DataTypeStruct>(
                DataTypes {make_nullable(datatype_map->get_key_type()),
                           make_nullable(datatype_map->get_value_type())},
                Strings {"key", "value"});

        // Theoretically, the struct element will never be null,
        // but FE expects the array element to be nullable
        return std::make_shared<DataTypeArray>(make_nullable(struct_type));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto* map_column =
                assert_cast<const ColumnMap*>(block.get_by_position(arguments[0]).column.get());

        auto struct_column = ColumnStruct::create(
                Columns {map_column->get_keys_ptr(), map_column->get_values_ptr()});

        // all struct elements are not null
        auto struct_null_map = ColumnUInt8::create(struct_column->size(), 0);
        auto nullable_struct_column =
                ColumnNullable::create(std::move(struct_column), std::move(struct_null_map));

        auto result_array_column = ColumnArray::create(std::move(nullable_struct_column),
                                                       map_column->get_offsets_ptr());

        block.replace_by_position(result, std::move(result_array_column));

        return Status::OK();
    }
};

class FunctionStrToMap : public IFunction {
public:
    static constexpr auto name = "str_to_map";
    static FunctionPtr create() { return std::make_shared<FunctionStrToMap>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()),
                                             make_nullable(std::make_shared<DataTypeString>()));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK(arguments.size() == 3);

        bool cols_const[2];
        ColumnPtr cols[2];
        for (size_t i = 0; i < 2; ++i) {
            cols_const[i] = is_column_const(*block.get_by_position(arguments[i]).column);
        }
        // convert to full column if necessary
        default_preprocess_parameter_columns(cols, cols_const, {0, 1}, block, arguments);
        const auto& [col3, col3_const] =
                unpack_if_const(block.get_by_position(arguments[2]).column);

        const auto& str_column = assert_cast<const ColumnString*>(cols[0].get());
        const auto& pair_delim_column = assert_cast<const ColumnString*>(cols[1].get());
        const auto& kv_delim_column = assert_cast<const ColumnString*>(col3.get());

        ColumnPtr result_col;
        if (cols_const[0] && cols_const[1]) {
            result_col = execute_vector<true, false>(input_rows_count, *str_column,
                                                     *pair_delim_column, *kv_delim_column);
        } else if (col3_const) {
            result_col = execute_vector<false, true>(input_rows_count, *str_column,
                                                     *pair_delim_column, *kv_delim_column);
        } else {
            result_col = execute_vector<false, false>(input_rows_count, *str_column,
                                                      *pair_delim_column, *kv_delim_column);
        }

        block.replace_by_position(result, std::move(result_col));

        return Status::OK();
    }

private:
    template <bool is_str_and_pair_delim_const, bool is_kv_delim_const>
    static ColumnPtr execute_vector(const size_t input_rows_count, const ColumnString& str_col,
                                    const ColumnString& pair_delim_col,
                                    const ColumnString& kv_delim_col) {
        // map keys column
        auto result_col_map_keys_data =
                ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
        result_col_map_keys_data->reserve(input_rows_count);
        // map values column
        auto result_col_map_vals_data =
                ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
        result_col_map_vals_data->reserve(input_rows_count);
        // map offsets column
        auto result_col_map_offsets = ColumnOffset64::create();
        result_col_map_offsets->reserve(input_rows_count);

        std::vector<std::string_view> kvs;
        std::string_view kv_delim;
        if constexpr (is_str_and_pair_delim_const) {
            auto str = str_col.get_data_at(0).to_string_view();
            auto pair_delim = pair_delim_col.get_data_at(0).to_string_view();
            kvs = split_pair_by_delim(str, pair_delim);
        }
        if constexpr (is_kv_delim_const) {
            kv_delim = kv_delim_col.get_data_at(0).to_string_view();
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if constexpr (!is_str_and_pair_delim_const) {
                auto str = str_col.get_data_at(i).to_string_view();
                auto pair_delim = pair_delim_col.get_data_at(i).to_string_view();
                kvs = split_pair_by_delim(str, pair_delim);
            }
            if constexpr (!is_kv_delim_const) {
                kv_delim = kv_delim_col.get_data_at(i).to_string_view();
            }

            for (const auto& kv : kvs) {
                auto kv_parts = split_kv_by_delim(kv, kv_delim);
                if (kv_parts.size() == 2) {
                    result_col_map_keys_data->insert_data(kv_parts[0].data(), kv_parts[0].size());
                    result_col_map_vals_data->insert_data(kv_parts[1].data(), kv_parts[1].size());
                } else {
                    result_col_map_keys_data->insert_data(kv.data(), kv.size());
                    result_col_map_vals_data->insert_default();
                }
            }
            result_col_map_offsets->insert_value(result_col_map_keys_data->size());
        }

        return ColumnMap::create(std::move(result_col_map_keys_data),
                                 std::move(result_col_map_vals_data),
                                 std::move(result_col_map_offsets));
    }

    static std::vector<std::string_view> split_pair_by_delim(const std::string_view& str,
                                                             const std::string_view& delim) {
        if (str.empty()) {
            return {str};
        }
        if (delim.empty()) {
            std::vector<std::string_view> result;
            size_t offset = 0;
            while (offset < str.size()) {
                auto len = get_utf8_byte_length(str[offset]);
                result.push_back(str.substr(offset, len));
                offset += len;
            }
            return result;
        }
        std::vector<std::string_view> result;
        size_t offset = 0;
        while (offset < str.size()) {
            auto pos = str.find(delim, offset);
            if (pos == std::string::npos) {
                result.push_back(str.substr(offset));
                break;
            }
            result.push_back(str.substr(offset, pos - offset));
            offset = pos + delim.size();
        }
        return result;
    }

    static std::vector<std::string_view> split_kv_by_delim(const std::string_view& str,
                                                           const std::string_view& delim) {
        if (str.empty()) {
            return {str};
        }
        if (delim.empty()) {
            auto len = get_utf8_byte_length(str[0]);
            return {str.substr(0, len), str.substr(len)};
        }
        auto pos = str.find(delim);
        if (pos == std::string::npos) {
            return {str};
        } else {
            return {str.substr(0, pos), str.substr(pos + delim.size())};
        }
    }
};

class FunctionMapContainsEntry : public IFunction {
public:
    static constexpr auto name = "map_contains_entry";
    static FunctionPtr create() { return std::make_shared<FunctionMapContainsEntry>(); }

    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr datatype = arguments[0];
        if (datatype->is_nullable()) {
            datatype = assert_cast<const DataTypeNullable*>(datatype.get())->get_nested_type();
        }
        DCHECK(datatype->get_primitive_type() == TYPE_MAP)
                << "first argument for function: " << name << " should be DataTypeMap";

        if (arguments[0]->is_nullable()) {
            return make_nullable(std::make_shared<DataTypeBool>());
        } else {
            return std::make_shared<DataTypeBool>();
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return _execute_type_check_and_dispatch(block, arguments, result);
    }

private:
    // assume result_matches is initialized to all 1s
    template <typename ColumnType>
    void _execute_column_comparison(const IColumn& map_entry_column, const UInt8* map_entry_nullmap,
                                    const IColumn& search_column, const UInt8* search_nullmap,
                                    const ColumnArray::Offsets64& map_offsets,
                                    const UInt8* map_row_nullmap, bool search_is_const,
                                    ColumnUInt8& result_matches) const {
        auto& result_data = result_matches.get_data();
        for (size_t row = 0; row < map_offsets.size(); ++row) {
            if (map_row_nullmap && map_row_nullmap[row]) {
                continue;
            }
            size_t map_start = row == 0 ? 0 : map_offsets[row - 1];
            size_t map_end = map_offsets[row];
            // const column always uses index 0
            size_t search_idx = search_is_const ? 0 : row;
            for (size_t i = map_start; i < map_end; ++i) {
                result_data[i] &=
                        compare_values<ColumnType>(map_entry_column, i, map_entry_nullmap,
                                                   search_column, search_idx, search_nullmap)
                                ? 1
                                : 0;
            }
        }
    }

    // dispatch column comparison by type, map_entry_column is the column of map's key or value, search_column is the column of search key or value
    void _dispatch_column_comparison(PrimitiveType type, const IColumn& map_entry_column,
                                     const UInt8* map_entry_nullmap, const IColumn& search_column,
                                     const UInt8* search_nullmap,
                                     const ColumnArray::Offsets64& map_offsets,
                                     const UInt8* map_row_nullmap, bool search_is_const,
                                     ColumnUInt8& result_matches) const {
        switch (type) {
        case TYPE_BOOLEAN:
            _execute_column_comparison<ColumnUInt8>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_TINYINT:
            _execute_column_comparison<ColumnInt8>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_SMALLINT:
            _execute_column_comparison<ColumnInt16>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_INT:
            _execute_column_comparison<ColumnInt32>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_BIGINT:
            _execute_column_comparison<ColumnInt64>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_LARGEINT:
            _execute_column_comparison<ColumnInt128>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_FLOAT:
            _execute_column_comparison<ColumnFloat32>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_DOUBLE:
            _execute_column_comparison<ColumnFloat64>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_DECIMAL32:
            _execute_column_comparison<ColumnDecimal32>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_DECIMAL64:
            _execute_column_comparison<ColumnDecimal64>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_DECIMAL128I:
            _execute_column_comparison<ColumnDecimal128V3>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_DECIMALV2:
            _execute_column_comparison<ColumnDecimal128V2>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_DECIMAL256:
            _execute_column_comparison<ColumnDecimal256>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_STRING:
        case TYPE_CHAR:
        case TYPE_VARCHAR:
            _execute_column_comparison<ColumnString>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_DATE:
            _execute_column_comparison<ColumnDate>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_DATETIME:
            _execute_column_comparison<ColumnDateTime>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_DATEV2:
            _execute_column_comparison<ColumnDateV2>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_DATETIMEV2:
            _execute_column_comparison<ColumnDateTimeV2>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_TIME:
            _execute_column_comparison<ColumnTime>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_TIMEV2:
            _execute_column_comparison<ColumnTimeV2>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_IPV4:
            _execute_column_comparison<ColumnIPv4>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        case TYPE_IPV6:
            _execute_column_comparison<ColumnIPv6>(
                    map_entry_column, map_entry_nullmap, search_column, search_nullmap, map_offsets,
                    map_row_nullmap, search_is_const, result_matches);
            break;
        default:
            // We have done type check before dispatching, so this should not happen
            DCHECK(false) << "Dispatching unsupported primitive type in " << get_name() << ": "
                          << static_cast<int>(type);
            break;
        }
    }

    // main loop function
    ColumnPtr _execute_all_rows(const ColumnMap* map_column, const ColumnPtr& map_row_nullmap_col,
                                const IColumn& key_column, const UInt8* key_nullmap,
                                const IColumn& value_column, const UInt8* value_nullmap,
                                PrimitiveType key_type, PrimitiveType value_type, bool key_is_const,
                                bool value_is_const) const {
        const auto& map_offsets = map_column->get_offsets();

        // remove the nullable wrapper of map's key and value
        const auto& map_keys_nullable =
                reinterpret_cast<const ColumnNullable&>(map_column->get_keys());
        const IColumn* map_keys_column = &map_keys_nullable.get_nested_column();
        const auto& map_keys_nullmap = map_keys_nullable.get_null_map_column().get_data().data();

        const auto& map_values_nullable =
                reinterpret_cast<const ColumnNullable&>(map_column->get_values());
        const IColumn* map_values_column = &map_values_nullable.get_nested_column();
        const auto& map_values_nullmap =
                map_values_nullable.get_null_map_column().get_data().data();

        auto result_column = ColumnUInt8::create(map_offsets.size(), 0);
        auto& result_data = result_column->get_data();

        const UInt8* map_row_nullmap = nullptr;
        if (map_row_nullmap_col) {
            map_row_nullmap =
                    assert_cast<const ColumnUInt8&>(*map_row_nullmap_col).get_data().data();
        }

        auto matches = ColumnUInt8::create(map_keys_column->size(), 1);

        // matches &= key_compare
        _dispatch_column_comparison(key_type, *map_keys_column, map_keys_nullmap, key_column,
                                    key_nullmap, map_offsets, map_row_nullmap, key_is_const,
                                    *matches);

        // matches &= value_compare
        _dispatch_column_comparison(value_type, *map_values_column, map_values_nullmap,
                                    value_column, value_nullmap, map_offsets, map_row_nullmap,
                                    value_is_const, *matches);

        // aggregate results by map boundaries
        auto& matches_data = matches->get_data();
        for (size_t row = 0; row < map_offsets.size(); ++row) {
            if (map_row_nullmap && map_row_nullmap[row]) {
                // result is null for this row
                continue;
            }

            size_t map_start = row == 0 ? 0 : map_offsets[row - 1];
            size_t map_end = map_offsets[row];

            bool found = false;
            for (size_t i = map_start; i < map_end && !found; ++i) {
                if (matches_data[i]) {
                    found = true;
                    break;
                }
            }
            result_data[row] = found;
        }

        if (map_row_nullmap_col) {
            return ColumnNullable::create(std::move(result_column), map_row_nullmap_col);
        }
        return result_column;
    }

    // type comparability check and dispatch
    Status _execute_type_check_and_dispatch(Block& block, const ColumnNumbers& arguments,
                                            uint32_t result) const {
        // get type information
        auto map_type = remove_nullable(block.get_by_position(arguments[0]).type);
        const auto* map_datatype = assert_cast<const DataTypeMap*>(map_type.get());
        auto map_key_type = remove_nullable(map_datatype->get_key_type());
        auto map_value_type = remove_nullable(map_datatype->get_value_type());
        auto search_key_type = remove_nullable(block.get_by_position(arguments[1]).type);
        auto search_value_type = remove_nullable(block.get_by_position(arguments[2]).type);

        bool key_types_comparable = type_comparable(map_key_type->get_primitive_type(),
                                                    search_key_type->get_primitive_type());
        bool value_types_comparable = type_comparable(map_value_type->get_primitive_type(),
                                                      search_value_type->get_primitive_type());

        // if types are not comparable, return error
        if (!key_types_comparable || !value_types_comparable) {
            return Status::RuntimeError(
                    "Type mismatch for function {}. "
                    "Map key type: {}, search key type: {}. "
                    "Map value type: {}, search value type: {}.",
                    get_name(), map_key_type->get_name(), search_key_type->get_name(),
                    map_value_type->get_name(), search_value_type->get_name());
        }

        // type check passed, extract columns and execute
        // extract map column
        auto map_column_ptr =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnMap* map_column = nullptr;
        ColumnPtr map_row_nullmap_col = nullptr;
        if (map_column_ptr->is_nullable()) {
            const auto* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(map_column_ptr.get());
            map_column = check_and_get_column<ColumnMap>(nullable_column->get_nested_column());
            map_row_nullmap_col = nullable_column->get_null_map_column_ptr();
        } else {
            map_column = check_and_get_column<ColumnMap>(*map_column_ptr.get());
        }
        if (!map_column) {
            return Status::RuntimeError("unsupported types for function {}({})", get_name(),
                                        block.get_by_position(arguments[0]).type->get_name());
        }

        // extract (search) key and value columns
        const auto& [key_column_ptr, key_is_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        const auto& [value_column_ptr, value_is_const] =
                unpack_if_const(block.get_by_position(arguments[2]).column);

        const IColumn* key_column = nullptr;
        const IColumn* value_column = nullptr;
        const UInt8* key_nullmap = nullptr;
        const UInt8* value_nullmap = nullptr;

        if (key_column_ptr->is_nullable()) {
            const auto* nullable_column = assert_cast<const ColumnNullable*>(key_column_ptr.get());
            key_column = &nullable_column->get_nested_column();
            key_nullmap = nullable_column->get_null_map_column().get_data().data();
        } else {
            key_column = key_column_ptr.get();
        }

        if (value_column_ptr->is_nullable()) {
            const auto* nullable_column =
                    assert_cast<const ColumnNullable*>(value_column_ptr.get());
            value_column = &nullable_column->get_nested_column();
            value_nullmap = nullable_column->get_null_map_column().get_data().data();
        } else {
            value_column = value_column_ptr.get();
        }

        ColumnPtr return_column = _execute_all_rows(
                map_column, map_row_nullmap_col, *key_column, key_nullmap, *value_column,
                value_nullmap, map_key_type->get_primitive_type(),
                map_value_type->get_primitive_type(), key_is_const, value_is_const);

        if (return_column) {
            block.replace_by_position(result, std::move(return_column));
            return Status::OK();
        }

        return Status::RuntimeError(
                "execute failed or unsupported types for function {}({}, {}, {})", get_name(),
                block.get_by_position(arguments[0]).type->get_name(),
                block.get_by_position(arguments[1]).type->get_name(),
                block.get_by_position(arguments[2]).type->get_name());
    }

    // generic type-specialized comparison function
    template <typename ColumnType>
    bool compare_values(const IColumn& left_col, size_t left_idx, const UInt8* left_nullmap,
                        const IColumn& right_col, size_t right_idx,
                        const UInt8* right_nullmap) const {
        // handle null values
        bool left_is_null = left_nullmap && left_nullmap[left_idx];
        bool right_is_null = right_nullmap && right_nullmap[right_idx];

        if (left_is_null && right_is_null) {
            return true;
        }
        if (left_is_null || right_is_null) {
            return false;
        }

        // use compare_at from typed column
        const auto& typed_left_col = assert_cast<const ColumnType&>(left_col);
        const auto& typed_right_col = assert_cast<const ColumnType&>(right_col);
        return typed_left_col.compare_at(left_idx, right_idx, typed_right_col,
                                         /*nan_direction_hint=*/1) == 0;
    }

    // type compatibility check
    bool type_comparable(PrimitiveType left_type, PrimitiveType right_type) const {
        // all string types use ColumnString, so they are comparable
        if (is_string_type(left_type) && is_string_type(right_type)) {
            return true;
        }

        // other types must be the same, and in the supported list
        if (left_type != right_type) {
            return false;
        }

        if (is_number(left_type)) {
            return true;
        }
        if (is_date_type(left_type)) {
            return true;
        }
        if (is_time_type(left_type)) {
            return true;
        }
        if (is_ip(left_type)) {
            return true;
        }

        return false;
    }
};

void register_function_map(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMap>();
    factory.register_function<FunctionMapContains<true>>();
    factory.register_function<FunctionMapContains<false>>();
    factory.register_function<FunctionMapKeysOrValues<true>>();
    factory.register_function<FunctionMapKeysOrValues<false>>();
    factory.register_function<FunctionMapEntries>();
    factory.register_function<FunctionStrToMap>();
    factory.register_function<FunctionMapContainsEntry>();
}

} // namespace doris::vectorized

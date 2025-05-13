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
#include "util/simd/vstring_function.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
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

        RETURN_IF_ERROR(map_column->deduplicate_keys());
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
            return make_nullable(std::make_shared<DataTypeNumber<UInt8>>());
        } else {
            if (arguments[0]->is_nullable()) {
                return make_nullable(std::make_shared<DataTypeNumber<UInt8>>());
            } else {
                return std::make_shared<DataTypeNumber<UInt8>>();
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
class FunctionMapEntries : public IFunction {
public:
    static constexpr auto name = is_key ? "map_keys" : "map_values";
    static FunctionPtr create() { return std::make_shared<FunctionMapEntries>(); }

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
        auto result_col_map_offsets = ColumnUInt64::create();
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

        auto map_column = ColumnMap::create(std::move(result_col_map_keys_data),
                                            std::move(result_col_map_vals_data),
                                            std::move(result_col_map_offsets));

        // `deduplicate_keys` always return ok
        static_cast<void>(map_column->deduplicate_keys());
        return map_column;
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

void register_function_map(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMap>();
    factory.register_function<FunctionMapContains<true>>();
    factory.register_function<FunctionMapContains<false>>();
    factory.register_function<FunctionMapEntries<true>>();
    factory.register_function<FunctionMapEntries<false>>();
    factory.register_function<FunctionStrToMap>();
}

} // namespace doris::vectorized

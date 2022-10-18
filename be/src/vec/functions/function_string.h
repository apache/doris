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

#ifndef USE_LIBCPP
#include <memory_resource>
#define PMR std::pmr
#else
#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include <boost/container/pmr/vector.hpp>
#define PMR boost::container::pmr
#endif

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <cstdint>
#include <string_view>

#include "exprs/math_functions.h"
#include "exprs/string_functions.h"
#include "udf/udf.h"
#include "util/md5.h"
#include "util/simd/vstring_function.h"
#include "util/sm3.h"
#include "util/url_parser.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

inline size_t get_utf8_byte_length(unsigned char byte) {
    size_t char_size = 0;
    if (byte < 0xC0) {
        char_size = 1;
    } else if (byte >= 0xF0) {
        char_size = 4;
    } else if (byte >= 0xE0) {
        char_size = 3;
    } else {
        char_size = 2;
    }
    return char_size;
}

inline size_t get_char_len(const std::string_view& str, std::vector<size_t>* str_index) {
    size_t char_len = 0;
    for (size_t i = 0, char_size = 0; i < str.length(); i += char_size) {
        char_size = get_utf8_byte_length(str[i]);
        str_index->push_back(i);
        ++char_len;
    }
    return char_len;
}

inline size_t get_char_len(const StringVal& str, std::vector<size_t>* str_index) {
    size_t char_len = 0;
    for (size_t i = 0, char_size = 0; i < str.len; i += char_size) {
        char_size = get_utf8_byte_length((unsigned)(str.ptr)[i]);
        str_index->push_back(i);
        ++char_len;
    }
    return char_len;
}

inline size_t get_char_len(const StringValue& str, size_t end_pos) {
    size_t char_len = 0;
    for (size_t i = 0, char_size = 0; i < std::min(str.len, end_pos); i += char_size) {
        char_size = get_utf8_byte_length((unsigned)(str.ptr)[i]);
        ++char_len;
    }
    return char_len;
}

struct StringOP {
    static void push_empty_string(int index, ColumnString::Chars& chars,
                                  ColumnString::Offsets& offsets) {
        offsets[index] = chars.size();
    }

    static void push_null_string(int index, ColumnString::Chars& chars,
                                 ColumnString::Offsets& offsets, NullMap& null_map) {
        null_map[index] = 1;
        push_empty_string(index, chars, offsets);
    }

    static void push_value_string(const std::string_view& string_value, int index,
                                  ColumnString::Chars& chars, ColumnString::Offsets& offsets) {
        chars.insert(string_value.data(), string_value.data() + string_value.size());
        offsets[index] = chars.size();
    }
};

struct SubstringUtil {
    static constexpr auto name = "substring";

    static void substring_execute(Block& block, const ColumnNumbers& arguments, size_t result,
                                  size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 3);
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        ColumnPtr argument_columns[3];

        for (int i = 0; i < 3; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[i])) {
                // Danger: Here must dispose the null map data first! Because
                // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
                // of column nullable mem of null map
                VectorizedUtils::update_null_map(null_map->get_data(),
                                                 nullable->get_null_map_data());
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        auto res = ColumnString::create();

        auto specific_str_column = assert_cast<const ColumnString*>(argument_columns[0].get());
        auto specific_start_column =
                assert_cast<const ColumnVector<Int32>*>(argument_columns[1].get());
        auto specific_len_column =
                assert_cast<const ColumnVector<Int32>*>(argument_columns[2].get());

        vector(specific_str_column->get_chars(), specific_str_column->get_offsets(),
               specific_start_column->get_data(), specific_len_column->get_data(),
               null_map->get_data(), res->get_chars(), res->get_offsets());

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
    }

private:
    static void vector(const ColumnString::Chars& chars, const ColumnString::Offsets& offsets,
                       const PaddedPODArray<Int32>& start, const PaddedPODArray<Int32>& len,
                       NullMap& null_map, ColumnString::Chars& res_chars,
                       ColumnString::Offsets& res_offsets) {
        int size = offsets.size();
        res_offsets.resize(size);
        res_chars.reserve(chars.size());

        std::array<std::byte, 128 * 1024> buf;
        PMR::monotonic_buffer_resource pool {buf.data(), buf.size()};
        PMR::vector<size_t> index {&pool};

        PMR::vector<std::pair<const unsigned char*, int>> strs(&pool);
        strs.resize(size);
        auto* __restrict data_ptr = chars.data();
        auto* __restrict offset_ptr = offsets.data();
        for (int i = 0; i < size; ++i) {
            strs[i].first = data_ptr + offset_ptr[i - 1];
            strs[i].second = offset_ptr[i] - offset_ptr[i - 1];
        }

        for (int i = 0; i < size; ++i) {
            auto [raw_str, str_size] = strs[i];
            // return empty string if start > src.length
            if (start[i] > str_size || str_size == 0 || start[i] == 0 || len[i] <= 0) {
                StringOP::push_empty_string(i, res_chars, res_offsets);
                continue;
            }
            // reference to string_function.cpp: substring
            size_t byte_pos = 0;
            index.clear();
            for (size_t j = 0, char_size = 0; j < str_size; j += char_size) {
                char_size = get_utf8_byte_length((unsigned)(raw_str)[j]);
                index.push_back(j);
                if (start[i] > 0 && index.size() > start[i] + len[i]) {
                    break;
                }
            }

            int fixed_pos = start[i];
            if (fixed_pos < -(int)index.size()) {
                StringOP::push_empty_string(i, res_chars, res_offsets);
                continue;
            }
            if (fixed_pos < 0) {
                fixed_pos = index.size() + fixed_pos + 1;
            }
            if (fixed_pos > index.size()) {
                StringOP::push_null_string(i, res_chars, res_offsets, null_map);
                continue;
            }

            byte_pos = index[fixed_pos - 1];
            int fixed_len = str_size - byte_pos;
            if (fixed_pos + len[i] <= index.size()) {
                fixed_len = index[fixed_pos + len[i] - 1] - byte_pos;
            }

            if (byte_pos <= str_size && fixed_len > 0) {
                // return StringVal(str.ptr + byte_pos, fixed_len);
                StringOP::push_value_string(
                        std::string_view {reinterpret_cast<const char*>(raw_str + byte_pos),
                                          (size_t)fixed_len},
                        i, res_chars, res_offsets);
            } else {
                StringOP::push_empty_string(i, res_chars, res_offsets);
            }
        }
    }
};

template <typename Impl>
class FunctionSubstring : public IFunction {
public:
    static constexpr auto name = SubstringUtil::name;
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionSubstring<Impl>>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }
    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct Substr3Impl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>(),
                std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        SubstringUtil::substring_execute(block, arguments, result, input_rows_count);
        return Status::OK();
    }
};

struct Substr2Impl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        auto params = ColumnInt32::create(input_rows_count);
        auto& strlen_data = params->get_data();

        auto str_col =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*str_col)) {
            str_col = nullable->get_nested_column_ptr();
        }
        auto& str_offset = assert_cast<const ColumnString*>(str_col.get())->get_offsets();

        for (int i = 0; i < input_rows_count; ++i) {
            strlen_data[i] = str_offset[i] - str_offset[i - 1];
        }

        block.insert({std::move(params), std::make_shared<DataTypeInt32>(), "strlen"});

        ColumnNumbers temp_arguments = {arguments[0], arguments[1], block.columns() - 1};

        SubstringUtil::substring_execute(block, temp_arguments, result, input_rows_count);
        return Status::OK();
    }
};

class FunctionLeft : public IFunction {
public:
    static constexpr auto name = "left";
    static FunctionPtr create() { return std::make_shared<FunctionLeft>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto int_type = std::make_shared<DataTypeInt32>();
        size_t num_columns_without_result = block.columns();
        block.insert({int_type->create_column_const(input_rows_count, to_field(1)), int_type,
                      "const 1"});
        ColumnNumbers temp_arguments(3);
        temp_arguments[0] = arguments[0];
        temp_arguments[1] = num_columns_without_result;
        temp_arguments[2] = arguments[1];
        SubstringUtil::substring_execute(block, temp_arguments, result, input_rows_count);
        return Status::OK();
    }
};

class FunctionRight : public IFunction {
public:
    static constexpr auto name = "right";
    static FunctionPtr create() { return std::make_shared<FunctionRight>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto int_type = std::make_shared<DataTypeInt32>();
        auto params1 = ColumnInt32::create(input_rows_count);
        auto params2 = ColumnInt32::create(input_rows_count);
        size_t num_columns_without_result = block.columns();

        // params1 = max(arg[1], -len(arg))
        auto& index_data = params1->get_data();
        auto& strlen_data = params2->get_data();

        // we don't have to update null_map because FunctionSubstring will
        // update it
        // getNestedColumnIfNull arg[0]
        auto str_col =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*str_col)) {
            str_col = nullable->get_nested_column_ptr();
        }
        auto& str_offset = assert_cast<const ColumnString*>(str_col.get())->get_offsets();

        // getNestedColumnIfNull arg[1]
        auto pos_col =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*pos_col)) {
            pos_col = nullable->get_nested_column_ptr();
        }
        auto& pos_data = assert_cast<const ColumnInt32*>(pos_col.get())->get_data();

        for (int i = 0; i < input_rows_count; ++i) {
            strlen_data[i] = str_offset[i] - str_offset[i - 1];
        }

        for (int i = 0; i < input_rows_count; ++i) {
            index_data[i] = std::max(-pos_data[i], -strlen_data[i]);
        }

        block.insert({std::move(params1), int_type, "index"});
        block.insert({std::move(params2), int_type, "strlen"});

        ColumnNumbers temp_arguments(3);
        temp_arguments[0] = arguments[0];
        temp_arguments[1] = num_columns_without_result;
        temp_arguments[2] = num_columns_without_result + 1;
        SubstringUtil::substring_execute(block, temp_arguments, result, input_rows_count);
        return Status::OK();
    }
};

class FunctionNullOrEmpty : public IFunction {
public:
    static constexpr auto name = "null_or_empty";
    static FunctionPtr create() { return std::make_shared<FunctionNullOrEmpty>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto res_map = ColumnUInt8::create(input_rows_count, 0);

        auto column = block.get_by_position(arguments[0]).column;
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*column)) {
            column = nullable->get_nested_column_ptr();
            VectorizedUtils::update_null_map(res_map->get_data(), nullable->get_null_map_data());
        }
        auto str_col = assert_cast<const ColumnString*>(column.get());
        const auto& offsets = str_col->get_offsets();

        auto& res_map_data = res_map->get_data();
        for (int i = 0; i < input_rows_count; ++i) {
            int size = offsets[i] - offsets[i - 1];
            res_map_data[i] |= (size == 0);
        }

        block.replace_by_position(result, std::move(res_map));
        return Status::OK();
    }
};

class FunctionStringConcat : public IFunction {
public:
    static constexpr auto name = "concat";
    static FunctionPtr create() { return std::make_shared<FunctionStringConcat>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }
    bool use_default_implementation_for_nulls() const override { return true; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        DCHECK_GE(arguments.size(), 1);

        if (arguments.size() == 1) {
            block.get_by_position(result).column = block.get_by_position(arguments[0]).column;
            return Status::OK();
        }

        int argument_size = arguments.size();
        ColumnPtr argument_columns[argument_size];

        std::vector<const ColumnString::Offsets*> offsets_list(argument_size);
        std::vector<const ColumnString::Chars*> chars_list(argument_size);

        for (int i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            auto col_str = assert_cast<const ColumnString*>(argument_columns[i].get());
            offsets_list[i] = &col_str->get_offsets();
            chars_list[i] = &col_str->get_chars();
        }

        auto res = ColumnString::create();
        auto& res_data = res->get_chars();
        auto& res_offset = res->get_offsets();

        res_offset.resize(input_rows_count);

        int res_reserve_size = 0;
        // we could ignore null string column
        // but it's not necessary to ignore it
        for (size_t i = 0; i < offsets_list.size(); ++i) {
            for (size_t j = 0; j < input_rows_count; ++j) {
                res_reserve_size += (*offsets_list[i])[j] - (*offsets_list[i])[j - 1];
            }
        }
        // for each terminal zero
        res_reserve_size += input_rows_count;

        res_data.resize(res_reserve_size);

        for (size_t i = 0; i < input_rows_count; ++i) {
            int current_length = 0;
            for (size_t j = 0; j < offsets_list.size(); ++j) {
                auto& current_offsets = *offsets_list[j];
                auto& current_chars = *chars_list[j];

                int size = current_offsets[i] - current_offsets[i - 1];
                memcpy(&res_data[res_offset[i - 1]] + current_length,
                       &current_chars[current_offsets[i - 1]], size);
                current_length += size;
            }
            res_offset[i] = res_offset[i - 1] + current_length;
        }

        block.get_by_position(result).column = std::move(res);
        return Status::OK();
    }
};

class FunctionStringElt : public IFunction {
public:
    static constexpr auto name = "elt";
    static FunctionPtr create() { return std::make_shared<FunctionStringElt>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }
    bool use_default_implementation_for_nulls() const override { return true; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        int arguent_size = arguments.size();
        auto pos_col =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*pos_col)) {
            pos_col = nullable->get_nested_column_ptr();
        }
        auto& pos_data = assert_cast<const ColumnInt32*>(pos_col.get())->get_data();
        auto pos = pos_data[0];
        int num_children = arguent_size - 1;
        if (pos < 1 || num_children == 0 || pos > num_children) {
            auto null_map = ColumnUInt8::create(input_rows_count, 1);
            auto res = ColumnString::create();
            auto& res_data = res->get_chars();
            auto& res_offset = res->get_offsets();
            res_offset.resize(input_rows_count);
            for (size_t i = 0; i < input_rows_count; ++i) {
                res_offset[i] = res_data.size();
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(res), std::move(null_map));
            return Status::OK();
        }

        block.get_by_position(result).column = block.get_by_position(arguments[pos]).column;
        return Status::OK();
    }
};

// concat_ws (string,string....) or (string, Array)
// TODO: avoid use fmtlib
class FunctionStringConcatWs : public IFunction {
public:
    using Chars = ColumnString::Chars;
    using Offsets = ColumnString::Offsets;

    static constexpr auto name = "concat_ws";
    static FunctionPtr create() { return std::make_shared<FunctionStringConcatWs>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        const IDataType* first_type = arguments[0].get();
        if (first_type->is_nullable()) {
            return make_nullable(std::make_shared<DataTypeString>());
        } else {
            return std::make_shared<DataTypeString>();
        }
    }
    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        DCHECK_GE(arguments.size(), 2);
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        // we create a zero column to simply implement
        auto const_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res = ColumnString::create();
        bool is_null_type = block.get_by_position(arguments[0]).type.get()->is_nullable();
        size_t argument_size = arguments.size();
        std::vector<const Offsets*> offsets_list(argument_size);
        std::vector<const Chars*> chars_list(argument_size);
        std::vector<const ColumnUInt8::Container*> null_list(argument_size);

        ColumnPtr argument_columns[argument_size];
        ColumnPtr argument_null_columns[argument_size];

        for (size_t i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<const ColumnNullable>(*argument_columns[i])) {
                // Danger: Here must dispose the null map data first! Because
                // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
                // of column nullable mem of null map
                null_list[i] = &nullable->get_null_map_data();
                argument_null_columns[i] = nullable->get_null_map_column_ptr();
                argument_columns[i] = nullable->get_nested_column_ptr();
            } else {
                null_list[i] = &const_null_map->get_data();
            }

            if (check_column<ColumnArray>(argument_columns[i].get())) {
                continue;
            }

            auto col_str = assert_cast<const ColumnString*>(argument_columns[i].get());
            offsets_list[i] = &col_str->get_offsets();
            chars_list[i] = &col_str->get_chars();
        }

        auto& res_data = res->get_chars();
        auto& res_offset = res->get_offsets();
        res_offset.resize(input_rows_count);

        VectorizedUtils::update_null_map(null_map->get_data(), *null_list[0]);
        fmt::memory_buffer buffer;
        std::vector<std::string_view> views;

        if (check_column<ColumnArray>(argument_columns[1].get())) {
            // Determine if the nested type of the array is String
            const ColumnArray& array_column =
                    reinterpret_cast<const ColumnArray&>(*argument_columns[1]);
            if (!array_column.get_data().is_column_string()) {
                return Status::NotSupported(
                        fmt::format("unsupported nested array of type {} for function {}",
                                    is_column_nullable(array_column.get_data())
                                            ? array_column.get_data().get_name()
                                            : array_column.get_data().get_family_name(),
                                    get_name()));
            }
            // Concat string in array
            _execute_array(input_rows_count, array_column, buffer, views, offsets_list, chars_list,
                           null_list, res_data, res_offset);

        } else {
            // Concat string
            _execute_string(input_rows_count, argument_size, buffer, views, offsets_list,
                            chars_list, null_list, res_data, res_offset);
        }
        if (is_null_type) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(res), std::move(null_map));
        } else {
            block.get_by_position(result).column = std::move(res);
        }
        return Status::OK();
    }

private:
    void _execute_array(const size_t& input_rows_count, const ColumnArray& array_column,
                        fmt::memory_buffer& buffer, std::vector<std::string_view>& views,
                        const std::vector<const Offsets*>& offsets_list,
                        const std::vector<const Chars*>& chars_list,
                        const std::vector<const ColumnUInt8::Container*>& null_list,
                        Chars& res_data, Offsets& res_offset) {
        // Get array nested column
        const UInt8* array_nested_null_map = nullptr;
        ColumnPtr array_nested_column = nullptr;

        if (is_column_nullable(array_column.get_data())) {
            const auto& array_nested_null_column =
                    reinterpret_cast<const ColumnNullable&>(array_column.get_data());
            // String's null map in array
            array_nested_null_map =
                    array_nested_null_column.get_null_map_column().get_data().data();
            array_nested_column = array_nested_null_column.get_nested_column_ptr();
        } else {
            array_nested_column = array_column.get_data_ptr();
        }

        const auto& string_column = reinterpret_cast<const ColumnString&>(*array_nested_column);
        const Chars& string_src_chars = string_column.get_chars();
        const auto& src_string_offsets = string_column.get_offsets();
        const auto& src_array_offsets = array_column.get_offsets();
        size_t current_src_array_offset = 0;

        // Concat string in array
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto& sep_offsets = *offsets_list[0];
            auto& sep_chars = *chars_list[0];
            auto& sep_nullmap = *null_list[0];

            if (sep_nullmap[i]) {
                res_offset[i] = res_data.size();
                current_src_array_offset += src_array_offsets[i] - src_array_offsets[i - 1];
                continue;
            }

            int sep_size = sep_offsets[i] - sep_offsets[i - 1];
            const char* sep_data = reinterpret_cast<const char*>(&sep_chars[sep_offsets[i - 1]]);

            std::string_view sep(sep_data, sep_size);
            buffer.clear();
            views.clear();

            for (auto next_src_array_offset = src_array_offsets[i];
                 current_src_array_offset < next_src_array_offset; ++current_src_array_offset) {
                const auto current_src_string_offset =
                        current_src_array_offset ? src_string_offsets[current_src_array_offset - 1]
                                                 : 0;
                size_t bytes_to_copy =
                        src_string_offsets[current_src_array_offset] - current_src_string_offset;
                const char* ptr =
                        reinterpret_cast<const char*>(&string_src_chars[current_src_string_offset]);

                if (array_nested_null_map == nullptr ||
                    !array_nested_null_map[current_src_array_offset]) {
                    views.emplace_back(ptr, bytes_to_copy);
                }
            }

            fmt::format_to(buffer, "{}", fmt::join(views, sep));

            StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                        res_offset);
        }
    }

    void _execute_string(const size_t& input_rows_count, const size_t& argument_size,
                         fmt::memory_buffer& buffer, std::vector<std::string_view>& views,
                         const std::vector<const Offsets*>& offsets_list,
                         const std::vector<const Chars*>& chars_list,
                         const std::vector<const ColumnUInt8::Container*>& null_list,
                         Chars& res_data, Offsets& res_offset) {
        // Concat string
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto& sep_offsets = *offsets_list[0];
            auto& sep_chars = *chars_list[0];
            auto& sep_nullmap = *null_list[0];
            if (sep_nullmap[i]) {
                res_offset[i] = res_data.size();
                continue;
            }

            int sep_size = sep_offsets[i] - sep_offsets[i - 1];
            const char* sep_data = reinterpret_cast<const char*>(&sep_chars[sep_offsets[i - 1]]);

            std::string_view sep(sep_data, sep_size);
            buffer.clear();
            views.clear();
            for (size_t j = 1; j < argument_size; ++j) {
                auto& current_offsets = *offsets_list[j];
                auto& current_chars = *chars_list[j];
                auto& current_nullmap = *null_list[j];
                int size = current_offsets[i] - current_offsets[i - 1];
                const char* ptr =
                        reinterpret_cast<const char*>(&current_chars[current_offsets[i - 1]]);
                if (!current_nullmap[i]) {
                    views.emplace_back(ptr, size);
                }
            }
            fmt::format_to(buffer, "{}", fmt::join(views, sep));
            StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                        res_offset);
        }
    }
};

class FunctionStringRepeat : public IFunction {
public:
    static constexpr auto name = "repeat";
    static FunctionPtr create() { return std::make_shared<FunctionStringRepeat>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    bool use_default_implementation_for_constants() const override { return true; }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        DCHECK_EQ(arguments.size(), 2);
        auto res = ColumnString::create();
        auto null_map = ColumnUInt8::create();

        ColumnPtr argument_ptr[2];
        argument_ptr[0] =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        argument_ptr[1] = block.get_by_position(arguments[1]).column;

        if (auto* col1 = check_and_get_column<ColumnString>(*argument_ptr[0])) {
            if (auto* col2 = check_and_get_column<ColumnInt32>(*argument_ptr[1])) {
                vector_vector(col1->get_chars(), col1->get_offsets(), col2->get_data(),
                              res->get_chars(), res->get_offsets(), null_map->get_data());
                block.replace_by_position(
                        result, ColumnNullable::create(std::move(res), std::move(null_map)));
                return Status::OK();
            } else if (auto* col2_const = check_and_get_column<ColumnConst>(*argument_ptr[1])) {
                DCHECK(check_and_get_column<ColumnInt32>(col2_const->get_data_column()));
                int repeat = col2_const->get_int(0);
                if (repeat <= 0) {
                    null_map->get_data().resize_fill(input_rows_count, 0);
                    res->insert_many_defaults(input_rows_count);
                } else {
                    vector_const(col1->get_chars(), col1->get_offsets(), repeat, res->get_chars(),
                                 res->get_offsets(), null_map->get_data());
                }
                block.replace_by_position(
                        result, ColumnNullable::create(std::move(res), std::move(null_map)));
                return Status::OK();
            }
        }

        return Status::RuntimeError("repeat function get error param: {}, {}",
                                    argument_ptr[0]->get_name(), argument_ptr[1]->get_name());
    }

    void vector_vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                       const ColumnInt32::Container& repeats, ColumnString::Chars& res_data,
                       ColumnString::Offsets& res_offsets, ColumnUInt8::Container& null_map) {
        size_t input_row_size = offsets.size();

        fmt::memory_buffer buffer;
        res_offsets.resize(input_row_size);
        null_map.resize_fill(input_row_size, 0);
        for (ssize_t i = 0; i < input_row_size; ++i) {
            buffer.clear();
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t size = offsets[i] - offsets[i - 1];
            int repeat = repeats[i];

            if (repeat <= 0) {
                StringOP::push_empty_string(i, res_data, res_offsets);
            } else if (repeat * size > DEFAULT_MAX_STRING_SIZE) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
            } else {
                for (int j = 0; j < repeat; ++j) {
                    buffer.append(raw_str, raw_str + size);
                }
                StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i,
                                            res_data, res_offsets);
            }
        }
    }

    // TODO: 1. use pmr::vector<char> replace fmt_buffer may speed up the code
    //       2. abstract the `vector_vector` and `vector_const`
    //       3. rethink we should use `DEFAULT_MAX_STRING_SIZE` to bigger here
    void vector_const(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                      int repeat, ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets,
                      ColumnUInt8::Container& null_map) {
        size_t input_row_size = offsets.size();

        fmt::memory_buffer buffer;
        res_offsets.resize(input_row_size);
        null_map.resize_fill(input_row_size, 0);
        for (ssize_t i = 0; i < input_row_size; ++i) {
            buffer.clear();
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t size = offsets[i] - offsets[i - 1];

            if (repeat * size > DEFAULT_MAX_STRING_SIZE) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
            } else {
                for (int j = 0; j < repeat; ++j) {
                    buffer.append(raw_str, raw_str + size);
                }
                StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i,
                                            res_data, res_offsets);
            }
        }
    }
};

template <typename Impl>
class FunctionStringPad : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionStringPad>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    bool use_default_implementation_for_nulls() const override { return true; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        DCHECK_GE(arguments.size(), 3);
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        // we create a zero column to simply implement
        auto const_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res = ColumnString::create();

        size_t argument_size = arguments.size();
        ColumnPtr argument_columns[argument_size];
        for (size_t i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<const ColumnNullable>(*argument_columns[i])) {
                // Danger: Here must dispose the null map data first! Because
                // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
                // of column nullable mem of null map
                VectorizedUtils::update_null_map(null_map->get_data(),
                                                 nullable->get_null_map_data());
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        auto& null_map_data = null_map->get_data();
        auto& res_offsets = res->get_offsets();
        auto& res_chars = res->get_chars();
        res_offsets.resize(input_rows_count);

        auto strcol = assert_cast<const ColumnString*>(argument_columns[0].get());
        auto& strcol_offsets = strcol->get_offsets();
        auto& strcol_chars = strcol->get_chars();

        auto col_len = assert_cast<const ColumnInt32*>(argument_columns[1].get());
        auto& col_len_data = col_len->get_data();

        auto padcol = assert_cast<const ColumnString*>(argument_columns[2].get());
        auto& padcol_offsets = padcol->get_offsets();
        auto& padcol_chars = padcol->get_chars();

        std::vector<size_t> str_index;
        std::vector<size_t> pad_index;

        fmt::memory_buffer buffer;

        for (size_t i = 0; i < input_rows_count; ++i) {
            str_index.clear();
            pad_index.clear();
            buffer.clear();
            if (null_map_data[i] || col_len_data[i] < 0) {
                // return NULL when input string is NULL or input length is invalid number
                null_map_data[i] = true;
                StringOP::push_empty_string(i, res_chars, res_offsets);
            } else {
                int str_len = strcol_offsets[i] - strcol_offsets[i - 1];
                const char* str_data =
                        reinterpret_cast<const char*>(&strcol_chars[strcol_offsets[i - 1]]);

                int pad_len = padcol_offsets[i] - padcol_offsets[i - 1];
                const char* pad_data =
                        reinterpret_cast<const char*>(&padcol_chars[padcol_offsets[i - 1]]);

                size_t str_char_size =
                        get_char_len(std::string_view(str_data, str_len), &str_index);
                size_t pad_char_size =
                        get_char_len(std::string_view(pad_data, pad_len), &pad_index);

                if (col_len_data[i] <= str_char_size) {
                    // truncate the input string
                    if (col_len_data[i] < str_char_size) {
                        buffer.append(str_data, str_data + str_index[col_len_data[i]]);
                    } else {
                        buffer.append(str_data, str_data + str_len);
                    }

                    StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i,
                                                res_chars, res_offsets);
                    continue;
                }
                if (pad_char_size == 0) {
                    // return NULL when the string to be paded is missing
                    null_map_data[i] = true;
                    StringOP::push_empty_string(i, res_chars, res_offsets);
                    continue;
                }

                int32_t pad_byte_len = 0;
                int32_t pad_times = (col_len_data[i] - str_char_size) / pad_char_size;
                int32_t pad_remainder = (col_len_data[i] - str_char_size) % pad_char_size;
                pad_byte_len = pad_times * pad_len;
                pad_byte_len += pad_index[pad_remainder];
                int32_t byte_len = str_len + pad_byte_len;
                // StringVal result(context, byte_len);
                if constexpr (Impl::is_lpad) {
                    int pad_idx = 0;
                    int result_index = 0;

                    // Prepend chars of pad.
                    while (result_index++ < pad_byte_len) {
                        buffer.push_back(pad_data[pad_idx++]);
                        pad_idx = pad_idx % pad_len;
                    }

                    // Append given string.
                    buffer.append(str_data, str_data + str_len);
                    StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i,
                                                res_chars, res_offsets);

                } else {
                    // is rpad
                    buffer.append(str_data, str_data + str_len);

                    // Append chars of pad until desired length
                    int pad_idx = 0;
                    int result_len = str_len;
                    while (result_len++ < byte_len) {
                        buffer.push_back(pad_data[pad_idx++]);
                        pad_idx = pad_idx % pad_len;
                    }
                    StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i,
                                                res_chars, res_offsets);
                }
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }
};

class FunctionSplitPart : public IFunction {
public:
    static constexpr auto name = "split_part";
    static FunctionPtr create() { return std::make_shared<FunctionSplitPart>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        DCHECK_EQ(arguments.size(), 3);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        // Create a zero column to simply implement
        auto const_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res = ColumnString::create();

        auto& null_map_data = null_map->get_data();
        auto& res_offsets = res->get_offsets();
        auto& res_chars = res->get_chars();
        res_offsets.resize(input_rows_count);

        size_t argument_size = arguments.size();
        ColumnPtr argument_columns[argument_size];
        for (size_t i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<const ColumnNullable>(*argument_columns[i])) {
                // Danger: Here must dispose the null map data first! Because
                // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
                // of column nullable mem of null map
                VectorizedUtils::update_null_map(null_map->get_data(),
                                                 nullable->get_null_map_data());
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        auto str_col = assert_cast<const ColumnString*>(argument_columns[0].get());

        auto delimiter_col = assert_cast<const ColumnString*>(argument_columns[1].get());

        auto part_num_col = assert_cast<const ColumnInt32*>(argument_columns[2].get());
        auto& part_num_col_data = part_num_col->get_data();

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (part_num_col_data[i] <= 0) {
                StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                continue;
            }

            auto delimiter = delimiter_col->get_data_at(i);
            auto delimiter_str = delimiter_col->get_data_at(i).to_string();
            auto part_number = part_num_col_data[i];
            auto str = str_col->get_data_at(i);
            if (delimiter.size == 0) {
                StringOP::push_empty_string(i, res_chars, res_offsets);
            } else if (delimiter.size == 1) {
                // If delimiter is a char, use memchr to split
                int32_t pre_offset = -1;
                int32_t offset = -1;
                int32_t num = 0;
                while (num < part_number) {
                    pre_offset = offset;
                    size_t n = str.size - offset - 1;
                    const char* pos = reinterpret_cast<const char*>(
                            memchr(str.data + offset + 1, delimiter_str[0], n));
                    if (pos != nullptr) {
                        offset = pos - str.data;
                        num++;
                    } else {
                        offset = str.size;
                        num = (num == 0) ? 0 : num + 1;
                        break;
                    }
                }

                if (num == part_number) {
                    StringOP::push_value_string(
                            std::string_view {
                                    reinterpret_cast<const char*>(str.data + pre_offset + 1),
                                    (size_t)offset - pre_offset - 1},
                            i, res_chars, res_offsets);
                } else {
                    StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                }
            } else {
                // If delimiter is a string, use memmem to split
                int32_t pre_offset = -delimiter.size;
                int32_t offset = -delimiter.size;
                int32_t num = 0;
                while (num < part_number) {
                    pre_offset = offset;
                    size_t n = str.size - offset - delimiter.size;
                    char* pos = reinterpret_cast<char*>(memmem(str.data + offset + delimiter.size,
                                                               n, delimiter.data, delimiter.size));
                    if (pos != nullptr) {
                        offset = pos - str.data;
                        num++;
                    } else {
                        offset = str.size;
                        num = (num == 0) ? 0 : num + 1;
                        break;
                    }
                }

                if (num == part_number) {
                    StringOP::push_value_string(
                            std::string_view {reinterpret_cast<const char*>(str.data + pre_offset +
                                                                            delimiter.size),
                                              (size_t)offset - pre_offset - delimiter.size},
                            i, res_chars, res_offsets);
                } else {
                    StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                }
            }
        }
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }
};

struct SM3Sum {
    static constexpr auto name = "sm3sum";
    using ObjectData = SM3Digest;
};

struct MD5Sum {
    static constexpr auto name = "md5sum";
    using ObjectData = Md5Digest;
};

template <typename Impl>
class FunctionStringMd5AndSM3 : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionStringMd5AndSM3>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }
    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        DCHECK_GE(arguments.size(), 1);

        int argument_size = arguments.size();
        ColumnPtr argument_columns[argument_size];

        std::vector<const ColumnString::Offsets*> offsets_list(argument_size);
        std::vector<const ColumnString::Chars*> chars_list(argument_size);

        for (int i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto col_str = assert_cast<const ColumnString*>(argument_columns[i].get())) {
                offsets_list[i] = &col_str->get_offsets();
                chars_list[i] = &col_str->get_chars();
            } else {
                return Status::RuntimeError("Illegal column {} of argument of function {}",
                                            block.get_by_position(arguments[0]).column->get_name(),
                                            get_name());
            }
        }

        auto res = ColumnString::create();
        auto& res_data = res->get_chars();
        auto& res_offset = res->get_offsets();

        res_offset.resize(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i) {
            using ObjectData = typename Impl::ObjectData;
            ObjectData digest;
            for (size_t j = 0; j < offsets_list.size(); ++j) {
                auto& current_offsets = *offsets_list[j];
                auto& current_chars = *chars_list[j];

                int size = current_offsets[i] - current_offsets[i - 1];
                if (size < 1) {
                    continue;
                }
                digest.update(&current_chars[current_offsets[i - 1]], size);
            }
            digest.digest();

            StringOP::push_value_string(std::string_view(digest.hex().c_str(), digest.hex().size()),
                                        i, res_data, res_offset);
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

class FunctionStringParseUrl : public IFunction {
public:
    static constexpr auto name = "parse_url";
    static FunctionPtr create() { return std::make_shared<FunctionStringParseUrl>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        auto& null_map_data = null_map->get_data();

        auto res = ColumnString::create();
        auto& res_offsets = res->get_offsets();
        auto& res_chars = res->get_chars();
        res_offsets.resize(input_rows_count);

        size_t argument_size = arguments.size();
        bool has_key = argument_size >= 3;

        ColumnPtr argument_columns[argument_size];
        for (size_t i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
        }

        const auto* url_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* part_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const ColumnString* key_col = nullptr;
        if (has_key) {
            key_col = check_and_get_column<ColumnString>(argument_columns[2].get());
        }

        if (!url_col || !part_col || (has_key && !key_col)) {
            return Status::InternalError("Not supported input arguments types");
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map_data[i]) {
                StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                continue;
            }

            auto part = part_col->get_data_at(i);
            StringValue p(const_cast<char*>(part.data), part.size);
            UrlParser::UrlPart url_part = UrlParser::get_url_part(p);
            StringValue url_key;
            if (has_key) {
                auto key = key_col->get_data_at(i);
                url_key = StringValue(const_cast<char*>(key.data), key.size);
            }

            auto source = url_col->get_data_at(i);
            StringValue url_val(const_cast<char*>(source.data), source.size);

            StringValue parse_res;
            bool success = false;
            if (has_key) {
                success = UrlParser::parse_url_key(url_val, url_part, url_key, &parse_res);
            } else {
                success = UrlParser::parse_url(url_val, url_part, &parse_res);
            }

            if (!success) {
                // url is malformed, or url_part is invalid.
                if (url_part == UrlParser::INVALID) {
                    return Status::RuntimeError(
                            "Invalid URL part: {}\n{}", std::string(part.data, part.size),
                            "(Valid URL parts are 'PROTOCOL', 'HOST', 'PATH', 'REF', 'AUTHORITY', "
                            "'FILE', 'USERINFO', 'PORT' and 'QUERY')");
                } else {
                    StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                    continue;
                }
            }

            StringOP::push_value_string(std::string_view(parse_res.ptr, parse_res.len), i,
                                        res_chars, res_offsets);
        }
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }
};

template <typename Impl>
class FunctionMoneyFormat : public IFunction {
public:
    static constexpr auto name = "money_format";
    static FunctionPtr create() { return std::make_shared<FunctionMoneyFormat<Impl>>(); }
    String get_name() const override { return name; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }
    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }
    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto res_column = ColumnString::create();
        ColumnPtr argument_column = block.get_by_position(arguments[0]).column;

        auto result_column = assert_cast<ColumnString*>(res_column.get());
        auto data_column = assert_cast<const typename Impl::ColumnType*>(argument_column.get());

        Impl::execute(context, result_column, data_column, input_rows_count);

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

struct MoneyFormatDoubleImpl {
    using ColumnType = ColumnVector<Float64>;

    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeFloat64>()}; }

    static void execute(FunctionContext* context, ColumnString* result_column,
                        const ColumnType* data_column, size_t input_rows_count) {
        for (size_t i = 0; i < input_rows_count; i++) {
            double value =
                    MathFunctions::my_double_round(data_column->get_element(i), 2, false, false);
            StringVal str = StringFunctions::do_money_format(context, fmt::format("{:.2f}", value));
            result_column->insert_data(reinterpret_cast<const char*>(str.ptr), str.len);
        }
    }
};

struct MoneyFormatInt64Impl {
    using ColumnType = ColumnVector<Int64>;

    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeInt64>()}; }

    static void execute(FunctionContext* context, ColumnString* result_column,
                        const ColumnType* data_column, size_t input_rows_count) {
        for (size_t i = 0; i < input_rows_count; i++) {
            Int64 value = data_column->get_element(i);
            StringVal str = StringFunctions::do_money_format<Int64, 26>(context, value);
            result_column->insert_data(reinterpret_cast<const char*>(str.ptr), str.len);
        }
    }
};

struct MoneyFormatInt128Impl {
    using ColumnType = ColumnVector<Int128>;

    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeInt128>()}; }

    static void execute(FunctionContext* context, ColumnString* result_column,
                        const ColumnType* data_column, size_t input_rows_count) {
        for (size_t i = 0; i < input_rows_count; i++) {
            Int128 value = data_column->get_element(i);
            StringVal str = StringFunctions::do_money_format<Int128, 52>(context, value);
            result_column->insert_data(reinterpret_cast<const char*>(str.ptr), str.len);
        }
    }
};

struct MoneyFormatDecimalImpl {
    using ColumnType = ColumnDecimal<Decimal128>;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeDecimal<Decimal128>>(27, 9)};
    }

    static void execute(FunctionContext* context, ColumnString* result_column,
                        const ColumnType* data_column, size_t input_rows_count) {
        for (size_t i = 0; i < input_rows_count; i++) {
            DecimalV2Val value = DecimalV2Val(data_column->get_element(i));

            DecimalV2Value rounded(0);
            DecimalV2Value::from_decimal_val(value).round(&rounded, 2, HALF_UP);

            StringVal str = StringFunctions::do_money_format<int64_t, 26>(
                    context, rounded.int_value(), abs(rounded.frac_value() / 10000000));

            result_column->insert_data(reinterpret_cast<const char*>(str.ptr), str.len);
        }
    }
};

class FunctionStringLocatePos : public IFunction {
public:
    static constexpr auto name = "locate";
    static FunctionPtr create() { return std::make_shared<FunctionStringLocatePos>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt32>();
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeInt32>()};
    }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto col_substr =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto col_str =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto col_pos =
                block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();

        ColumnInt32::MutablePtr col_res = ColumnInt32::create();

        auto& vec_pos = reinterpret_cast<const ColumnInt32*>(col_pos.get())->get_data();
        auto& vec_res = col_res->get_data();
        vec_res.resize(input_rows_count);

        for (int i = 0; i < input_rows_count; ++i) {
            vec_res[i] = locate_pos(col_substr->get_data_at(i).to_string_val(),
                                    col_str->get_data_at(i).to_string_val(), vec_pos[i]);
        }

        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

private:
    int locate_pos(StringVal substr, StringVal str, int start_pos) {
        if (substr.len == 0) {
            if (start_pos <= 0) {
                return 0;
            } else if (start_pos == 1) {
                return 1;
            } else if (start_pos > str.len) {
                return 0;
            } else {
                return start_pos;
            }
        }
        // Hive returns 0 for *start_pos <= 0,
        // but throws an exception for *start_pos > str->len.
        // Since returning 0 seems to be Hive's error condition, return 0.
        std::vector<size_t> index;
        size_t char_len = get_char_len(str, &index);
        if (start_pos <= 0 || start_pos > str.len || start_pos > char_len) {
            return 0;
        }
        StringValue substr_sv = StringValue::from_string_val(substr);
        StringSearch search(&substr_sv);
        // Input start_pos starts from 1.
        StringValue adjusted_str(reinterpret_cast<char*>(str.ptr) + index[start_pos - 1],
                                 str.len - index[start_pos - 1]);
        int32_t match_pos = search.search(&adjusted_str);
        if (match_pos >= 0) {
            // Hive returns the position in the original string starting from 1.
            return start_pos + get_char_len(adjusted_str, match_pos);
        } else {
            return 0;
        }
    }
};

class FunctionReplace : public IFunction {
public:
    static constexpr auto name = "replace";
    static FunctionPtr create() { return std::make_shared<FunctionReplace>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeString>()};
    }

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto col_origin =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto col_old =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto col_new =
                block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();

        ColumnString::MutablePtr col_res = ColumnString::create();

        for (int i = 0; i < input_rows_count; ++i) {
            StringRef origin_str =
                    assert_cast<const ColumnString*>(col_origin.get())->get_data_at(i);
            StringRef old_str = assert_cast<const ColumnString*>(col_old.get())->get_data_at(i);
            StringRef new_str = assert_cast<const ColumnString*>(col_new.get())->get_data_at(i);

            std::string result = replace(origin_str.to_string(), old_str.to_string_view(),
                                         new_str.to_string_view());
            col_res->insert_data(result.data(), result.length());
        }

        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

private:
    std::string replace(std::string str, std::string_view old_str, std::string_view new_str) {
        if (old_str.empty()) {
            return str;
        }
        std::string::size_type pos = 0;
        std::string::size_type oldLen = old_str.size();
        std::string::size_type newLen = new_str.size();
        while ((pos = str.find(old_str, pos)) != std::string::npos) {
            str.replace(pos, oldLen, new_str);
            pos += newLen;
        }
        return str;
    }
};

struct ReverseImpl {
    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets) {
        auto rows_count = offsets.size();
        res_offsets.resize(rows_count);
        res_data.reserve(data.size());
        for (ssize_t i = 0; i < rows_count; ++i) {
            auto src_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int64_t src_len = offsets[i] - offsets[i - 1];
            char dst[src_len];
            simd::VStringFunctions::reverse(StringVal((uint8_t*)src_str, src_len),
                                            StringVal((uint8_t*)dst, src_len));
            StringOP::push_value_string(std::string_view(dst, src_len), i, res_data, res_offsets);
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized

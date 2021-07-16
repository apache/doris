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

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <string_view>

#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

inline size_t get_utf8_byte_length(unsigned char byte) {
    size_t char_size = 0;
    if (byte >= 0xFC) {
        char_size = 6;
    } else if (byte >= 0xF8) {
        char_size = 5;
    } else if (byte >= 0xF0) {
        char_size = 4;
    } else if (byte >= 0xE0) {
        char_size = 3;
    } else if (byte >= 0xC0) {
        char_size = 2;
    } else {
        char_size = 1;
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

struct StringOP {
    static void push_empty_string(int index, ColumnString::Chars& chars,
                                  ColumnString::Offsets& offsets) {
        chars.push_back('\0');
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
        chars.push_back('\0');
        offsets[index] = chars.size();
    }
};

class FunctionSubstring : public IFunction {
public:
    static constexpr auto name = "substring";
    static FunctionPtr create() { return std::make_shared<FunctionSubstring>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
        substring_execute(block, arguments, result, input_rows_count);
        return Status::OK();
    }
    static void substring_execute(Block& block, const ColumnNumbers& arguments, size_t result,
                                  size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 3);
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        ColumnPtr argument_columns[3];

        for (int i = 0; i < 3; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[i])) {
                argument_columns[i] = nullable->get_nested_column_ptr();
                VectorizedUtils::update_null_map(null_map->get_data(),
                                                 nullable->get_null_map_data());
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
        std::vector<size_t> index;

        for (int i = 0; i < size; ++i) {
            const char* raw_str = reinterpret_cast<const char*>(&chars[offsets[i - 1]]);
            int str_size = offsets[i] - offsets[i - 1] - 1;
            // return null if start > src.length
            if (start[i] > str_size) {
                StringOP::push_null_string(i, res_chars, res_offsets, null_map);
                continue;
            }
            // return "" if len < 0 or str == 0 or start == 0
            if (len[i] <= 0 || str_size == 0 || start[i] == 0) {
                StringOP::push_empty_string(i, res_chars, res_offsets);
                continue;
            }
            // reference to string_function.cpp: substring
            size_t byte_pos = 0;
            index.clear();
            for (size_t j = 0, char_size = 0; j < str_size; j += char_size) {
                char_size = get_utf8_byte_length((unsigned)(raw_str)[i]);
                index.push_back(j);
                if (start[i] > 0 && index.size() > start[i] + len[i]) {
                    break;
                }
            }

            int fixed_pos = start[i];
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
                StringOP::push_value_string(std::string_view{raw_str + byte_pos, (size_t)fixed_len},
                                            i, res_chars, res_offsets);
            } else {
                StringOP::push_empty_string(i, res_chars, res_offsets);
            }
        }
    }
};

class FunctionLeft : public FunctionSubstring {
public:
    static constexpr auto name = "left";
    static FunctionPtr create() { return std::make_shared<FunctionLeft>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
        auto int_type = std::make_shared<DataTypeInt32>();
        size_t num_columns_without_result = block.columns();
        block.insert({int_type->create_column_const(input_rows_count, to_field(1))
                              ->convert_to_full_column_if_const(),
                      int_type, "const 1"});
        ColumnNumbers temp_arguments(3);
        temp_arguments[0] = arguments[0];
        temp_arguments[1] = num_columns_without_result;
        temp_arguments[2] = arguments[1];
        FunctionSubstring::substring_execute(block, temp_arguments, result, input_rows_count);
        return Status::OK();
    }
};

class FunctionRight : public FunctionSubstring {
public:
    static constexpr auto name = "right";
    static FunctionPtr create() { return std::make_shared<FunctionRight>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
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
            strlen_data[i] = str_offset[i] - str_offset[i - 1] - 1;
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
        FunctionSubstring::substring_execute(block, temp_arguments, result, input_rows_count);
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

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
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
            int size = offsets[i] - offsets[i - 1] - 1;
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
        return make_nullable(std::make_shared<DataTypeString>());
    }
    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
        DCHECK_GE(arguments.size(), 1);

        if (arguments.size() == 1) {
            if (block.get_by_position(arguments[0]).column->is_nullable()) {
                block.get_by_position(result).column = block.get_by_position(arguments[0]).column;
            } else {
                block.get_by_position(result).column =
                        make_nullable(block.get_by_position(arguments[0]).column);
            }
            return Status::OK();
        }

        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        int argument_size = arguments.size();
        ColumnPtr argument_columns[argument_size];

        std::vector<const ColumnString::Offsets*> offsets_list(argument_size);
        std::vector<const ColumnString::Chars*> chars_list(argument_size);

        for (int i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<const ColumnNullable>(*argument_columns[i])) {
                argument_columns[i] = nullable->get_nested_column_ptr();
                VectorizedUtils::update_null_map(null_map->get_data(),
                                                 nullable->get_null_map_data());
            }
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
                res_reserve_size += (*offsets_list[i])[j] - (*offsets_list[i])[j - 1] - 1;
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

                int size = current_offsets[i] - current_offsets[i - 1] - 1;
                memcpy(&res_data[res_offset[i - 1]] + current_length,
                       &current_chars[current_offsets[i - 1]], size);
                current_length += size;
            }
            // add terminal zero
            *(&res_data[res_offset[i - 1]] + current_length) = '\0';
            current_length++;
            res_offset[i] = res_offset[i - 1] + current_length;
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }
};

// concat_ws (string,string....)
// TODO: avoid use fmtlib
class FunctionStringConcatWs : public IFunction {
public:
    static constexpr auto name = "concat_ws";
    static FunctionPtr create() { return std::make_shared<FunctionStringConcatWs>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
        DCHECK_GE(arguments.size(), 2);
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        // we create a zero column to simply implement
        auto const_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res = ColumnString::create();

        size_t argument_size = arguments.size();
        std::vector<const ColumnString::Offsets*> offsets_list(argument_size);
        std::vector<const ColumnString::Chars*> chars_list(argument_size);
        std::vector<const ColumnUInt8::Container*> null_list(argument_size);

        ColumnPtr argument_columns[3];

        for (size_t i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<const ColumnNullable>(*argument_columns[i])) {
                argument_columns[i] = nullable->get_nested_column_ptr();
                null_list[i] = &nullable->get_null_map_data();
            } else {
                null_list[i] = &const_null_map->get_data();
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

        for (size_t i = 0; i < input_rows_count; ++i) {
            auto& seq_offsets = *offsets_list[0];
            auto& seq_chars = *chars_list[0];
            auto& seq_nullmap = *null_list[0];
            if (seq_nullmap[i]) {
                res_data.push_back('\0');
                res_offset[i] = res_data.size();
                continue;
            }

            int seq_size = seq_offsets[i] - seq_offsets[i - 1] - 1;
            const char* seq_data = reinterpret_cast<const char*>(&seq_chars[seq_offsets[i - 1]]);

            std::string_view seq(seq_data, seq_size);
            buffer.clear();
            views.clear();
            for (size_t j = 1; j < argument_size; ++j) {
                auto& current_offsets = *offsets_list[j];
                auto& current_chars = *chars_list[j];
                auto& current_nullmap = *null_list[j];
                int size = current_offsets[i] - current_offsets[i - 1] - 1;
                const char* ptr =
                        reinterpret_cast<const char*>(&current_chars[current_offsets[i - 1]]);
                if (!current_nullmap[i]) {
                    views.emplace_back(ptr, size);
                }
            }
            fmt::format_to(buffer, "{}", fmt::join(views, seq));
            StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                        res_offset);
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }
};

class FunctionStringRepeat : public IFunction {
public:
    static constexpr auto name = "repeat";
    static FunctionPtr create() { return std::make_shared<FunctionStringRepeat>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }
    bool use_default_implementation_for_constants() const override { return true; }
    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
        DCHECK_EQ(arguments.size(), 2);
        auto res = ColumnString::create();

        ColumnPtr argument_ptr[2];
        argument_ptr[0] =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        argument_ptr[1] =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        if (auto* col1 = check_and_get_column<ColumnString>(*argument_ptr[0])) {
            if (auto* col2 = check_and_get_column<ColumnInt32>(*argument_ptr[1])) {
                vector_vector(col1->get_chars(), col1->get_offsets(), col2->get_data(),
                              res->get_chars(), res->get_offsets());
                block.replace_by_position(result, std::move(res));
                return Status::OK();
            }
        }

        return Status::RuntimeError(fmt::format("not support {}", get_name()));
    }

    void vector_vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                       const ColumnInt32::Container& repeats, ColumnString::Chars& res_data,
                       ColumnString::Offsets& res_offsets) {
        size_t input_row_size = offsets.size();
        //
        fmt::memory_buffer buffer;
        res_offsets.resize(input_row_size);
        for (size_t i = 0; i < input_row_size; ++i) {
            buffer.clear();
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int size = offsets[i] - offsets[i - 1] - 1;
            int repeat = repeats[i];
            // assert size * repeat won't exceed
            DCHECK_LE(static_cast<int64_t>(size) * repeat, std::numeric_limits<int32_t>::max());
            for (int i = 0; i < repeat; ++i) {
                buffer.append(raw_str, raw_str + size);
            }
            StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                        res_offsets);
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
    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
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
                argument_columns[i] = nullable->get_nested_column_ptr();
                VectorizedUtils::update_null_map(null_map->get_data(),
                                                 nullable->get_null_map_data());
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
                StringOP::push_empty_string(i, res_chars, res_offsets);
            } else {
                int str_len = strcol_offsets[i] - strcol_offsets[i - 1] - 1;
                const char* str_data =
                        reinterpret_cast<const char*>(&strcol_chars[strcol_offsets[i - 1]]);

                int pad_len = padcol_offsets[i] - padcol_offsets[i - 1] - 1;
                const char* pad_data =
                        reinterpret_cast<const char*>(&padcol_chars[padcol_offsets[i - 1]]);

                size_t str_char_size =
                        get_char_len(std::string_view(str_data, str_len), &str_index);
                size_t pad_char_size =
                        get_char_len(std::string_view(pad_data, pad_len), &pad_index);

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

} // namespace doris::vectorized
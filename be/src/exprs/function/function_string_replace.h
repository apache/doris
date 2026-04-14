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

#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>

#include "common/compiler_util.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/string_ref.h"
#include "exec/common/stringop_substring.h"
#include "exec/common/template_helpers.hpp"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function_context.h"
#include "util/simd/vstring_function.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"

struct ReplaceImpl {
    static constexpr auto name = "replace";
};

struct ReplaceEmptyImpl {
    static constexpr auto name = "replace_empty";
};

template <typename Impl, bool empty>
class FunctionReplace : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionReplace<Impl, empty>>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeString>()};
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        // We need a local variable to hold a reference to the converted column.
        // So that the converted column will not be released before we use it.
        ColumnPtr col[3];
        bool col_const[3];
        for (size_t i = 0; i < 3; ++i) {
            std::tie(col[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }

        const auto* col_origin_str = assert_cast<const ColumnString*>(col[0].get());
        const auto* col_old_str = assert_cast<const ColumnString*>(col[1].get());
        const auto* col_new_str = assert_cast<const ColumnString*>(col[2].get());

        ColumnString::MutablePtr col_res = ColumnString::create();

        std::visit(
                [&](auto origin_str_const, auto old_str_const, auto new_str_const) {
                    for (int i = 0; i < input_rows_count; ++i) {
                        StringRef origin_str =
                                col_origin_str->get_data_at(index_check_const<origin_str_const>(i));
                        StringRef old_str =
                                col_old_str->get_data_at(index_check_const<old_str_const>(i));
                        StringRef new_str =
                                col_new_str->get_data_at(index_check_const<new_str_const>(i));

                        std::string result =
                                replace(origin_str.to_string(), old_str.to_string_view(),
                                        new_str.to_string_view());

                        col_res->insert_data(result.data(), result.length());
                    }
                },
                make_bool_variant(col_const[0]), make_bool_variant(col_const[1]),
                make_bool_variant(col_const[2]));

        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

private:
    std::string replace(std::string str, std::string_view old_str, std::string_view new_str) const {
        if (old_str.empty()) {
            if constexpr (empty) {
                return str;
            } else {
                // Different from "Replace" only when the search string is empty.
                // it will insert `new_str` in front of every character and at the end of the old str.
                if (new_str.empty()) {
                    return str;
                }
                if (simd::VStringFunctions::is_ascii({str.data(), str.size()})) {
                    std::string result;
                    ColumnString::check_chars_length(
                            str.length() * (new_str.length() + 1) + new_str.length(), 0);
                    result.reserve(str.length() * (new_str.length() + 1) + new_str.length());
                    for (char c : str) {
                        result += new_str;
                        result += c;
                    }
                    result += new_str;
                    return result;
                } else {
                    std::string result;
                    result.reserve(str.length() * (new_str.length() + 1) + new_str.length());
                    for (size_t i = 0, utf8_char_len = 0; i < str.size(); i += utf8_char_len) {
                        utf8_char_len = UTF8_BYTE_LENGTH[(unsigned char)str[i]];
                        result += new_str;
                        result.append(&str[i], utf8_char_len);
                    }
                    result += new_str;
                    ColumnString::check_chars_length(result.size(), 0);
                    return result;
                }
            }
        } else {
            std::string::size_type pos = 0;
            std::string::size_type oldLen = old_str.size();
            std::string::size_type newLen = new_str.size();
            while ((pos = str.find(old_str, pos)) != std::string::npos) {
                str.replace(pos, oldLen, new_str);
                pos += newLen;
            }
            return str;
        }
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
            std::string dst;
            dst.resize(src_len);
            simd::VStringFunctions::reverse(StringRef((uint8_t*)src_str, src_len), &dst);
            StringOP::push_value_string(std::string_view(dst.data(), src_len), i, res_data,
                                        res_offsets);
        }
        return Status::OK();
    }
};

template <typename Impl>
class FunctionSubReplace : public IFunction {
public:
    static constexpr auto name = "sub_replace";

    static FunctionPtr create() { return std::make_shared<FunctionSubReplace<Impl>>(); }

    String get_name() const override { return name; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool is_variadic() const override { return true; }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct SubReplaceImpl {
    static Status replace_execute(Block& block, const ColumnNumbers& arguments, uint32_t result,
                                  size_t input_rows_count) {
        auto res_column = ColumnString::create();
        auto* result_column = assert_cast<ColumnString*>(res_column.get());
        auto args_null_map = ColumnUInt8::create(input_rows_count, 0);
        ColumnPtr argument_columns[4];
        bool col_const[4];
        for (int i = 0; i < 4; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }
        const auto* data_column = assert_cast<const ColumnString*>(argument_columns[0].get());
        const auto* mask_column = assert_cast<const ColumnString*>(argument_columns[1].get());
        const auto* start_column = assert_cast<const ColumnInt32*>(argument_columns[2].get());
        const auto* length_column = assert_cast<const ColumnInt32*>(argument_columns[3].get());

        std::visit(
                [&](auto origin_str_const, auto new_str_const, auto start_const, auto len_const) {
                    if (data_column->is_ascii()) {
                        vector_ascii<origin_str_const, new_str_const, start_const, len_const>(
                                data_column, mask_column, start_column->get_data(),
                                length_column->get_data(), args_null_map->get_data(), result_column,
                                input_rows_count);
                    } else {
                        vector_utf8<origin_str_const, new_str_const, start_const, len_const>(
                                data_column, mask_column, start_column->get_data(),
                                length_column->get_data(), args_null_map->get_data(), result_column,
                                input_rows_count);
                    }
                },
                make_bool_variant(col_const[0]), make_bool_variant(col_const[1]),
                make_bool_variant(col_const[2]), make_bool_variant(col_const[3]));
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res_column), std::move(args_null_map));
        return Status::OK();
    }

private:
    template <bool origin_str_const, bool new_str_const, bool start_const, bool len_const>
    static void vector_ascii(const ColumnString* data_column, const ColumnString* mask_column,
                             const PaddedPODArray<Int32>& args_start,
                             const PaddedPODArray<Int32>& args_length, NullMap& args_null_map,
                             ColumnString* result_column, size_t input_rows_count) {
        ColumnString::Chars& res_chars = result_column->get_chars();
        ColumnString::Offsets& res_offsets = result_column->get_offsets();
        for (size_t row = 0; row < input_rows_count; ++row) {
            StringRef origin_str =
                    data_column->get_data_at(index_check_const<origin_str_const>(row));
            StringRef new_str = mask_column->get_data_at(index_check_const<new_str_const>(row));
            const auto start = args_start[index_check_const<start_const>(row)];
            const auto length = args_length[index_check_const<len_const>(row)];
            const size_t origin_str_len = origin_str.size;
            //input is null, start < 0, len < 0, str_size <= start. return NULL
            if (args_null_map[row] || start < 0 || length < 0 || origin_str_len <= start) {
                res_offsets.push_back(res_chars.size());
                args_null_map[row] = 1;
            } else {
                std::string_view replace_str = new_str.to_string_view();
                std::string result = origin_str.to_string();
                result.replace(start, length, replace_str);
                result_column->insert_data(result.data(), result.length());
            }
        }
    }

    template <bool origin_str_const, bool new_str_const, bool start_const, bool len_const>
    static void vector_utf8(const ColumnString* data_column, const ColumnString* mask_column,
                            const PaddedPODArray<Int32>& args_start,
                            const PaddedPODArray<Int32>& args_length, NullMap& args_null_map,
                            ColumnString* result_column, size_t input_rows_count) {
        ColumnString::Chars& res_chars = result_column->get_chars();
        ColumnString::Offsets& res_offsets = result_column->get_offsets();

        for (size_t row = 0; row < input_rows_count; ++row) {
            StringRef origin_str =
                    data_column->get_data_at(index_check_const<origin_str_const>(row));
            StringRef new_str = mask_column->get_data_at(index_check_const<new_str_const>(row));
            const auto start = args_start[index_check_const<start_const>(row)];
            const auto length = args_length[index_check_const<len_const>(row)];
            //input is null, start < 0, len < 0 return NULL
            if (args_null_map[row] || start < 0 || length < 0) {
                res_offsets.push_back(res_chars.size());
                args_null_map[row] = 1;
                continue;
            }

            const auto [start_byte_len, start_char_len] =
                    simd::VStringFunctions::iterate_utf8_with_limit_length(origin_str.begin(),
                                                                           origin_str.end(), start);

            // start >= orgin.size
            DCHECK(start_char_len <= start);
            if (start_byte_len == origin_str.size) {
                res_offsets.push_back(res_chars.size());
                args_null_map[row] = 1;
                continue;
            }

            auto [end_byte_len, end_char_len] =
                    simd::VStringFunctions::iterate_utf8_with_limit_length(
                            origin_str.begin() + start_byte_len, origin_str.end(), length);
            DCHECK(end_char_len <= length);
            std::string_view replace_str = new_str.to_string_view();
            std::string result = origin_str.to_string();
            result.replace(start_byte_len, end_byte_len, replace_str);
            result_column->insert_data(result.data(), result.length());
        }
    }
};

struct SubReplaceThreeImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto params = ColumnInt32::create(input_rows_count);
        auto& strlen_data = params->get_data();

        auto str_col =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        if (const auto* nullable = check_and_get_column<const ColumnNullable>(*str_col)) {
            str_col = nullable->get_nested_column_ptr();
        }
        const auto* str_column = assert_cast<const ColumnString*>(str_col.get());
        // use utf8 len
        for (int i = 0; i < input_rows_count; ++i) {
            StringRef str_ref = str_column->get_data_at(i);
            strlen_data[i] = simd::VStringFunctions::get_char_len(str_ref.data, str_ref.size);
        }

        block.insert({std::move(params), std::make_shared<DataTypeInt32>(), "strlen"});
        ColumnNumbers temp_arguments = {arguments[0], arguments[1], arguments[2],
                                        block.columns() - 1};
        return SubReplaceImpl::replace_execute(block, temp_arguments, result, input_rows_count);
    }
};

struct SubReplaceFourImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        return SubReplaceImpl::replace_execute(block, arguments, result, input_rows_count);
    }
};

class FunctionOverlay : public IFunction {
public:
    static constexpr auto name = "overlay";
    static FunctionPtr create() { return std::make_shared<FunctionOverlay>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 4; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 4);

        bool col_const[4];
        ColumnPtr argument_columns[4];
        for (int i = 0; i < 4; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }

        const auto* col_origin = assert_cast<const ColumnString*>(argument_columns[0].get());

        const auto* col_pos =
                assert_cast<const ColumnInt32*>(argument_columns[1].get())->get_data().data();
        const auto* col_len =
                assert_cast<const ColumnInt32*>(argument_columns[2].get())->get_data().data();
        const auto* col_insert = assert_cast<const ColumnString*>(argument_columns[3].get());

        ColumnString::MutablePtr col_res = ColumnString::create();

        // if all input string is ascii, we can use ascii function to handle it
        const bool is_all_ascii = col_origin->is_ascii() && col_insert->is_ascii();
        std::visit(
                [&](auto origin_const, auto pos_const, auto len_const, auto insert_const) {
                    if (is_all_ascii) {
                        vector_ascii<origin_const, pos_const, len_const, insert_const>(
                                col_origin, col_pos, col_len, col_insert, col_res,
                                input_rows_count);
                    } else {
                        vector_utf8<origin_const, pos_const, len_const, insert_const>(
                                col_origin, col_pos, col_len, col_insert, col_res,
                                input_rows_count);
                    }
                },
                make_bool_variant(col_const[0]), make_bool_variant(col_const[1]),
                make_bool_variant(col_const[2]), make_bool_variant(col_const[3]));
        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

private:
    template <bool origin_const, bool pos_const, bool len_const, bool insert_const>
    static void vector_ascii(const ColumnString* col_origin, int const* col_pos, int const* col_len,
                             const ColumnString* col_insert, ColumnString::MutablePtr& col_res,
                             size_t input_rows_count) {
        auto& col_res_chars = col_res->get_chars();
        auto& col_res_offsets = col_res->get_offsets();
        StringRef origin_str, insert_str;
        for (size_t i = 0; i < input_rows_count; i++) {
            origin_str = col_origin->get_data_at(index_check_const<origin_const>(i));
            // pos is 1-based index,so we need to minus 1
            const auto pos = col_pos[index_check_const<pos_const>(i)] - 1;
            const auto len = col_len[index_check_const<len_const>(i)];
            insert_str = col_insert->get_data_at(index_check_const<insert_const>(i));
            const auto origin_size = origin_str.size;
            if (pos >= origin_size || pos < 0) {
                // If pos is not within the length of the string, the original string is returned.
                col_res->insert_data(origin_str.data, origin_str.size);
                continue;
            }
            col_res_chars.insert(origin_str.data,
                                 origin_str.data + pos); // copy origin_str with index 0 to pos - 1
            if (pos + len > origin_size || len < 0) {
                col_res_chars.insert(insert_str.begin(),
                                     insert_str.end()); // copy all of insert_str.
            } else {
                col_res_chars.insert(insert_str.begin(),
                                     insert_str.end()); // copy all of insert_str.
                col_res_chars.insert(
                        origin_str.data + pos + len,
                        origin_str.end()); // copy origin_str from pos+len-1 to the end of the line.
            }
            ColumnString::check_chars_length(col_res_chars.size(), col_res_offsets.size());
            col_res_offsets.push_back(col_res_chars.size());
        }
    }

    template <bool origin_const, bool pos_const, bool len_const, bool insert_const>
    NO_SANITIZE_UNDEFINED static void vector_utf8(const ColumnString* col_origin,
                                                  int const* col_pos, int const* col_len,
                                                  const ColumnString* col_insert,
                                                  ColumnString::MutablePtr& col_res,
                                                  size_t input_rows_count) {
        auto& col_res_chars = col_res->get_chars();
        auto& col_res_offsets = col_res->get_offsets();
        StringRef origin_str, insert_str;
        // utf8_origin_offsets is used to store the offset of each utf8 character in the original string.
        // for example, if the original string is "丝多a睿", utf8_origin_offsets will be {0, 3, 6, 7}.
        std::vector<size_t> utf8_origin_offsets;
        for (size_t i = 0; i < input_rows_count; i++) {
            origin_str = col_origin->get_data_at(index_check_const<origin_const>(i));
            // pos is 1-based index,so we need to minus 1
            const auto pos = col_pos[index_check_const<pos_const>(i)] - 1;
            const auto len = col_len[index_check_const<len_const>(i)];
            insert_str = col_insert->get_data_at(index_check_const<insert_const>(i));
            utf8_origin_offsets.clear();

            for (size_t ni = 0, char_size = 0; ni < origin_str.size; ni += char_size) {
                utf8_origin_offsets.push_back(ni);
                char_size = get_utf8_byte_length(origin_str.data[ni]);
            }

            const size_t utf8_origin_size = utf8_origin_offsets.size();

            if (pos >= utf8_origin_size || pos < 0) {
                // If pos is not within the length of the string, the original string is returned.
                col_res->insert_data(origin_str.data, origin_str.size);
                continue;
            }
            col_res_chars.insert(
                    origin_str.data,
                    origin_str.data +
                            utf8_origin_offsets[pos]); // copy origin_str with index 0 to pos - 1
            if (pos + len >= utf8_origin_size || len < 0) {
                col_res_chars.insert(insert_str.begin(),
                                     insert_str.end()); // copy all of insert_str.
            } else {
                col_res_chars.insert(insert_str.begin(),
                                     insert_str.end()); // copy all of insert_str.
                col_res_chars.insert(
                        origin_str.data + utf8_origin_offsets[pos + len],
                        origin_str.end()); // copy origin_str from pos+len-1 to the end of the line.
            }
            ColumnString::check_chars_length(col_res_chars.size(), col_res_offsets.size());
            col_res_offsets.push_back(col_res_chars.size());
        }
    }
};

#include "common/compile_check_avoid_end.h"
} // namespace doris

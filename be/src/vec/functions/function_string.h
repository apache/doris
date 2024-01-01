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

#include <glog/logging.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include <algorithm>
#include <array>
#include <boost/iterator/iterator_facade.hpp>
#include <cstddef>
#include <memory>
#include <ostream>
#include <tuple>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "runtime/decimalv2_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_search.hpp"
#include "util/sha.h"
#include "util/string_util.h"
#include "util/utf8_check.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/common/int_exp.h"
#include "vec/common/memcmp_small.h"
#include "vec/common/memcpy_small.h"
#include "vec/common/pod_array.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/io/io_helper.h"

#ifndef USE_LIBCPP
#include <memory_resource>
#define PMR std::pmr
#else
#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include <boost/container/pmr/vector.hpp>
#define PMR boost::container::pmr
#endif

#include <fmt/format.h>

#include <cstdint>
#include <string>
#include <string_view>

#include "exprs/math_functions.h"
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
#include "vec/common/pinyin.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

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
        ColumnString::check_chars_length(chars.size() + string_value.size(), offsets.size());

        chars.insert(string_value.data(), string_value.data() + string_value.size());
        offsets[index] = chars.size();
    }
};

struct SubstringUtil {
    static constexpr auto name = "substring";

    static void substring_execute(Block& block, const ColumnNumbers& arguments, size_t result,
                                  size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 3);
        auto res = ColumnString::create();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        bool col_const[3];
        ColumnPtr argument_columns[3];
        for (int i = 0; i < 3; ++i) {
            col_const[i] = is_column_const(*block.get_by_position(arguments[i]).column);
        }
        argument_columns[0] = col_const[0] ? static_cast<const ColumnConst&>(
                                                     *block.get_by_position(arguments[0]).column)
                                                     .convert_to_full_column()
                                           : block.get_by_position(arguments[0]).column;

        default_preprocess_parameter_columns(argument_columns, col_const, {1, 2}, block, arguments);

        for (int i = 0; i < 3; i++) {
            check_set_nullable(argument_columns[i], null_map, col_const[i]);
        }

        auto specific_str_column = assert_cast<const ColumnString*>(argument_columns[0].get());
        auto specific_start_column =
                assert_cast<const ColumnVector<Int32>*>(argument_columns[1].get());
        auto specific_len_column =
                assert_cast<const ColumnVector<Int32>*>(argument_columns[2].get());
        if (col_const[1] && col_const[2]) {
            vectors<true>(specific_str_column->get_chars(), specific_str_column->get_offsets(),
                          specific_start_column->get_data(), specific_len_column->get_data(),
                          null_map->get_data(), res->get_chars(), res->get_offsets());
        } else {
            vectors<false>(specific_str_column->get_chars(), specific_str_column->get_offsets(),
                           specific_start_column->get_data(), specific_len_column->get_data(),
                           null_map->get_data(), res->get_chars(), res->get_offsets());
        }
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
    }

private:
    template <bool Const>
    static void vectors(const ColumnString::Chars& chars, const ColumnString::Offsets& offsets,
                        const PaddedPODArray<Int32>& start, const PaddedPODArray<Int32>& len,
                        NullMap& null_map, ColumnString::Chars& res_chars,
                        ColumnString::Offsets& res_offsets) {
        size_t size = offsets.size();
        res_offsets.resize(size);
        res_chars.reserve(chars.size());

        std::array<std::byte, 128 * 1024> buf;
        PMR::monotonic_buffer_resource pool {buf.data(), buf.size()};
        PMR::vector<size_t> index {&pool};

        auto* __restrict data_ptr = chars.data();
        auto* __restrict offset_ptr = offsets.data();

        if constexpr (Const) {
            const auto start_value = start[0];
            const auto len_value = len[0];
            if (start_value == 0 || len_value <= 0) {
                for (size_t i = 0; i < size; ++i) {
                    StringOP::push_empty_string(i, res_chars, res_offsets);
                }
            } else {
                for (size_t i = 0; i < size; ++i) {
                    const int str_size = offset_ptr[i] - offset_ptr[i - 1];
                    const uint8_t* raw_str = data_ptr + offset_ptr[i - 1];
                    // return empty string if start > src.length
                    if (start_value > str_size || start_value < -str_size || str_size == 0) {
                        StringOP::push_empty_string(i, res_chars, res_offsets);
                        continue;
                    }
                    // reference to string_function.cpp: substring
                    size_t byte_pos = 0;
                    index.clear();
                    for (size_t j = 0, char_size = 0;
                         j < str_size &&
                         (start_value <= 0 || index.size() <= start_value + len_value);
                         j += char_size) {
                        char_size = UTF8_BYTE_LENGTH[(unsigned char)(raw_str)[j]];
                        index.push_back(j);
                    }

                    int fixed_pos = start_value;
                    if (fixed_pos < 0) {
                        fixed_pos = str_size + fixed_pos + 1;
                    } else if (fixed_pos > index.size()) {
                        StringOP::push_null_string(i, res_chars, res_offsets, null_map);
                        continue;
                    }

                    byte_pos = index[fixed_pos - 1];
                    int fixed_len = str_size - byte_pos;
                    if (fixed_pos + len_value <= index.size()) {
                        fixed_len = index[fixed_pos + len_value - 1] - byte_pos;
                    }

                    if (byte_pos <= str_size && fixed_len > 0) {
                        // return StringRef(str.data + byte_pos, fixed_len);
                        StringOP::push_value_string(
                                std::string_view {reinterpret_cast<const char*>(raw_str + byte_pos),
                                                  (size_t)fixed_len},
                                i, res_chars, res_offsets);
                    } else {
                        StringOP::push_empty_string(i, res_chars, res_offsets);
                    }
                }
            }
        } else {
            PMR::vector<std::pair<const unsigned char*, int>> strs(&pool);
            strs.resize(size);
            for (int i = 0; i < size; ++i) {
                strs[i].first = data_ptr + offset_ptr[i - 1];
                strs[i].second = offset_ptr[i] - offset_ptr[i - 1];
            }

            for (size_t i = 0; i < size; ++i) {
                auto [raw_str, str_size] = strs[i];
                const auto& start_value = start[i];
                const auto& len_value = len[i];

                // return empty string if start > src.length
                if (start_value > str_size || str_size == 0 || start_value == 0 || len_value <= 0) {
                    StringOP::push_empty_string(i, res_chars, res_offsets);
                    continue;
                }
                // reference to string_function.cpp: substring
                size_t byte_pos = 0;
                index.clear();
                for (size_t j = 0, char_size = 0; j < str_size; j += char_size) {
                    char_size = UTF8_BYTE_LENGTH[(unsigned char)(raw_str)[j]];
                    index.push_back(j);
                    if (start_value > 0 && index.size() > start_value + len_value) {
                        break;
                    }
                }

                int fixed_pos = start_value;
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
                if (fixed_pos + len_value <= index.size()) {
                    fixed_len = index[fixed_pos + len_value - 1] - byte_pos;
                }

                if (byte_pos <= str_size && fixed_len > 0) {
                    // return StringRef(str.data + byte_pos, fixed_len);
                    StringOP::push_value_string(
                            std::string_view {reinterpret_cast<const char*>(raw_str + byte_pos),
                                              (size_t)fixed_len},
                            i, res_chars, res_offsets);
                } else {
                    StringOP::push_empty_string(i, res_chars, res_offsets);
                }
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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
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
        auto col_len = ColumnInt32::create(input_rows_count);
        auto& strlen_data = col_len->get_data();

        ColumnPtr str_col;
        bool str_const;
        std::tie(str_col, str_const) = unpack_if_const(block.get_by_position(arguments[0]).column);
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*str_col)) {
            str_col = nullable->get_nested_column_ptr();
        }
        auto& str_offset = assert_cast<const ColumnString*>(str_col.get())->get_offsets();

        if (str_const) {
            std::fill(strlen_data.begin(), strlen_data.end(), str_offset[0] - str_offset[-1]);
        } else {
            for (int i = 0; i < input_rows_count; ++i) {
                strlen_data[i] = str_offset[i] - str_offset[i - 1];
            }
        }

        // we complete the column2(strlen) with the default value - each row's strlen.
        block.insert({std::move(col_len), std::make_shared<DataTypeInt32>(), "strlen"});
        ColumnNumbers temp_arguments = {arguments[0], arguments[1], block.columns() - 1};

        SubstringUtil::substring_execute(block, temp_arguments, result, input_rows_count);
        return Status::OK();
    }
};

template <bool Reverse>
class FunctionMaskPartial;

class FunctionMask : public IFunction {
public:
    static constexpr auto name = "mask";
    static constexpr unsigned char DEFAULT_UPPER_MASK = 'X';
    static constexpr unsigned char DEFAULT_LOWER_MASK = 'x';
    static constexpr unsigned char DEFAULT_NUMBER_MASK = 'n';
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionMask>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 1);
        DCHECK_LE(arguments.size(), 4);

        char upper = DEFAULT_UPPER_MASK, lower = DEFAULT_LOWER_MASK, number = DEFAULT_NUMBER_MASK;

        auto res = ColumnString::create();
        const auto& source_column =
                assert_cast<const ColumnString&>(*block.get_by_position(arguments[0]).column);

        if (arguments.size() > 1) {
            auto& col = *block.get_by_position(arguments[1]).column;
            auto string_ref = col.get_data_at(0);
            if (string_ref.size > 0) {
                upper = *string_ref.data;
            }
        }

        if (arguments.size() > 2) {
            auto& col = *block.get_by_position(arguments[2]).column;
            auto string_ref = col.get_data_at(0);
            if (string_ref.size > 0) {
                lower = *string_ref.data;
            }
        }

        if (arguments.size() > 3) {
            auto& col = *block.get_by_position(arguments[3]).column;
            auto string_ref = col.get_data_at(0);
            if (string_ref.size > 0) {
                number = *string_ref.data;
            }
        }

        if (arguments.size() > 4) {
            return Status::InvalidArgument(
                    fmt::format("too many arguments for function {}", get_name()));
        }

        vector_mask(source_column, *res, upper, lower, number);

        block.get_by_position(result).column = std::move(res);

        return Status::OK();
    }
    friend class FunctionMaskPartial<true>;
    friend class FunctionMaskPartial<false>;

private:
    static void vector_mask(const ColumnString& source, ColumnString& result, const char upper,
                            const char lower, const char number) {
        result.get_chars().resize(source.get_chars().size());
        result.get_offsets().resize(source.get_offsets().size());
        memcpy_small_allow_read_write_overflow15(
                result.get_offsets().data(), source.get_offsets().data(),
                source.get_offsets().size() * sizeof(ColumnString::Offset));

        const unsigned char* src = source.get_chars().data();
        const size_t size = source.get_chars().size();
        unsigned char* res = result.get_chars().data();
        mask(src, size, upper, lower, number, res);
    }

    static void mask(const unsigned char* __restrict src, const size_t size,
                     const unsigned char upper, const unsigned char lower,
                     const unsigned char number, unsigned char* __restrict res) {
        for (size_t i = 0; i != size; ++i) {
            auto c = src[i];
            if (c >= 'A' && c <= 'Z') {
                res[i] = upper;
            } else if (c >= 'a' && c <= 'z') {
                res[i] = lower;
            } else if (c >= '0' && c <= '9') {
                res[i] = number;
            } else {
                res[i] = c;
            }
        }
    }
};

template <bool Reverse>
class FunctionMaskPartial : public IFunction {
public:
    static constexpr auto name = Reverse ? "mask_last_n" : "mask_first_n";
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionMaskPartial>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 1);
        DCHECK_LE(arguments.size(), 2);

        int n = -1;

        auto res = ColumnString::create();
        auto col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnString& source_column = assert_cast<const ColumnString&>(*col);

        if (arguments.size() == 2) {
            auto& col = *block.get_by_position(arguments[1]).column;
            n = col.get_int(0);
        } else if (arguments.size() > 2) {
            return Status::InvalidArgument(
                    fmt::format("too many arguments for function {}", get_name()));
        }

        if (n == -1) {
            FunctionMask::vector_mask(source_column, *res, FunctionMask::DEFAULT_UPPER_MASK,
                                      FunctionMask::DEFAULT_LOWER_MASK,
                                      FunctionMask::DEFAULT_NUMBER_MASK);
        } else if (n >= 0) {
            vector(source_column, n, *res);
        }

        block.get_by_position(result).column = std::move(res);

        return Status::OK();
    }

private:
    static void vector(const ColumnString& src, int n, ColumnString& result) {
        const auto num_rows = src.size();
        auto* chars = src.get_chars().data();
        auto* offsets = src.get_offsets().data();
        result.get_chars().resize(src.get_chars().size());
        result.get_offsets().resize(src.get_offsets().size());
        memcpy_small_allow_read_write_overflow15(
                result.get_offsets().data(), src.get_offsets().data(),
                src.get_offsets().size() * sizeof(ColumnString::Offset));
        auto* res = result.get_chars().data();

        for (ssize_t i = 0; i != num_rows; ++i) {
            auto offset = offsets[i - 1];
            int len = offsets[i] - offset;
            if constexpr (Reverse) {
                auto start = std::max(len - n, 0);
                if (start > 0) {
                    memcpy(&res[offset], &chars[offset], start);
                }
                offset += start;
            } else {
                if (n < len) {
                    memcpy(&res[offset + n], &chars[offset + n], len - n);
                }
            }

            len = std::min(n, len);
            FunctionMask::mask(&chars[offset], len, FunctionMask::DEFAULT_UPPER_MASK,
                               FunctionMask::DEFAULT_LOWER_MASK, FunctionMask::DEFAULT_NUMBER_MASK,
                               &res[offset]);
        }
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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
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

struct NullOrEmptyImpl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeUInt8>()}; }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result, size_t input_rows_count, bool reverse) {
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
        if (reverse) {
            for (int i = 0; i < input_rows_count; ++i) {
                res_map_data[i] = !res_map_data[i];
            }
        }

        block.replace_by_position(result, std::move(res_map));
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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        static_cast<void>(NullOrEmptyImpl::execute(context, block, arguments, result,
                                                   input_rows_count, false));
        return Status::OK();
    }
};

class FunctionNotNullOrEmpty : public IFunction {
public:
    static constexpr auto name = "not_null_or_empty";
    static FunctionPtr create() { return std::make_shared<FunctionNotNullOrEmpty>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        static_cast<void>(NullOrEmptyImpl::execute(context, block, arguments, result,
                                                   input_rows_count, true));
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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
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

        size_t res_reserve_size = 0;
        // we could ignore null string column
        // but it's not necessary to ignore it
        for (size_t i = 0; i < offsets_list.size(); ++i) {
            for (size_t j = 0; j < input_rows_count; ++j) {
                size_t append = (*offsets_list[i])[j] - (*offsets_list[i])[j - 1];
                // check whether the concat output might overflow(unlikely)
                if (UNLIKELY(UINT_MAX - append < res_reserve_size)) {
                    return Status::BufferAllocFailed("concat output is too large to allocate");
                }
                res_reserve_size += append;
            }
        }
        if ((UNLIKELY(UINT_MAX - input_rows_count < res_reserve_size))) {
            return Status::BufferAllocFailed("concat output is too large to allocate");
        }

        res_data.resize(res_reserve_size);

        for (size_t i = 0; i < input_rows_count; ++i) {
            int current_length = 0;
            for (size_t j = 0; j < offsets_list.size(); ++j) {
                auto& current_offsets = *offsets_list[j];
                auto& current_chars = *chars_list[j];

                int size = current_offsets[i] - current_offsets[i - 1];
                if (size > 0) {
                    memcpy_small_allow_read_write_overflow15(
                            &res_data[res_offset[i - 1]] + current_length,
                            &current_chars[current_offsets[i - 1]], size);
                    current_length += size;
                }
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
        return make_nullable(std::make_shared<DataTypeString>());
    }
    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        int arguent_size = arguments.size();
        int num_children = arguent_size - 1;
        auto res = ColumnString::create();

        if (auto const_column = check_and_get_column<ColumnConst>(
                    *block.get_by_position(arguments[0]).column)) {
            auto data = const_column->get_data_at(0);
            // return NULL, pos is null or pos < 0 or pos > num_children
            auto is_null = data.data == nullptr;
            auto pos = is_null ? 0 : *(Int32*)data.data;
            is_null = pos <= 0 || pos > num_children;

            auto null_map = ColumnUInt8::create(input_rows_count, is_null);
            if (is_null) {
                res->insert_many_defaults(input_rows_count);
            } else {
                auto& target_column = block.get_by_position(arguments[pos]).column;
                if (auto target_const_column = check_and_get_column<ColumnConst>(*target_column)) {
                    auto target_data = target_const_column->get_data_at(0);
                    if (target_data.data == nullptr) {
                        null_map = ColumnUInt8::create(input_rows_count, is_null);
                        res->insert_many_defaults(input_rows_count);
                    } else {
                        res->insert_many_data(target_data.data, target_data.size, input_rows_count);
                    }
                } else if (auto target_nullable_column =
                                   check_and_get_column<ColumnNullable>(*target_column)) {
                    auto& target_null_map = target_nullable_column->get_null_map_data();
                    VectorizedUtils::update_null_map(
                            assert_cast<ColumnUInt8&>(*null_map).get_data(), target_null_map);

                    auto& target_str_column = assert_cast<const ColumnString&>(
                            target_nullable_column->get_nested_column());
                    res->get_chars().assign(target_str_column.get_chars().begin(),
                                            target_str_column.get_chars().end());
                    res->get_offsets().assign(target_str_column.get_offsets().begin(),
                                              target_str_column.get_offsets().end());
                } else {
                    auto& target_str_column = assert_cast<const ColumnString&>(*target_column);
                    res->get_chars().assign(target_str_column.get_chars().begin(),
                                            target_str_column.get_chars().end());
                    res->get_offsets().assign(target_str_column.get_offsets().begin(),
                                              target_str_column.get_offsets().end());
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(res), std::move(null_map));
        } else if (auto pos_null_column = check_and_get_column<ColumnNullable>(
                           *block.get_by_position(arguments[0]).column)) {
            auto& pos_column =
                    assert_cast<const ColumnInt32&>(pos_null_column->get_nested_column());
            auto& pos_null_map = pos_null_column->get_null_map_data();
            auto null_map = ColumnUInt8::create(input_rows_count, false);
            auto& res_null_map = assert_cast<ColumnUInt8&>(*null_map).get_data();

            for (size_t i = 0; i < input_rows_count; ++i) {
                auto pos = pos_column.get_element(i);
                res_null_map[i] =
                        pos_null_map[i] || pos <= 0 || pos > num_children ||
                        block.get_by_position(arguments[pos]).column->get_data_at(i).data ==
                                nullptr;
                if (res_null_map[i]) {
                    res->insert_default();
                } else {
                    auto insert_data = block.get_by_position(arguments[pos]).column->get_data_at(i);
                    res->insert_data(insert_data.data, insert_data.size);
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(res), std::move(null_map));
        } else {
            auto& pos_column =
                    assert_cast<const ColumnInt32&>(*block.get_by_position(arguments[0]).column);
            auto null_map = ColumnUInt8::create(input_rows_count, false);
            auto& res_null_map = assert_cast<ColumnUInt8&>(*null_map).get_data();

            for (size_t i = 0; i < input_rows_count; ++i) {
                auto pos = pos_column.get_element(i);
                res_null_map[i] =
                        pos <= 0 || pos > num_children ||
                        block.get_by_position(arguments[pos]).column->get_data_at(i).data ==
                                nullptr;
                if (res_null_map[i]) {
                    res->insert_default();
                } else {
                    auto insert_data = block.get_by_position(arguments[pos]).column->get_data_at(i);
                    res->insert_data(insert_data.data, insert_data.size);
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(res), std::move(null_map));
        }
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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
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
                        Chars& res_data, Offsets& res_offset) const {
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
                         Chars& res_data, Offsets& res_offset) const {
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
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
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
                              res->get_chars(), res->get_offsets(), null_map->get_data(),
                              context->state()->repeat_max_num());
                block.replace_by_position(
                        result, ColumnNullable::create(std::move(res), std::move(null_map)));
                return Status::OK();
            } else if (auto* col2_const = check_and_get_column<ColumnConst>(*argument_ptr[1])) {
                DCHECK(check_and_get_column<ColumnInt32>(col2_const->get_data_column()));
                int repeat = 0;
                repeat = std::min<int>(col2_const->get_int(0), context->state()->repeat_max_num());

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
                       ColumnString::Offsets& res_offsets, ColumnUInt8::Container& null_map,
                       const int repeat_max_num) const {
        size_t input_row_size = offsets.size();

        fmt::memory_buffer buffer;
        res_offsets.resize(input_row_size);
        null_map.resize_fill(input_row_size, 0);
        for (ssize_t i = 0; i < input_row_size; ++i) {
            buffer.clear();
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t size = offsets[i] - offsets[i - 1];
            int repeat = 0;
            repeat = std::min<int>(repeats[i], repeat_max_num);

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
                      ColumnUInt8::Container& null_map) const {
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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
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
                        simd::VStringFunctions::get_char_len(str_data, str_len, str_index);
                size_t pad_char_size =
                        simd::VStringFunctions::get_char_len(pad_data, pad_len, pad_index);

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
                // StringRef result(context, byte_len);
                if constexpr (Impl::is_lpad) {
                    int result_index = 0;

                    // Prepend chars of pad.
                    while (result_index < pad_byte_len) {
                        int remain = std::min(pad_len, pad_byte_len - result_index);
                        buffer.append(pad_data, pad_data + remain);
                        result_index += remain;
                    }

                    // Append given string.
                    buffer.append(str_data, str_data + str_len);
                    StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i,
                                                res_chars, res_offsets);

                } else {
                    // is rpad
                    buffer.append(str_data, str_data + str_len);

                    // Append chars of pad until desired length
                    int result_len = str_len;
                    while (result_len < byte_len) {
                        int remain = std::min(pad_len, byte_len - result_len);
                        buffer.append(pad_data, pad_data + remain);
                        result_len += remain;
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

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 3);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        // Create a zero column to simply implement
        auto const_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res = ColumnString::create();

        auto& null_map_data = null_map->get_data();
        auto& res_offsets = res->get_offsets();
        auto& res_chars = res->get_chars();
        res_offsets.resize(input_rows_count);

        const size_t argument_size = arguments.size();
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
            if (part_num_col_data[i] == 0) {
                StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                continue;
            }

            auto delimiter = delimiter_col->get_data_at(i);
            auto delimiter_str = delimiter_col->get_data_at(i).to_string();
            auto part_number = part_num_col_data[i];
            auto str = str_col->get_data_at(i);
            if (delimiter.size == 0) {
                StringOP::push_empty_string(i, res_chars, res_offsets);
                continue;
            }

            if (part_number > 0) {
                if (delimiter.size == 1) {
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
                        char* pos =
                                reinterpret_cast<char*>(memmem(str.data + offset + delimiter.size,
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
                                std::string_view {reinterpret_cast<const char*>(
                                                          str.data + pre_offset + delimiter.size),
                                                  (size_t)offset - pre_offset - delimiter.size},
                                i, res_chars, res_offsets);
                    } else {
                        StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                    }
                }
            } else {
                part_number = -part_number;
                auto str_str = str.to_string();
                int32_t offset = str.size;
                int32_t pre_offset = offset;
                int32_t num = 0;
                auto substr = str_str;
                while (num <= part_number && offset >= 0) {
                    offset = (int)substr.rfind(delimiter, offset);
                    if (offset != -1) {
                        if (++num == part_number) {
                            break;
                        }
                        pre_offset = offset;
                        offset = offset - 1;
                        substr = str_str.substr(0, pre_offset);
                    } else {
                        break;
                    }
                }
                num = (offset == -1 && num != 0) ? num + 1 : num;

                if (num == part_number) {
                    if (offset == -1) {
                        StringOP::push_value_string(
                                std::string_view {reinterpret_cast<const char*>(str.data),
                                                  (size_t)pre_offset},
                                i, res_chars, res_offsets);
                    } else {
                        StringOP::push_value_string(
                                std::string_view {str_str.substr(
                                        offset + delimiter.size,
                                        (size_t)pre_offset - offset - delimiter.size)},
                                i, res_chars, res_offsets);
                    }
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

class FunctionSubstringIndex : public IFunction {
public:
    static constexpr auto name = "substring_index";
    static FunctionPtr create() { return std::make_shared<FunctionSubstringIndex>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 3);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        // Create a zero column to simply implement
        auto const_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res = ColumnString::create();

        auto& res_offsets = res->get_offsets();
        auto& res_chars = res->get_chars();
        res_offsets.resize(input_rows_count);
        ColumnPtr content_column;
        bool content_const = false;
        std::tie(content_column, content_const) =
                unpack_if_const(block.get_by_position(arguments[0]).column);

        if (auto* nullable = check_and_get_column<const ColumnNullable>(*content_column)) {
            // Danger: Here must dispose the null map data first! Because
            // argument_columns[0]=nullable->get_nested_column_ptr(); will release the mem
            // of column nullable mem of null map
            VectorizedUtils::update_null_map(null_map->get_data(), nullable->get_null_map_data());
            content_column = nullable->get_nested_column_ptr();
        }

        auto str_col = assert_cast<const ColumnString*>(content_column.get());

        [[maybe_unused]] const auto& [delimiter_col, delimiter_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        auto delimiter = delimiter_col->get_data_at(0);
        int32_t delimiter_size = delimiter.size;

        [[maybe_unused]] const auto& [part_num_col, part_const] =
                unpack_if_const(block.get_by_position(arguments[2]).column);
        auto part_number = *((int*)part_num_col->get_data_at(0).data);

        if (part_number == 0 || delimiter_size == 0) {
            for (size_t i = 0; i < input_rows_count; ++i) {
                StringOP::push_empty_string(i, res_chars, res_offsets);
            }
        } else if (part_number > 0) {
            if (delimiter_size == 1) {
                // If delimiter is a char, use memchr to split
                for (size_t i = 0; i < input_rows_count; ++i) {
                    auto str = str_col->get_data_at(i);
                    int32_t offset = -1;
                    int32_t num = 0;
                    while (num < part_number) {
                        size_t n = str.size - offset - 1;
                        const char* pos = reinterpret_cast<const char*>(
                                memchr(str.data + offset + 1, delimiter.data[0], n));
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
                                std::string_view {reinterpret_cast<const char*>(str.data),
                                                  (size_t)offset},
                                i, res_chars, res_offsets);
                    } else {
                        StringOP::push_value_string(std::string_view(str.data, str.size), i,
                                                    res_chars, res_offsets);
                    }
                }
            } else {
                StringRef delimiter_ref(delimiter);
                StringSearch search(&delimiter_ref);
                for (size_t i = 0; i < input_rows_count; ++i) {
                    auto str = str_col->get_data_at(i);
                    int32_t offset = -delimiter_size;
                    int32_t num = 0;
                    while (num < part_number) {
                        size_t n = str.size - offset - delimiter_size;
                        // search first match delimter_ref index from src string among str_offset to end
                        const char* pos = search.search(str.data + offset + delimiter_size, n);
                        if (pos < str.data + str.size) {
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
                                std::string_view {reinterpret_cast<const char*>(str.data),
                                                  (size_t)offset},
                                i, res_chars, res_offsets);
                    } else {
                        StringOP::push_value_string(std::string_view(str.data, str.size), i,
                                                    res_chars, res_offsets);
                    }
                }
            }
        } else {
            // if part_number is negative
            part_number = -part_number;
            for (size_t i = 0; i < input_rows_count; ++i) {
                auto str = str_col->get_data_at(i);
                auto str_str = str.to_string();
                int32_t offset = str.size;
                int32_t pre_offset = offset;
                int32_t num = 0;
                auto substr = str_str;
                while (num <= part_number && offset >= 0) {
                    offset = (int)substr.rfind(delimiter, offset);
                    if (offset != -1) {
                        if (++num == part_number) {
                            break;
                        }
                        pre_offset = offset;
                        offset = offset - 1;
                        substr = str_str.substr(0, pre_offset);
                    } else {
                        break;
                    }
                }
                num = (offset == -1 && num != 0) ? num + 1 : num;

                if (num == part_number) {
                    if (offset == -1) {
                        StringOP::push_value_string(std::string_view(str.data, str.size), i,
                                                    res_chars, res_offsets);
                    } else {
                        StringOP::push_value_string(
                                std::string_view {str.data + offset + delimiter_size,
                                                  str.size - offset - delimiter_size},
                                i, res_chars, res_offsets);
                    }
                } else {
                    StringOP::push_value_string(std::string_view(str.data, str.size), i, res_chars,
                                                res_offsets);
                }
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }
};

class FunctionSplitByString : public IFunction {
public:
    static constexpr auto name = "split_by_string";

    static FunctionPtr create() { return std::make_shared<FunctionSplitByString>(); }
    using NullMapType = PaddedPODArray<UInt8>;

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_string(arguments[0]))
                << "first argument for function: " << name << " should be string"
                << " and arguments[0] is " << arguments[0]->get_name();
        DCHECK(is_string(arguments[1]))
                << "second argument for function: " << name << " should be string"
                << " and arguments[1] is " << arguments[1]->get_name();
        return std::make_shared<DataTypeArray>(make_nullable(arguments[0]));
    }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t /*input_rows_count*/) const override {
        DCHECK_EQ(arguments.size(), 2);

        const auto& [src_column, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_column, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        DataTypePtr right_column_type = block.get_by_position(arguments[1]).type;
        DataTypePtr src_column_type = block.get_by_position(arguments[0]).type;
        auto dest_column_ptr = ColumnArray::create(make_nullable(src_column_type)->create_column(),
                                                   ColumnArray::ColumnOffsets::create());

        IColumn* dest_nested_column = &dest_column_ptr->get_data();
        auto& dest_offsets = dest_column_ptr->get_offsets();
        DCHECK(dest_nested_column != nullptr);
        dest_nested_column->reserve(0);
        dest_offsets.reserve(0);

        NullMapType* dest_nested_null_map = nullptr;
        ColumnNullable* dest_nullable_col = reinterpret_cast<ColumnNullable*>(dest_nested_column);
        dest_nested_column = dest_nullable_col->get_nested_column_ptr();
        dest_nested_null_map = &dest_nullable_col->get_null_map_column().get_data();

        auto col_left = check_and_get_column<ColumnString>(src_column.get());
        if (!col_left) {
            return Status::InternalError("Left operator of function {} can not be {}", get_name(),
                                         src_column_type->get_name());
        }

        auto col_right = check_and_get_column<ColumnString>(right_column.get());
        if (!col_right) {
            return Status::InternalError("Right operator of function {} can not be {}", get_name(),
                                         right_column_type->get_name());
        }

        // split_by_string(ColumnString, "xxx")
        if (right_const) {
            _execute_constant_delimiter(*col_left, col_right->get_data_at(0), *dest_nested_column,
                                        dest_offsets, dest_nested_null_map);
        } else if (left_const) {
            // split_by_string("xxx", ColumnString)
            _execute_constant_src_string(col_left->get_data_at(0), *col_right, *dest_nested_column,
                                         dest_offsets, dest_nested_null_map);
        } else {
            // split_by_string(ColumnString, ColumnString)
            _execute_vector(*col_left, *col_right, *dest_nested_column, dest_offsets,
                            dest_nested_null_map);
        }

        block.replace_by_position(result, std::move(dest_column_ptr));

        return Status::OK();
    }

private:
    void _execute_constant_delimiter(const ColumnString& src_column_string,
                                     const StringRef& delimiter_ref, IColumn& dest_nested_column,
                                     ColumnArray::Offsets64& dest_offsets,
                                     NullMapType* dest_nested_null_map) const {
        ColumnString& dest_column_string = reinterpret_cast<ColumnString&>(dest_nested_column);
        ColumnString::Chars& column_string_chars = dest_column_string.get_chars();
        ColumnString::Offsets& column_string_offsets = dest_column_string.get_offsets();
        column_string_chars.reserve(0);

        ColumnArray::Offset64 string_pos = 0;
        ColumnArray::Offset64 dest_pos = 0;
        ColumnArray::Offset64 src_offsets_size = src_column_string.get_offsets().size();

        StringSearch search(&delimiter_ref);

        for (size_t i = 0; i < src_offsets_size; i++) {
            const StringRef str_ref = src_column_string.get_data_at(i);

            if (str_ref.size == 0) {
                dest_offsets.push_back(dest_pos);
                continue;
            }
            if (delimiter_ref.size == 0) {
                for (size_t str_pos = 0; str_pos < str_ref.size;) {
                    const size_t str_offset = str_pos;
                    const size_t old_size = column_string_chars.size();
                    str_pos++;
                    const size_t new_size = old_size + 1;
                    column_string_chars.resize(new_size);
                    memcpy(column_string_chars.data() + old_size, str_ref.data + str_offset, 1);
                    (*dest_nested_null_map).push_back(false);
                    string_pos++;
                    dest_pos++;
                    column_string_offsets.push_back(string_pos);
                }
            } else {
                for (size_t str_pos = 0; str_pos <= str_ref.size;) {
                    const size_t str_offset = str_pos;
                    const size_t old_size = column_string_chars.size();
                    // search first match delimter_ref index from src string among str_offset to end
                    const char* result_start =
                            search.search(str_ref.data + str_offset, str_ref.size - str_offset);
                    // compute split part size
                    const size_t split_part_size = result_start - str_ref.data - str_offset;
                    // save dist string split part
                    if (split_part_size > 0) {
                        const size_t new_size = old_size + split_part_size;
                        column_string_chars.resize(new_size);
                        memcpy_small_allow_read_write_overflow15(
                                column_string_chars.data() + old_size, str_ref.data + str_offset,
                                split_part_size);
                        // add dist string offset
                        string_pos += split_part_size;
                    }
                    column_string_offsets.push_back(string_pos);
                    // not null
                    (*dest_nested_null_map).push_back(false);
                    // array offset + 1
                    dest_pos++;
                    // add src string str_pos to next search start
                    str_pos += split_part_size + delimiter_ref.size;
                }
            }
            dest_offsets.push_back(dest_pos);
        }
    }

    void _execute_vector(const ColumnString& src_column_string,
                         const ColumnString& delimiter_column, IColumn& dest_nested_column,
                         ColumnArray::Offsets64& dest_offsets,
                         NullMapType* dest_nested_null_map) const {
        ColumnString& dest_column_string = reinterpret_cast<ColumnString&>(dest_nested_column);
        ColumnString::Chars& column_string_chars = dest_column_string.get_chars();
        ColumnString::Offsets& column_string_offsets = dest_column_string.get_offsets();
        column_string_chars.reserve(0);

        ColumnArray::Offset64 string_pos = 0;
        ColumnArray::Offset64 dest_pos = 0;
        ColumnArray::Offset64 src_offsets_size = src_column_string.get_offsets().size();

        for (size_t i = 0; i < src_offsets_size; i++) {
            const StringRef delimiter_ref = delimiter_column.get_data_at(i);
            const StringRef str_ref = src_column_string.get_data_at(i);

            if (str_ref.size == 0) {
                dest_offsets.push_back(dest_pos);
                continue;
            }
            if (delimiter_ref.size == 0) {
                for (size_t str_pos = 0; str_pos < str_ref.size;) {
                    const size_t str_offset = str_pos;
                    const size_t old_size = column_string_chars.size();
                    str_pos++;
                    const size_t new_size = old_size + 1;
                    column_string_chars.resize(new_size);
                    memcpy(column_string_chars.data() + old_size, str_ref.data + str_offset, 1);
                    (*dest_nested_null_map).push_back(false);
                    string_pos++;
                    dest_pos++;
                    column_string_offsets.push_back(string_pos);
                }
            } else {
                for (size_t str_pos = 0; str_pos <= str_ref.size;) {
                    const size_t str_offset = str_pos;
                    const size_t old_size = column_string_chars.size();
                    const size_t split_part_size = split_str(str_pos, str_ref, delimiter_ref);
                    str_pos += delimiter_ref.size;
                    const size_t new_size = old_size + split_part_size;
                    column_string_chars.resize(new_size);
                    if (split_part_size > 0) {
                        memcpy_small_allow_read_write_overflow15(
                                column_string_chars.data() + old_size, str_ref.data + str_offset,
                                split_part_size);
                    }
                    (*dest_nested_null_map).push_back(false);
                    string_pos += split_part_size;
                    dest_pos++;
                    column_string_offsets.push_back(string_pos);
                }
            }
            dest_offsets.push_back(dest_pos);
        }
    }

    void _execute_constant_src_string(const StringRef& str_ref, const ColumnString& delimiter_col,
                                      IColumn& dest_nested_column,
                                      ColumnArray::Offsets64& dest_offsets,
                                      NullMapType* dest_nested_null_map) const {
        ColumnString& dest_column_string = reinterpret_cast<ColumnString&>(dest_nested_column);
        ColumnString::Chars& column_string_chars = dest_column_string.get_chars();
        ColumnString::Offsets& column_string_offsets = dest_column_string.get_offsets();
        column_string_chars.reserve(0);

        ColumnArray::Offset64 string_pos = 0;
        ColumnArray::Offset64 dest_pos = 0;
        const ColumnArray::Offset64 delimiter_offsets_size = delimiter_col.get_offsets().size();

        for (size_t i = 0; i < delimiter_offsets_size; ++i) {
            const StringRef delimiter_ref = delimiter_col.get_data_at(i);

            if (delimiter_ref.size == 0) {
                for (size_t str_pos = 0; str_pos < str_ref.size;) {
                    const size_t str_offset = str_pos;
                    const size_t old_size = column_string_chars.size();
                    str_pos++;
                    const size_t new_size = old_size + 1;
                    column_string_chars.resize(new_size);
                    memcpy(column_string_chars.data() + old_size, str_ref.data + str_offset, 1);
                    (*dest_nested_null_map).push_back(false);
                    string_pos++;
                    dest_pos++;
                    column_string_offsets.push_back(string_pos);
                }
            } else {
                for (size_t str_pos = 0; str_pos <= str_ref.size;) {
                    const size_t str_offset = str_pos;
                    const size_t old_size = column_string_chars.size();
                    const size_t split_part_size = split_str(str_pos, str_ref, delimiter_ref);
                    str_pos += delimiter_ref.size;
                    const size_t new_size = old_size + split_part_size;
                    column_string_chars.resize(new_size);
                    if (split_part_size > 0) {
                        memcpy_small_allow_read_write_overflow15(
                                column_string_chars.data() + old_size, str_ref.data + str_offset,
                                split_part_size);
                    }
                    (*dest_nested_null_map).push_back(false);
                    string_pos += split_part_size;
                    dest_pos++;
                    column_string_offsets.push_back(string_pos);
                }
            }
            dest_offsets.push_back(dest_pos);
        }
    }

    size_t split_str(size_t& pos, const StringRef str_ref, StringRef delimiter_ref) const {
        size_t old_size = pos;
        size_t str_size = str_ref.size;
        while (pos < str_size &&
               memcmp_small_allow_overflow15(str_ref.data + pos, delimiter_ref.data,
                                             delimiter_ref.size)) {
            pos++;
        }
        return pos - old_size;
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
class FunctionStringDigestOneArg : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionStringDigestOneArg>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
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

class FunctionStringDigestSHA1 : public IFunction {
public:
    static constexpr auto name = "sha1";
    static FunctionPtr create() { return std::make_shared<FunctionStringDigestSHA1>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 1);

        ColumnPtr str_col = block.get_by_position(arguments[0]).column;
        auto& data = assert_cast<const ColumnString*>(str_col.get())->get_chars();
        auto& offset = assert_cast<const ColumnString*>(str_col.get())->get_offsets();

        auto res_col = ColumnString::create();
        auto& res_data = res_col->get_chars();
        auto& res_offset = res_col->get_offsets();
        res_offset.resize(input_rows_count);

        SHA1Digest digest;
        for (size_t i = 0; i < input_rows_count; ++i) {
            int size = offset[i] - offset[i - 1];
            digest.reset(&data[offset[i - 1]], size);
            std::string_view ans = digest.digest();

            StringOP::push_value_string(ans, i, res_data, res_offset);
        }

        block.replace_by_position(result, std::move(res_col));
        return Status::OK();
    }
};

class FunctionStringDigestSHA2 : public IFunction {
public:
    static constexpr auto name = "sha2";
    static FunctionPtr create() { return std::make_shared<FunctionStringDigestSHA2>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        DCHECK(!is_column_const(*block.get_by_position(arguments[0]).column));

        ColumnPtr str_col = block.get_by_position(arguments[0]).column;
        auto& data = assert_cast<const ColumnString*>(str_col.get())->get_chars();
        auto& offset = assert_cast<const ColumnString*>(str_col.get())->get_offsets();

        [[maybe_unused]] const auto& [right_column, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        auto digest_length = assert_cast<const ColumnInt32*>(right_column.get())->get_data()[0];

        auto res_col = ColumnString::create();
        auto& res_data = res_col->get_chars();
        auto& res_offset = res_col->get_offsets();
        res_offset.resize(input_rows_count);

        if (digest_length == 224) {
            execute_base<SHA224Digest>(data, offset, input_rows_count, res_data, res_offset);
        } else if (digest_length == 256) {
            execute_base<SHA256Digest>(data, offset, input_rows_count, res_data, res_offset);
        } else if (digest_length == 384) {
            execute_base<SHA384Digest>(data, offset, input_rows_count, res_data, res_offset);
        } else if (digest_length == 512) {
            execute_base<SHA512Digest>(data, offset, input_rows_count, res_data, res_offset);
        } else {
            return Status::InvalidArgument(
                    "sha2's digest length only support 224/256/384/512 but meet {}", digest_length);
        }

        block.replace_by_position(result, std::move(res_col));
        return Status::OK();
    }

private:
    template <typename T>
    void execute_base(const ColumnString::Chars& data, const ColumnString::Offsets& offset,
                      int input_rows_count, ColumnString::Chars& res_data,
                      ColumnString::Offsets& res_offset) const {
        T digest;
        for (size_t i = 0; i < input_rows_count; ++i) {
            int size = offset[i] - offset[i - 1];
            digest.reset(&data[offset[i - 1]], size);
            std::string_view ans = digest.digest();

            StringOP::push_value_string(ans, i, res_data, res_offset);
        }
    }
};

class FunctionExtractURLParameter : public IFunction {
public:
    static constexpr auto name = "extract_url_parameter";
    static FunctionPtr create() { return std::make_shared<FunctionExtractURLParameter>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto col_url =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto col_parameter =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto url_col = assert_cast<const ColumnString*>(col_url.get());
        auto parameter_col = assert_cast<const ColumnString*>(col_parameter.get());

        ColumnString::MutablePtr col_res = ColumnString::create();

        for (int i = 0; i < input_rows_count; ++i) {
            auto source = url_col->get_data_at(i);
            auto param = parameter_col->get_data_at(i);
            auto res = extract_url(source, param);

            col_res->insert_data(res.data, res.size);
        }

        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

private:
    StringRef extract_url(StringRef url, StringRef parameter) const {
        if (url.size == 0 || parameter.size == 0) {
            return StringRef("", 0);
        }
        return UrlParser::extract_url(url, parameter);
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
                        size_t result, size_t input_rows_count) const override {
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
            StringRef p(const_cast<char*>(part.data), part.size);
            UrlParser::UrlPart url_part = UrlParser::get_url_part(p);
            StringRef url_key;
            if (has_key) {
                auto key = key_col->get_data_at(i);
                url_key = StringRef(const_cast<char*>(key.data), key.size);
            }

            auto source = url_col->get_data_at(i);
            StringRef url_val(const_cast<char*>(source.data), source.size);

            StringRef parse_res;
            bool success = false;
            if (has_key) {
                success = UrlParser::parse_url_key(url_val, url_part, url_key, &parse_res);
            } else {
                success = UrlParser::parse_url(url_val, url_part, &parse_res);
            }

            if (!success) {
                // url is malformed, or url_part is invalid.
                if (url_part == UrlParser::INVALID) {
                    return Status::RuntimeError("Invalid URL part: {}\n{}",
                                                std::string(part.data, part.size),
                                                "(Valid URL parts are 'PROTOCOL', 'HOST', "
                                                "'PATH', 'REF', 'AUTHORITY', "
                                                "'FILE', 'USERINFO', 'PORT' and 'QUERY')");
                } else {
                    StringOP::push_null_string(i, res_chars, res_offsets, null_map_data);
                    continue;
                }
            }

            StringOP::push_value_string(std::string_view(parse_res.data, parse_res.size), i,
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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto res_column = ColumnString::create();
        ColumnPtr argument_column = block.get_by_position(arguments[0]).column;

        auto result_column = assert_cast<ColumnString*>(res_column.get());

        Impl::execute(context, result_column, argument_column, input_rows_count);

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

namespace MoneyFormat {

template <typename T, size_t N>
StringRef do_money_format(FunctionContext* context, const T int_value,
                          const int32_t frac_value = 0) {
    char local[N];
    char* p = SimpleItoaWithCommas(int_value, local, sizeof(local));
    int32_t string_val_len = local + sizeof(local) - p + 3;
    StringRef result = context->create_temp_string_val(string_val_len);
    char* result_data = const_cast<char*>(result.data);
    memcpy(result_data, p, string_val_len - 3);
    *(result_data + string_val_len - 3) = '.';
    *(result_data + string_val_len - 2) = '0' + (frac_value / 10);
    *(result_data + string_val_len - 1) = '0' + (frac_value % 10);
    return result;
};

// Note string value must be valid decimal string which contains two digits after the decimal point
static StringRef do_money_format(FunctionContext* context, const string& value) {
    bool is_positive = (value[0] != '-');
    int32_t result_len = value.size() + (value.size() - (is_positive ? 4 : 5)) / 3;
    StringRef result = context->create_temp_string_val(result_len);
    char* result_data = const_cast<char*>(result.data);
    if (!is_positive) {
        *result_data = '-';
    }
    for (int i = value.size() - 4, j = result_len - 4; i >= 0; i = i - 3, j = j - 4) {
        *(result_data + j) = *(value.data() + i);
        if (i - 1 < 0) {
            break;
        }
        *(result_data + j - 1) = *(value.data() + i - 1);
        if (i - 2 < 0) {
            break;
        }
        *(result_data + j - 2) = *(value.data() + i - 2);
        if (j - 3 > 1 || (j - 3 == 1 && is_positive)) {
            *(result_data + j - 3) = ',';
        }
    }
    memcpy(result_data + result_len - 3, value.data() + value.size() - 3, 3);
    return result;
};

} // namespace MoneyFormat
struct MoneyFormatDoubleImpl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeFloat64>()}; }

    static void execute(FunctionContext* context, ColumnString* result_column,
                        const ColumnPtr col_ptr, size_t input_rows_count) {
        const auto* data_column = assert_cast<const ColumnVector<Float64>*>(col_ptr.get());
        for (size_t i = 0; i < input_rows_count; i++) {
            double value =
                    MathFunctions::my_double_round(data_column->get_element(i), 2, false, false);
            StringRef str = MoneyFormat::do_money_format(context, fmt::format("{:.2f}", value));
            result_column->insert_data(str.data, str.size);
        }
    }
};

struct MoneyFormatInt64Impl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeInt64>()}; }

    static void execute(FunctionContext* context, ColumnString* result_column,
                        const ColumnPtr col_ptr, size_t input_rows_count) {
        const auto* data_column = assert_cast<const ColumnVector<Int64>*>(col_ptr.get());
        for (size_t i = 0; i < input_rows_count; i++) {
            Int64 value = data_column->get_element(i);
            StringRef str = MoneyFormat::do_money_format<Int64, 26>(context, value);
            result_column->insert_data(str.data, str.size);
        }
    }
};

struct MoneyFormatInt128Impl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeInt128>()}; }

    static void execute(FunctionContext* context, ColumnString* result_column,
                        const ColumnPtr col_ptr, size_t input_rows_count) {
        const auto* data_column = assert_cast<const ColumnVector<Int128>*>(col_ptr.get());
        for (size_t i = 0; i < input_rows_count; i++) {
            Int128 value = data_column->get_element(i);
            StringRef str = MoneyFormat::do_money_format<Int128, 52>(context, value);
            result_column->insert_data(str.data, str.size);
        }
    }
};

struct MoneyFormatDecimalImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeDecimal<Decimal128V2>>(27, 9)};
    }

    static void execute(FunctionContext* context, ColumnString* result_column, ColumnPtr col_ptr,
                        size_t input_rows_count) {
        if (auto* decimalv2_column = check_and_get_column<ColumnDecimal<Decimal128V2>>(*col_ptr)) {
            for (size_t i = 0; i < input_rows_count; i++) {
                DecimalV2Value value = DecimalV2Value(decimalv2_column->get_element(i));

                DecimalV2Value rounded(0);
                value.round(&rounded, 2, HALF_UP);

                StringRef str = MoneyFormat::do_money_format<int64_t, 26>(
                        context, rounded.int_value(), abs(rounded.frac_value() / 10000000));

                result_column->insert_data(str.data, str.size);
            }
        } else if (auto* decimal32_column =
                           check_and_get_column<ColumnDecimal<Decimal32>>(*col_ptr)) {
            const UInt32 scale = decimal32_column->get_scale();
            const auto multiplier =
                    scale > 2 ? common::exp10_i32(scale - 2) : common::exp10_i32(2 - scale);
            for (size_t i = 0; i < input_rows_count; i++) {
                Decimal32 frac_part = decimal32_column->get_fractional_part(i);
                if (scale > 2) {
                    int delta = ((frac_part % multiplier) << 1) > multiplier;
                    frac_part = frac_part / multiplier + delta;
                } else if (scale < 2) {
                    frac_part = frac_part * multiplier;
                }

                StringRef str = MoneyFormat::do_money_format<int64_t, 26>(
                        context, decimal32_column->get_whole_part(i), frac_part);

                result_column->insert_data(str.data, str.size);
            }
        } else if (auto* decimal64_column =
                           check_and_get_column<ColumnDecimal<Decimal64>>(*col_ptr)) {
            const UInt32 scale = decimal64_column->get_scale();
            const auto multiplier =
                    scale > 2 ? common::exp10_i32(scale - 2) : common::exp10_i32(2 - scale);
            for (size_t i = 0; i < input_rows_count; i++) {
                Decimal64 frac_part = decimal64_column->get_fractional_part(i);
                if (scale > 2) {
                    int delta = ((frac_part % multiplier) << 1) > multiplier;
                    frac_part = frac_part / multiplier + delta;
                } else if (scale < 2) {
                    frac_part = frac_part * multiplier;
                }

                StringRef str = MoneyFormat::do_money_format<int64_t, 26>(
                        context, decimal64_column->get_whole_part(i), frac_part);

                result_column->insert_data(str.data, str.size);
            }
        } else if (auto* decimal128_column =
                           check_and_get_column<ColumnDecimal<Decimal128V3>>(*col_ptr)) {
            const UInt32 scale = decimal128_column->get_scale();
            const auto multiplier =
                    scale > 2 ? common::exp10_i32(scale - 2) : common::exp10_i32(2 - scale);
            for (size_t i = 0; i < input_rows_count; i++) {
                Decimal128V3 frac_part = decimal128_column->get_fractional_part(i);
                if (scale > 2) {
                    int delta = ((frac_part % multiplier) << 1) > multiplier;
                    frac_part = frac_part / multiplier + delta;
                } else if (scale < 2) {
                    frac_part = frac_part * multiplier;
                }

                StringRef str = MoneyFormat::do_money_format<__int128, 53>(
                        context, decimal128_column->get_whole_part(i), frac_part);

                result_column->insert_data(str.data, str.size);
            }
        }
        // TODO: decimal256
        /* else if (auto* decimal256_column =
                           check_and_get_column<ColumnDecimal<Decimal256>>(*col_ptr)) {
            const UInt32 scale = decimal256_column->get_scale();
            const auto multiplier =
                    scale > 2 ? common::exp10_i32(scale - 2) : common::exp10_i32(2 - scale);
            for (size_t i = 0; i < input_rows_count; i++) {
                Decimal256 frac_part = decimal256_column->get_fractional_part(i);
                if (scale > 2) {
                    int delta = ((frac_part % multiplier) << 1) > multiplier;
                    frac_part = Decimal256(frac_part / multiplier + delta);
                } else if (scale < 2) {
                    frac_part = Decimal256(frac_part * multiplier);
                }

                StringRef str = MoneyFormat::do_money_format<int64_t, 26>(
                        context, decimal256_column->get_whole_part(i), frac_part);

                result_column->insert_data(str.data, str.size);
            }
        }*/
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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 3);
        bool col_const[3];
        ColumnPtr argument_columns[3];
        for (int i = 0; i < 3; ++i) {
            col_const[i] = is_column_const(*block.get_by_position(arguments[i]).column);
        }
        argument_columns[2] = col_const[2] ? static_cast<const ColumnConst&>(
                                                     *block.get_by_position(arguments[2]).column)
                                                     .convert_to_full_column()
                                           : block.get_by_position(arguments[2]).column;
        default_preprocess_parameter_columns(argument_columns, col_const, {0, 1}, block, arguments);

        auto col_left = assert_cast<const ColumnString*>(argument_columns[0].get());
        auto col_right = assert_cast<const ColumnString*>(argument_columns[1].get());
        auto col_pos = assert_cast<const ColumnVector<Int32>*>(argument_columns[2].get());

        ColumnInt32::MutablePtr col_res = ColumnInt32::create();
        auto& vec_res = col_res->get_data();
        vec_res.resize(block.rows());

        if (col_const[0] && col_const[1]) {
            scalar_search<true>(col_left->get_data_at(0), col_right, col_pos->get_data(), vec_res);
        } else if (col_const[0] && !col_const[1]) {
            scalar_search<false>(col_left->get_data_at(0), col_right, col_pos->get_data(), vec_res);
        } else if (!col_const[0] && col_const[1]) {
            vector_search<true>(col_left, col_right, col_pos->get_data(), vec_res);
        } else {
            vector_search<false>(col_left, col_right, col_pos->get_data(), vec_res);
        }
        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

private:
    template <bool Const>
    void scalar_search(const StringRef& ldata, const ColumnString* col_right,
                       const PaddedPODArray<Int32>& posdata, PaddedPODArray<Int32>& res) const {
        const ColumnString::Chars& rdata = col_right->get_chars();
        const ColumnString::Offsets& roffsets = col_right->get_offsets();

        auto size = posdata.size();
        res.resize(size);
        StringRef substr(ldata.data, ldata.size);
        std::shared_ptr<StringSearch> search_ptr(new StringSearch(&substr));

        for (int i = 0; i < size; ++i) {
            if constexpr (!Const) {
                const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
                int r_str_size = roffsets[i] - roffsets[i - 1];

                StringRef str(r_raw_str, r_str_size);
                res[i] = locate_pos(substr, str, search_ptr, posdata[i]);
            } else {
                res[i] = locate_pos(substr, col_right->get_data_at(0), search_ptr, posdata[i]);
            }
        }
    }

    template <bool Const>
    void vector_search(const ColumnString* col_left, const ColumnString* col_right,
                       const PaddedPODArray<Int32>& posdata, PaddedPODArray<Int32>& res) const {
        const ColumnString::Chars& rdata = col_right->get_chars();
        const ColumnString::Offsets& roffsets = col_right->get_offsets();

        const ColumnString::Chars& ldata = col_left->get_chars();
        const ColumnString::Offsets& loffsets = col_left->get_offsets();

        auto size = posdata.size();
        res.resize(size);
        std::shared_ptr<StringSearch> search_ptr;
        for (int i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1];

            StringRef substr(l_raw_str, l_str_size);
            if constexpr (!Const) {
                const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
                int r_str_size = roffsets[i] - roffsets[i - 1];

                StringRef str(r_raw_str, r_str_size);
                res[i] = locate_pos(substr, str, search_ptr, posdata[i]);
            } else {
                res[i] = locate_pos(substr, col_right->get_data_at(0), search_ptr, posdata[i]);
            }
        }
    }

    int locate_pos(StringRef substr, StringRef str, std::shared_ptr<StringSearch> search_ptr,
                   int start_pos) const {
        if (substr.size == 0) {
            if (start_pos <= 0) {
                return 0;
            } else if (start_pos == 1) {
                return 1;
            } else if (start_pos > str.size) {
                return 0;
            } else {
                return start_pos;
            }
        }
        // Hive returns 0 for *start_pos <= 0,
        // but throws an exception for *start_pos > str->len.
        // Since returning 0 seems to be Hive's error condition, return 0.
        std::vector<size_t> index;
        size_t char_len = simd::VStringFunctions::get_char_len(str.data, str.size, index);
        if (start_pos <= 0 || start_pos > str.size || start_pos > char_len) {
            return 0;
        }
        if (!search_ptr) {
            search_ptr.reset(new StringSearch(&substr));
        }
        // Input start_pos starts from 1.
        StringRef adjusted_str(str.data + index[start_pos - 1], str.size - index[start_pos - 1]);
        int32_t match_pos = search_ptr->search(&adjusted_str);
        if (match_pos >= 0) {
            // Hive returns the position in the original string starting from 1.
            size_t len = std::min(adjusted_str.size, (size_t)match_pos);
            return start_pos + simd::VStringFunctions::get_char_len(adjusted_str.data, len);
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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
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
    std::string replace(std::string str, std::string_view old_str, std::string_view new_str) const {
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
            string dst;
            dst.resize(src_len);
            simd::VStringFunctions::reverse(StringRef((uint8_t*)src_str, src_len),
                                            StringRef((uint8_t*)dst.data(), src_len));
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

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct SubReplaceImpl {
    static Status replace_execute(Block& block, const ColumnNumbers& arguments, size_t result,
                                  size_t input_rows_count) {
        auto res_column = ColumnString::create();
        auto result_column = assert_cast<ColumnString*>(res_column.get());
        auto args_null_map = ColumnUInt8::create(input_rows_count, 0);
        ColumnPtr argument_columns[4];
        for (int i = 0; i < 4; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[i])) {
                // Danger: Here must dispose the null map data first! Because
                // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
                // of column nullable mem of null map
                VectorizedUtils::update_null_map(args_null_map->get_data(),
                                                 nullable->get_null_map_data());
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        auto data_column = assert_cast<const ColumnString*>(argument_columns[0].get());
        auto mask_column = assert_cast<const ColumnString*>(argument_columns[1].get());
        auto start_column = assert_cast<const ColumnVector<Int32>*>(argument_columns[2].get());
        auto length_column = assert_cast<const ColumnVector<Int32>*>(argument_columns[3].get());

        vector(data_column, mask_column, start_column->get_data(), length_column->get_data(),
               args_null_map->get_data(), result_column, input_rows_count);

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res_column), std::move(args_null_map));
        return Status::OK();
    }

private:
    static void vector(const ColumnString* data_column, const ColumnString* mask_column,
                       const PaddedPODArray<Int32>& start, const PaddedPODArray<Int32>& length,
                       NullMap& args_null_map, ColumnString* result_column,
                       size_t input_rows_count) {
        ColumnString::Chars& res_chars = result_column->get_chars();
        ColumnString::Offsets& res_offsets = result_column->get_offsets();
        for (size_t row = 0; row < input_rows_count; ++row) {
            StringRef origin_str = data_column->get_data_at(row);
            StringRef new_str = mask_column->get_data_at(row);
            size_t origin_str_len = origin_str.size;
            //input is null, start < 0, len < 0, str_size <= start. return NULL
            if (args_null_map[row] || start[row] < 0 || length[row] < 0 ||
                origin_str_len <= start[row]) {
                res_offsets.push_back(res_chars.size());
                args_null_map[row] = 1;
            } else {
                std::string_view replace_str = new_str.to_string_view();
                std::string result = origin_str.to_string();
                result.replace(start[row], length[row], replace_str);
                result_column->insert_data(result.data(), result.length());
            }
        }
    }
};

struct SubReplaceThreeImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        auto params = ColumnInt32::create(input_rows_count);
        auto& strlen_data = params->get_data();

        auto str_col =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*str_col)) {
            str_col = nullable->get_nested_column_ptr();
        }
        auto& str_offset = assert_cast<const ColumnString*>(str_col.get())->get_offsets();

        for (int i = 0; i < input_rows_count; ++i) {
            strlen_data[i] = str_offset[i] - str_offset[i - 1];
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
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        return SubReplaceImpl::replace_execute(block, arguments, result, input_rows_count);
    }
};

class FunctionConvertTo : public IFunction {
public:
    static constexpr auto name = "convert_to";

    static FunctionPtr create() { return std::make_shared<FunctionConvertTo>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return std::make_shared<DataTypeString>();
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope != FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        if (!context->is_col_constant(1)) {
            return Status::InvalidArgument(
                    "character argument to convert function must be constant.");
        }
        const auto& character_data = context->get_constant_col(1)->column_ptr->get_data_at(0);
        if (!iequal(character_data.to_string(), "gbk")) {
            return Status::RuntimeError(
                    "Illegal second argument column of function convert. now only support "
                    "convert to character set of gbk");
        }

        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr argument_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnString* str_col = static_cast<const ColumnString*>(argument_column.get());
        const auto& str_offset = str_col->get_offsets();
        const auto& str_chars = str_col->get_chars();
        auto col_res = ColumnString::create();
        auto& res_offset = col_res->get_offsets();
        auto& res_chars = col_res->get_chars();
        res_offset.resize(input_rows_count);
        // max pinyin size is 6, double of utf8 chinese word 3, add one char to set '~'
        res_chars.resize(str_chars.size() * 2 + input_rows_count);

        size_t in_len = 0, out_len = 0;
        for (int i = 0; i < input_rows_count; ++i) {
            in_len = str_offset[i] - str_offset[i - 1];
            const char* in = reinterpret_cast<const char*>(&str_chars[str_offset[i - 1]]);
            char* out = reinterpret_cast<char*>(&res_chars[res_offset[i - 1]]);
            _utf8_to_pinyin(in, in_len, out, &out_len);
            res_offset[i] = res_offset[i - 1] + out_len;
        }
        res_chars.resize(res_offset[input_rows_count - 1]);
        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

    void _utf8_to_pinyin(const char* in, size_t in_len, char* out, size_t* out_len) const {
        auto do_memcpy = [](char*& dest, const char*& from, size_t size) {
            memcpy_small_allow_read_write_overflow15(dest, from, size);
            dest += size;
            from += size;
        };
        auto from = in;
        auto dest = out;

        while (from - in < in_len) {
            auto length = get_utf8_byte_length(*from);
            if (length != 3) {
                do_memcpy(dest, from, length);
            } else {
                // convert utf8 to unicode code to get pinyin offset
                if (auto tmp = (((int)(*from & 0x0F)) << 12) | (((int)(*(from + 1) & 0x3F)) << 6) |
                               (*(from + 2) & 0x3F);
                    tmp >= START_UNICODE_OFFSET and tmp < END_UNICODE_OFFSET) {
                    const char* buf = nullptr;
                    if (tmp >= START_UNICODE_OFFSET && tmp < MID_UNICODE_OFFSET) {
                        buf = PINYIN_DICT1 + (tmp - START_UNICODE_OFFSET) * MAX_PINYIN_LEN;
                    } else if (tmp >= MID_UNICODE_OFFSET && tmp < END_UNICODE_OFFSET) {
                        buf = PINYIN_DICT2 + (tmp - MID_UNICODE_OFFSET) * MAX_PINYIN_LEN;
                    }

                    auto end = strchr(buf, ' ');
                    auto len = end != nullptr ? end - buf : MAX_PINYIN_LEN;
                    // set first char '~' just make sure all english word lower than chinese word
                    *dest = 126;
                    memcpy(dest + 1, buf, len);
                    dest += (len + 1);
                    from += 3;
                } else {
                    do_memcpy(dest, from, 3);
                }
            }
        }

        *out_len = dest - out;
    }
};

// refer to https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_char
//      UTF8
// 多	0xe5, 0xa4, 0x9a	0xb6, 0xe0
// 睿	0xe7, 0x9d, 0xbf	0xee, 0xa3
// 丝	0xe4, 0xb8, 0x9d	0xcb, 0xbf 14989469
// MySQL behaviour:
// mysql> select char(0xe4, 0xb8, 0x9d using utf8);
// +-----------------------------------+
// | char(0xe4, 0xb8, 0x9d using utf8) |
// +-----------------------------------+
// | 丝                                |
// +-----------------------------------+
// 1 row in set, 1 warning (0.00 sec)
// mysql> select char(14989469 using utf8);
// +---------------------------+
// | char(14989469 using utf8) |
// +---------------------------+
// | 丝                        |
// +---------------------------+
// 1 row in set, 1 warning (0.00 sec)
// mysql> select char(0xe5, 0xa4, 0x9a, 0xe7, 0x9d, 0xbf, 0xe4, 0xb8, 0x9d, 68, 111, 114, 105, 115 using utf8);
// +---------------------------------------------------------------------------------------------+
// | char(0xe5, 0xa4, 0x9a, 0xe7, 0x9d, 0xbf, 0xe4, 0xb8, 0x9d, 68, 111, 114, 105, 115 using utf8) |
// +---------------------------------------------------------------------------------------------+
// | 多睿丝Doris                                                                                 |
// +---------------------------------------------------------------------------------------------+
// mysql> select char(68, 111, 114, 0, 105, null, 115 using utf8);
// +--------------------------------------------------+
// | char(68, 111, 114, 0, 105, null, 115 using utf8) |
// +--------------------------------------------------+
// | Dor is                                           |
// +--------------------------------------------------+

// return null:
// mysql>  select char(255 using utf8);
// +----------------------+
// | char(255 using utf8) |
// +----------------------+
// | NULL                 |
// +----------------------+
// 1 row in set, 2 warnings (0.00 sec)
//
// mysql> show warnings;
// +---------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
// | Level   | Code | Message                                                                                                                                                                     |
// +---------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
// | Warning | 3719 | 'utf8' is currently an alias for the character set UTF8MB3, but will be an alias for UTF8MB4 in a future release. Please consider using UTF8MB4 in order to be unambiguous. |
// | Warning | 1300 | Invalid utf8mb3 character string: 'FF'                                                                                                                                      |
// +---------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
// 2 rows in set (0.01 sec)

// max int value:
// mysql> select char(18446744073709551615);
// +--------------------------------------------------------+
// | char(18446744073709551615)                             |
// +--------------------------------------------------------+
// | 0xFFFFFFFF                                             |
// +--------------------------------------------------------+
// 1 row in set (0.00 sec)
//
// mysql> select char(18446744073709551616);
// +--------------------------------------------------------+
// | char(18446744073709551616)                             |
// +--------------------------------------------------------+
// | 0xFFFFFFFF                                             |
// +--------------------------------------------------------+
// 1 row in set, 1 warning (0.00 sec)
//
// mysql> show warnings;
// +---------+------+-----------------------------------------------------------+
// | Level   | Code | Message                                                   |
// +---------+------+-----------------------------------------------------------+
// | Warning | 1292 | Truncated incorrect DECIMAL value: '18446744073709551616' |
// +---------+------+-----------------------------------------------------------+
// 1 row in set (0.00 sec)

// table columns:
// mysql> select * from t;
// +------+------+------+
// | f1   | f2   | f3   |
// +------+------+------+
// |  228 |  184 |  157 |
// |  228 |  184 |    0 |
// |  228 |  184 |   99 |
// |   99 |  228 |  184 |
// +------+------+------+
// 4 rows in set (0.00 sec)
//
// mysql> select char(f1, f2, f3 using utf8) from t;
// +-----------------------------+
// | char(f1, f2, f3 using utf8) |
// +-----------------------------+
// | 丝                          |
// |                             |
// |                             |
// | c                           |
// +-----------------------------+
// 4 rows in set, 4 warnings (0.00 sec)
//
// mysql> show warnings;
// +---------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
// | Level   | Code | Message                                                                                                                                                                     |
// +---------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
// | Warning | 3719 | 'utf8' is currently an alias for the character set UTF8MB3, but will be an alias for UTF8MB4 in a future release. Please consider using UTF8MB4 in order to be unambiguous. |
// | Warning | 1300 | Invalid utf8mb3 character string: 'E4B800'                                                                                                                                  |
// | Warning | 1300 | Invalid utf8mb3 character string: 'E4B863'                                                                                                                                  |
// | Warning | 1300 | Invalid utf8mb3 character string: 'E4B8'                                                                                                                                    |
// +---------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
class FunctionIntToChar : public IFunction {
public:
    static constexpr auto name = "char";
    static FunctionPtr create() { return std::make_shared<FunctionIntToChar>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 2);

        int argument_size = arguments.size();
        std::vector<ColumnPtr> str_columns(argument_size - 1);
        std::vector<const ColumnString::Offsets*> offsets_list(argument_size - 1);
        std::vector<const ColumnString::Chars*> chars_list(argument_size - 1);

        // convert each argument columns to column string and then concat the string columns
        for (size_t i = 1; i < argument_size; ++i) {
            if (auto const_column = check_and_get_column<const ColumnConst>(
                        *block.get_by_position(arguments[i]).column)) {
                // ignore null
                if (const_column->only_null()) {
                    str_columns[i - 1] = nullptr;
                } else {
                    auto str_column = ColumnString::create();
                    auto& chars = str_column->get_chars();
                    auto& offsets = str_column->get_offsets();
                    offsets.resize(1);
                    const ColumnVector<Int32>* int_column;
                    if (auto* nullable = check_and_get_column<const ColumnNullable>(
                                const_column->get_data_column())) {
                        int_column = assert_cast<const ColumnVector<Int32>*>(
                                nullable->get_nested_column_ptr().get());
                    } else {
                        int_column = assert_cast<const ColumnVector<Int32>*>(
                                &const_column->get_data_column());
                    }
                    int int_val = int_column->get_int(0);
                    integer_to_char_(0, &int_val, chars, offsets);
                    str_columns[i - 1] =
                            ColumnConst::create(std::move(str_column), input_rows_count);
                }
                offsets_list[i - 1] = nullptr;
                chars_list[i - 1] = nullptr;
            } else {
                auto str_column = ColumnString::create();
                auto& chars = str_column->get_chars();
                auto& offsets = str_column->get_offsets();
                // data.resize(input_rows_count);
                offsets.resize(input_rows_count);

                if (auto nullable = check_and_get_column<const ColumnNullable>(
                            *block.get_by_position(arguments[i]).column)) {
                    const auto* int_data = assert_cast<const ColumnVector<Int32>*>(
                                                   nullable->get_nested_column_ptr().get())
                                                   ->get_data()
                                                   .data();
                    const auto* null_map_data = nullable->get_null_map_data().data();
                    for (size_t j = 0; j < input_rows_count; ++j) {
                        // ignore null
                        if (null_map_data[j]) {
                            offsets[j] = offsets[j - 1];
                        } else {
                            integer_to_char_(j, int_data + j, chars, offsets);
                        }
                    }
                } else {
                    const auto* int_data = assert_cast<const ColumnVector<Int32>*>(
                                                   block.get_by_position(arguments[i]).column.get())
                                                   ->get_data()
                                                   .data();
                    for (size_t j = 0; j < input_rows_count; ++j) {
                        integer_to_char_(j, int_data + j, chars, offsets);
                    }
                }
                offsets_list[i - 1] = &str_column->get_offsets();
                chars_list[i - 1] = &str_column->get_chars();
                str_columns[i - 1] = std::move(str_column);
            }
        }

        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res = ColumnString::create();
        auto& res_data = res->get_chars();
        auto& res_offset = res->get_offsets();

        size_t res_reserve_size = 0;
        for (size_t i = 0; i < argument_size - 1; ++i) {
            if (!str_columns[i]) {
                continue;
            }
            if (auto const_column = check_and_get_column<const ColumnConst>(*str_columns[i])) {
                auto str_column =
                        assert_cast<const ColumnString*>(&(const_column->get_data_column()));
                auto& offsets = str_column->get_offsets();
                res_reserve_size += (offsets[0] - offsets[-1]) * input_rows_count;
            } else {
                for (size_t j = 0; j < input_rows_count; ++j) {
                    size_t append = (*offsets_list[i])[j] - (*offsets_list[i])[j - 1];
                    // check whether the output might overflow(unlikely)
                    if (UNLIKELY(UINT_MAX - append < res_reserve_size)) {
                        return Status::BufferAllocFailed(
                                "function char output is too large to allocate");
                    }
                    res_reserve_size += append;
                }
            }
        }
        if ((UNLIKELY(UINT_MAX - input_rows_count < res_reserve_size))) {
            return Status::BufferAllocFailed("function char output is too large to allocate");
        }

        res_data.resize(res_reserve_size);
        res_offset.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            int current_length = 0;
            for (size_t j = 0; j < argument_size - 1; ++j) {
                if (!str_columns[j]) {
                    continue;
                }
                if (auto const_column = check_and_get_column<const ColumnConst>(*str_columns[j])) {
                    auto str_column =
                            assert_cast<const ColumnString*>(&(const_column->get_data_column()));
                    auto data_item = str_column->get_data_at(0);
                    memcpy_small_allow_read_write_overflow15(
                            &res_data[res_offset[i - 1]] + current_length, data_item.data,
                            data_item.size);
                    current_length += data_item.size;
                } else {
                    auto& current_offsets = *offsets_list[j];
                    auto& current_chars = *chars_list[j];

                    int size = current_offsets[i] - current_offsets[i - 1];
                    if (size > 0) {
                        memcpy_small_allow_read_write_overflow15(
                                &res_data[res_offset[i - 1]] + current_length,
                                &current_chars[current_offsets[i - 1]], size);
                        current_length += size;
                    }
                }
            }
            res_offset[i] = res_offset[i - 1] + current_length;
        }

        // validate utf8
        auto* null_map_data = null_map->get_data().data();
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (!validate_utf8((const char*)(&res_data[res_offset[i - 1]]),
                               res_offset[i] - res_offset[i - 1])) {
                null_map_data[i] = 1;
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }

private:
    void integer_to_char_(int line_num, const int* num, ColumnString::Chars& chars,
                          IColumn::Offsets& offsets) const {
        if (0 == *num) {
            chars.push_back(' ');
            offsets[line_num] = offsets[line_num - 1] + 1;
            return;
        }
        const char* bytes = (const char*)(num);
#if __BYTE_ORDER == __LITTLE_ENDIAN
        int k = 3;
        for (; k >= 0; --k) {
            if (bytes[k]) {
                break;
            }
        }
        offsets[line_num] = offsets[line_num - 1] + k + 1;
        for (; k >= 0; --k) {
            chars.push_back(bytes[k] ? bytes[k] : ' ');
        }
#else
        int k = 0;
        for (; k < 4; ++k) {
            if (bytes[k]) {
                break;
            }
        }
        offsets[line_num] = offsets[line_num - 1] + 4 - k;
        for (; k < 4; ++k) {
            chars.push_back(bytes[k] ? bytes[k] : ' ');
        }
#endif
    }
};
} // namespace doris::vectorized

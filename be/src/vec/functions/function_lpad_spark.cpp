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

#include "vec/functions/function_lpad_spark.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <vector>

#include "common/cast_set.h"
#include "util/simd/vstring_function.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_varbinary.h"
#include "vec/common/assert_cast.h"
#include "vec/utils/stringop_substring.h"
#include "vec/utils/template_helpers.hpp"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

static ColumnPtr default_string_pad_column() {
    auto default_pad = ColumnString::create();
    auto& default_pad_offsets = default_pad->get_offsets();
    auto& default_pad_chars = default_pad->get_chars();
    default_pad_offsets.resize(1);
    StringOP::push_value_string(" ", 0, default_pad_chars, default_pad_offsets);
    return default_pad;
}

static ColumnPtr default_varbinary_pad_column() {
    auto default_pad = ColumnVarbinary::create();
    default_pad->insert_data("\0", 1);
    return default_pad;
}

struct PreparedPadArguments {
    std::array<ColumnPtr, 3> columns;
    std::array<bool, 3> column_const {};
};

static PreparedPadArguments prepare_pad_arguments(const Block& block,
                                                  const ColumnNumbers& arguments,
                                                  ColumnPtr default_pad) {
    PreparedPadArguments prepared;
    const size_t n = arguments.size();
    DCHECK(n == 2 || n == 3);
    for (size_t i = 0; i < n; ++i) {
        std::tie(prepared.columns[i], prepared.column_const[i]) =
                unpack_if_const(block.get_by_position(arguments[i]).column);
    }
    if (n == 2) {
        prepared.columns[2] = std::move(default_pad);
        prepared.column_const[2] = true;
    }
    return prepared;
}

template <bool str_const, bool len_const, bool pad_const>
static void execute_lpad_spark_string_utf8_rows(
        const ColumnString::Offsets& strcol_offsets, const ColumnString::Chars& strcol_chars,
        const ColumnInt32::Container& col_len_data, const ColumnString::Offsets& padcol_offsets,
        const ColumnString::Chars& padcol_chars, ColumnString::Offsets& res_offsets,
        ColumnString::Chars& res_chars, size_t input_rows_count) {
    std::vector<size_t> pad_index;
    size_t const_pad_char_size = 0;
    if constexpr (pad_const) {
        const_pad_char_size = simd::VStringFunctions::get_char_len((const char*)padcol_chars.data(),
                                                                   padcol_offsets[0], pad_index);
    }

    fmt::memory_buffer buffer;
    buffer.resize(strcol_chars.size());
    size_t buffer_len = 0;

    for (size_t i = 0; i < input_rows_count; ++i) {
        if constexpr (!pad_const) {
            pad_index.clear();
        }
        const auto len = col_len_data[index_check_const<len_const>(i)];

        if (len <= 0) {
            res_offsets[i] = cast_set<uint32_t>(buffer_len);
            continue;
        }

        const auto str_idx = index_check_const<str_const>(i);
        const int str_len = strcol_offsets[str_idx] - strcol_offsets[str_idx - 1];
        const auto* str_data = &strcol_chars[strcol_offsets[str_idx - 1]];
        const auto pad_idx = index_check_const<pad_const>(i);
        const int pad_len = padcol_offsets[pad_idx] - padcol_offsets[pad_idx - 1];
        const auto* pad_data = &padcol_chars[padcol_offsets[pad_idx - 1]];

        auto [iterate_byte_len, iterate_char_len] =
                simd::VStringFunctions::iterate_utf8_with_limit_length(
                        (const char*)str_data, (const char*)str_data + str_len, len);

        if (iterate_char_len == len) {
            buffer.resize(buffer_len + iterate_byte_len);
            memcpy(buffer.data() + buffer_len, str_data, iterate_byte_len);
            buffer_len += iterate_byte_len;
            res_offsets[i] = cast_set<uint32_t>(buffer_len);
            continue;
        }

        size_t pad_char_size;
        if constexpr (!pad_const) {
            pad_char_size =
                    simd::VStringFunctions::get_char_len((const char*)pad_data, pad_len, pad_index);
        } else {
            pad_char_size = const_pad_char_size;
        }

        if (pad_char_size == 0) {
            buffer.resize(buffer_len + str_len);
            memcpy(buffer.data() + buffer_len, str_data, str_len);
            buffer_len += str_len;
            res_offsets[i] = cast_set<uint32_t>(buffer_len);
            continue;
        }

        const size_t str_char_size = iterate_char_len;
        const size_t pad_times = (len - str_char_size) / pad_char_size;
        const size_t pad_remainder_len = pad_index[(len - str_char_size) % pad_char_size];
        const size_t new_capacity = str_len + size_t(pad_times + 1) * pad_len;
        ColumnString::check_chars_length(buffer_len + new_capacity, i);
        buffer.resize(buffer_len + new_capacity);

        StringOP::fast_repeat((uint8_t*)buffer.data() + buffer_len, pad_data, pad_len,
                              cast_set<int32_t>(pad_times));
        buffer_len += pad_times * pad_len;

        memcpy(buffer.data() + buffer_len, pad_data, pad_remainder_len);
        buffer_len += pad_remainder_len;

        memcpy(buffer.data() + buffer_len, str_data, str_len);
        buffer_len += str_len;
        res_offsets[i] = cast_set<uint32_t>(buffer_len);
    }
    res_chars.insert(buffer.data(), buffer.data() + buffer_len);
}

static Status execute_lpad_spark_string_core(Block& block, uint32_t result, size_t input_rows_count,
                                             const PreparedPadArguments& prepared) {
    auto null_map = ColumnUInt8::create(input_rows_count, 0);
    auto res = ColumnString::create();
    auto& res_offsets = res->get_offsets();
    auto& res_chars = res->get_chars();
    res_offsets.resize(input_rows_count);

    const auto* strcol = assert_cast<const ColumnString*>(prepared.columns[0].get());
    const auto& strcol_offsets = strcol->get_offsets();
    const auto& strcol_chars = strcol->get_chars();

    const auto* col_len = assert_cast<const ColumnInt32*>(prepared.columns[1].get());
    const auto& col_len_data = col_len->get_data();

    const auto* padcol = assert_cast<const ColumnString*>(prepared.columns[2].get());
    const auto& padcol_offsets = padcol->get_offsets();
    const auto& padcol_chars = padcol->get_chars();

    std::visit(
            [&](auto str_tag, auto len_tag, auto pad_tag) {
                execute_lpad_spark_string_utf8_rows<decltype(str_tag)::value,
                                                    decltype(len_tag)::value,
                                                    decltype(pad_tag)::value>(
                        strcol_offsets, strcol_chars, col_len_data, padcol_offsets, padcol_chars,
                        res_offsets, res_chars, input_rows_count);
            },
            vectorized::make_bool_variant(prepared.column_const[0]),
            vectorized::make_bool_variant(prepared.column_const[1]),
            vectorized::make_bool_variant(prepared.column_const[2]));

    block.get_by_position(result).column =
            ColumnNullable::create(std::move(res), std::move(null_map));
    return Status::OK();
}

template <bool bin_const, bool len_const, bool pad_const>
static void execute_lpad_spark_varbinary_rows(const ColumnVarbinary* bincol,
                                              const ColumnInt32::Container& col_len_data,
                                              const ColumnVarbinary* padcol, ColumnVarbinary* res,
                                              size_t input_rows_count) {
    std::vector<char> row_scratch;
    for (size_t i = 0; i < input_rows_count; ++i) {
        StringView bytes = bincol->get_data()[index_check_const<bin_const>(i)];
        const int len = col_len_data[index_check_const<len_const>(i)];
        StringView pad = padcol->get_data()[index_check_const<pad_const>(i)];

        if (len <= 0) {
            res->insert_default();
            continue;
        }
        const int blen = static_cast<int>(bytes.size());
        const int plen = static_cast<int>(pad.size());
        if (plen == 0) {
            const int out_len = std::min(blen, len);
            res->insert_data(bytes.data(), static_cast<size_t>(out_len));
            continue;
        }
        row_scratch.resize(static_cast<size_t>(len));
        char* p = row_scratch.data();
        const int min_len = std::min(len, blen);
        memcpy(p + static_cast<size_t>(len - min_len), bytes.data(), static_cast<size_t>(min_len));
        if (blen < len) {
            const int left_span = len - blen;
            for (int pos = 0; pos < left_span; pos += plen) {
                const int chunk = std::min(plen, left_span - pos);
                memcpy(p + static_cast<size_t>(pos), pad.data(), static_cast<size_t>(chunk));
            }
        }
        res->insert_data(p, static_cast<size_t>(len));
    }
}

static Status execute_lpad_spark_varbinary_core(Block& block, uint32_t result,
                                                size_t input_rows_count,
                                                const PreparedPadArguments& prepared) {
    auto null_map = ColumnUInt8::create(input_rows_count, 0);
    MutableColumnPtr res_col = ColumnVarbinary::create();
    auto* res = assert_cast<ColumnVarbinary*>(res_col.get());

    const auto* bincol = assert_cast<const ColumnVarbinary*>(prepared.columns[0].get());
    const auto* col_len = assert_cast<const ColumnInt32*>(prepared.columns[1].get());
    const auto& col_len_data = col_len->get_data();
    const auto* padcol = assert_cast<const ColumnVarbinary*>(prepared.columns[2].get());

    std::visit(
            [&](auto bin_c, auto len_c, auto pad_c) {
                execute_lpad_spark_varbinary_rows<decltype(bin_c)::value, decltype(len_c)::value,
                                                  decltype(pad_c)::value>(
                        bincol, col_len_data, padcol, res, input_rows_count);
            },
            vectorized::make_bool_variant(prepared.column_const[0]),
            vectorized::make_bool_variant(prepared.column_const[1]),
            vectorized::make_bool_variant(prepared.column_const[2]));

    block.get_by_position(result).column =
            ColumnNullable::create(std::move(res_col), std::move(null_map));
    return Status::OK();
}

static Status execute_lpad_spark_string_block(Block& block, const ColumnNumbers& arguments,
                                              uint32_t result, size_t input_rows_count) {
    auto prepared = prepare_pad_arguments(block, arguments, default_string_pad_column());
    return execute_lpad_spark_string_core(block, result, input_rows_count, prepared);
}

static Status execute_lpad_spark_varbinary_block(Block& block, const ColumnNumbers& arguments,
                                                 uint32_t result, size_t input_rows_count) {
    auto prepared = prepare_pad_arguments(block, arguments, default_varbinary_pad_column());
    return execute_lpad_spark_varbinary_core(block, result, input_rows_count, prepared);
}

template <int NumArgs>
Status FunctionLpadSparkString<NumArgs>::execute_impl(FunctionContext* /*context*/, Block& block,
                                                      const ColumnNumbers& arguments,
                                                      uint32_t result,
                                                      size_t input_rows_count) const {
    return execute_lpad_spark_string_block(block, arguments, result, input_rows_count);
}

template <int NumArgs>
Status FunctionLpadSparkVarbinary<NumArgs>::execute_impl(FunctionContext* /*context*/, Block& block,
                                                         const ColumnNumbers& arguments,
                                                         uint32_t result,
                                                         size_t input_rows_count) const {
    return execute_lpad_spark_varbinary_block(block, arguments, result, input_rows_count);
}

template Status FunctionLpadSparkString<2>::execute_impl(FunctionContext*, Block&,
                                                         const ColumnNumbers&, uint32_t,
                                                         size_t) const;
template Status FunctionLpadSparkString<3>::execute_impl(FunctionContext*, Block&,
                                                         const ColumnNumbers&, uint32_t,
                                                         size_t) const;
template Status FunctionLpadSparkVarbinary<2>::execute_impl(FunctionContext*, Block&,
                                                            const ColumnNumbers&, uint32_t,
                                                            size_t) const;
template Status FunctionLpadSparkVarbinary<3>::execute_impl(FunctionContext*, Block&,
                                                            const ColumnNumbers&, uint32_t,
                                                            size_t) const;

#include "common/compile_check_end.h"
} // namespace doris::vectorized

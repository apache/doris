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

#include <sys/types.h>

#include <algorithm>
#include <array>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/locale.hpp>
#include <climits>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

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
#include <string_view>

#include "udf/udf.h"
#include "util/simd/vstring_function.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
struct StringOP {
    static void push_empty_string(size_t index, ColumnString::Chars& chars,
                                  ColumnString::Offsets& offsets) {
        offsets[index] = (ColumnString::Offset)chars.size();
    }

    static void push_null_string(size_t index, ColumnString::Chars& chars,
                                 ColumnString::Offsets& offsets, NullMap& null_map) {
        null_map[index] = 1;
        push_empty_string(index, chars, offsets);
    }

    static void push_value_string(const std::string_view& string_value, size_t index,
                                  ColumnString::Chars& chars, ColumnString::Offsets& offsets) {
        ColumnString::check_chars_length(chars.size() + string_value.size(), offsets.size());

        chars.insert(string_value.data(), string_value.data() + string_value.size());
        offsets[index] = (ColumnString::Offset)chars.size();
    }

    static void push_value_string_reserved_and_allow_overflow(const std::string_view& string_value,
                                                              size_t index,
                                                              ColumnString::Chars& chars,
                                                              ColumnString::Offsets& offsets) {
        chars.insert_assume_reserved_and_allow_overflow(string_value.data(),
                                                        string_value.data() + string_value.size());
        offsets[index] = (ColumnString::Offset)chars.size();
    }

    static void fast_repeat(uint8_t* dst, const uint8_t* src, size_t src_size,
                            int32_t repeat_times) {
        if (UNLIKELY(repeat_times <= 0)) {
            return;
        }
        uint8_t* dst_begin = dst;
        uint8_t* dst_curr = dst;
        int32_t k = 0;
        int32_t is_odd = repeat_times & 1;
        repeat_times >>= 1;

        memcpy(dst_curr, src, src_size);
        dst_curr += src_size;
        for (; repeat_times > 0; k += 1, is_odd = repeat_times & 1, repeat_times >>= 1) {
            int64_t len = src_size * (1 << k);
            memcpy(dst_curr, dst_begin, len);
            dst_curr += len;
            if (is_odd) {
                memcpy(dst_curr, dst_begin, len);
                dst_curr += len;
            }
        }
    }
};

struct SubstringUtil {
    static constexpr auto name = "substring";

    static void substring_execute(Block& block, const ColumnNumbers& arguments, uint32_t result,
                                  size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 3);
        auto res = ColumnString::create();

        bool col_const[3];
        ColumnPtr argument_columns[3];
        for (int i = 0; i < 3; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }

        const auto* specific_str_column =
                assert_cast<const ColumnString*>(argument_columns[0].get());
        const auto* specific_start_column =
                assert_cast<const ColumnInt32*>(argument_columns[1].get());
        const auto* specific_len_column =
                assert_cast<const ColumnInt32*>(argument_columns[2].get());

        bool is_ascii = specific_str_column->is_ascii();

        std::visit(
                [&](auto is_ascii, auto str_const, auto start_const, auto len_const) {
                    vectors<is_ascii, str_const, start_const, len_const>(
                            specific_str_column->get_chars(), specific_str_column->get_offsets(),
                            specific_start_column->get_data(), specific_len_column->get_data(),
                            res->get_chars(), res->get_offsets(), input_rows_count);
                },
                vectorized::make_bool_variant(is_ascii),
                vectorized::make_bool_variant(col_const[0]),
                vectorized::make_bool_variant(col_const[1]),
                vectorized::make_bool_variant(col_const[2]));
        block.get_by_position(result).column = std::move(res);
    }

private:
    template <bool is_ascii, bool str_const, bool start_const, bool len_const>
    static void vectors(const ColumnString::Chars& chars, const ColumnString::Offsets& offsets,
                        const PaddedPODArray<Int32>& start, const PaddedPODArray<Int32>& len,
                        ColumnString::Chars& res_chars, ColumnString::Offsets& res_offsets,
                        size_t size) {
        res_offsets.resize(size);

        if constexpr (start_const && len_const) {
            if (start[0] == 0 || len[0] <= 0) {
                for (size_t i = 0; i < size; ++i) {
                    StringOP::push_empty_string(i, res_chars, res_offsets);
                }
                return;
            }
        }

        if constexpr (str_const) {
            res_chars.reserve(size * chars.size());
        } else {
            res_chars.reserve(chars.size());
        }

        if constexpr (is_ascii) {
            vectors_ascii<str_const, start_const, len_const>(chars, offsets, start, len, res_chars,
                                                             res_offsets, size);
        } else {
            vectors_utf8<str_const, start_const, len_const>(chars, offsets, start, len, res_chars,
                                                            res_offsets, size);
        }
    }

    template <bool str_const, bool start_const, bool len_const>
    NO_SANITIZE_UNDEFINED static void vectors_utf8(
            const ColumnString::Chars& chars, const ColumnString::Offsets& offsets,
            const PaddedPODArray<Int32>& start, const PaddedPODArray<Int32>& len,
            ColumnString::Chars& res_chars, ColumnString::Offsets& res_offsets, size_t size) {
        std::array<std::byte, 128 * 1024> buf;
        PMR::monotonic_buffer_resource pool {buf.data(), buf.size()};
        PMR::vector<size_t> index {&pool};

        for (size_t i = 0; i < size; ++i) {
            int str_size = offsets[index_check_const<str_const>(i)] -
                           offsets[index_check_const<str_const>(i) - 1];
            const char* str_data =
                    (char*)chars.data() + offsets[index_check_const<str_const>(i) - 1];
            int start_value = start[index_check_const<start_const>(i)];
            int len_value = len[index_check_const<len_const>(i)];
            // Unsigned numbers cannot be used here because start_value can be negative.
            int char_len = simd::VStringFunctions::get_char_len(str_data, str_size);
            // return empty string if start > src.length
            // Here, start_value is compared against the length of the character.
            if (start_value > char_len || str_size == 0 || start_value == 0 || len_value <= 0) {
                StringOP::push_empty_string(i, res_chars, res_offsets);
                continue;
            }

            size_t byte_pos = 0;
            index.clear();
            for (size_t j = 0, char_size = 0; j < str_size; j += char_size) {
                char_size = get_utf8_byte_length(str_data[j]);
                index.push_back(j);
                // index_size represents the number of characters from the beginning of the character to the current position.
                // So index.size() > start_value + len_value breaks because you don't need to get the characters after start + len characters.
                if (start_value > 0 && index.size() > start_value + len_value) {
                    break;
                }
            }

            int64_t fixed_pos = start_value;
            if (fixed_pos < -(int)index.size()) {
                StringOP::push_empty_string(i, res_chars, res_offsets);
                continue;
            }
            if (fixed_pos < 0) {
                fixed_pos = index.size() + fixed_pos + 1;
            }

            byte_pos = index[fixed_pos - 1];
            size_t fixed_len = str_size - byte_pos;
            if (fixed_pos + len_value <= index.size()) {
                fixed_len = index[fixed_pos + len_value - 1] - byte_pos;
            }

            if (byte_pos <= str_size && fixed_len > 0) {
                StringOP::push_value_string_reserved_and_allow_overflow(
                        {str_data + byte_pos, fixed_len}, i, res_chars, res_offsets);
            } else {
                StringOP::push_empty_string(i, res_chars, res_offsets);
            }
        }
    }

    template <bool str_const, bool start_const, bool len_const>
    static void vectors_ascii(const ColumnString::Chars& chars,
                              const ColumnString::Offsets& offsets,
                              const PaddedPODArray<Int32>& start, const PaddedPODArray<Int32>& len,
                              ColumnString::Chars& res_chars, ColumnString::Offsets& res_offsets,
                              size_t size) {
        for (size_t i = 0; i < size; ++i) {
            int str_size = offsets[index_check_const<str_const>(i)] -
                           offsets[index_check_const<str_const>(i) - 1];
            const char* str_data =
                    (char*)chars.data() + offsets[index_check_const<str_const>(i) - 1];
            int start_value = start[index_check_const<start_const>(i)];
            int len_value = len[index_check_const<len_const>(i)];

            if (start_value > str_size || start_value < -str_size || str_size == 0 ||
                len_value <= 0) {
                StringOP::push_empty_string(i, res_chars, res_offsets);
                continue;
            }
            int fixed_pos = start_value - 1;
            if (fixed_pos < 0) {
                fixed_pos = str_size + fixed_pos + 1;
            }
            size_t fixed_len = std::min(str_size - fixed_pos, len_value);
            StringOP::push_value_string_reserved_and_allow_overflow(
                    {str_data + fixed_pos, fixed_len}, i, res_chars, res_offsets);
        }
    }
};

#include "common/compile_check_end.h"

} // namespace doris::vectorized
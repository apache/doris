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

#include <crc32c/crc32c.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <unicode/normalizer2.h>
#include <unicode/stringpiece.h>
#include <unicode/unistr.h>

#include <algorithm>
#include <bit>
#include <boost/locale.hpp>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <format>
#include <iomanip>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/compiler_util.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/define_primitive_type.h"
#include "core/memcpy_small.h"
#include "core/pod_array.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "exec/common/hash_table/phmap_fwd_decl.h"
#include "exec/common/pinyin.h"
#include "exec/common/stringop_substring.h"
#include "exec/common/template_helpers.hpp"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/function_needs_to_handle_null.h"
#include "exprs/function_context.h"
#include "pugixml.hpp"
#include "util/hash_util.hpp"
#include "util/raw_value.h"
#include "util/simd/vstring_function.h"
#include "util/string_util.h"
#include "util/utf8_check.h"

#ifndef USE_LIBCPP
#include <memory_resource>
#define PMR std::pmr
#else
#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include <boost/container/pmr/vector.hpp>
#define PMR boost::container::pmr
#endif

#include "exprs/function/simple_function_factory.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"

class FunctionAutoPartitionName : public IFunction {
public:
    static constexpr auto name = "auto_partition_name";
    static FunctionPtr create() { return std::make_shared<FunctionAutoPartitionName>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }
    bool use_default_implementation_for_nulls() const override { return false; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        size_t argument_size = arguments.size();
        auto const_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        std::vector<const ColumnString::Chars*> chars_list(argument_size);
        std::vector<const ColumnString::Offsets*> offsets_list(argument_size);
        std::vector<bool> is_const_args(argument_size);
        std::vector<const ColumnUInt8::Container*> null_list(argument_size);
        std::vector<ColumnPtr> argument_null_columns(argument_size);

        std::vector<ColumnPtr> argument_columns(argument_size);
        for (int i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (const auto* nullable =
                        check_and_get_column<const ColumnNullable>(*argument_columns[i])) {
                null_list[i] = &nullable->get_null_map_data();
                argument_null_columns[i] = nullable->get_null_map_column_ptr();
                argument_columns[i] = nullable->get_nested_column_ptr();
            } else {
                null_list[i] = &const_null_map->get_data();
            }

            const auto& [col, is_const] =
                    unpack_if_const(block.get_by_position(arguments[i]).column);

            const auto* col_str = assert_cast<const ColumnString*>(argument_columns[i].get());
            chars_list[i] = &col_str->get_chars();
            offsets_list[i] = &col_str->get_offsets();
            is_const_args[i] = is_const;
        }

        auto res = ColumnString::create();
        auto& res_data = res->get_chars();
        auto& res_offset = res->get_offsets();
        res_offset.resize(input_rows_count);

        const char* partition_type = chars_list[0]->raw_data();
        // partition type is list|range
        if (std::strncmp(partition_type, "list", 4) == 0) {
            return _auto_partition_type_of_list(chars_list, offsets_list, is_const_args, null_list,
                                                res_data, res_offset, input_rows_count,
                                                argument_size, block, result, res);
        } else {
            return _auto_partition_type_of_range(chars_list, offsets_list, is_const_args, res_data,
                                                 res_offset, input_rows_count, argument_size, block,
                                                 result, res);
        }
        return Status::OK();
    }

private:
    std::u16string _string_to_u16string(const std::string& str) const {
        return boost::locale::conv::utf_to_utf<char16_t>(str);
    }

    std::string _string_to_unicode(const std::u16string& s) const {
        std::string res_s;
        res_s.reserve(s.size());
        if (s.length() > 0 && s[0] == '-') {
            res_s += '_';
        }
        for (int i = 0; i < s.length(); i++) {
            char16_t ch = s[i];
            if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
                res_s += ch;
            } else {
                int unicodeValue = _get_code_point_at(s, i);
                res_s += fmt::format("{:02x}", static_cast<uint32_t>(unicodeValue));
            }
        }
        return res_s;
    }

    int _get_code_point_at(const std::u16string& str, std::size_t index) const {
        char16_t first = str[index];
        // [0xD800,0xDBFF] is the scope of the first code unit
        if ((first >= 0xD800 && first <= 0xDBFF) && (index + 1 < str.size())) {
            char16_t second = str[index + 1];
            // [0xDC00,0xDFFF] is the scope of the second code unit
            if (second >= 0xDC00 && second <= 0xDFFF) {
                return ((first - 0xD800) << 10) + (second - 0xDC00) + 0x10000;
            }
        }

        return first;
    }
    Status _auto_partition_type_of_list(std::vector<const ColumnString::Chars*>& chars_list,
                                        std::vector<const ColumnString::Offsets*>& offsets_list,
                                        std::vector<bool>& is_const_args,
                                        const std::vector<const ColumnUInt8::Container*>& null_list,
                                        auto& res_data, auto& res_offset, size_t input_rows_count,
                                        size_t argument_size, Block& block, uint32_t result,
                                        auto& res) const {
        int curr_len = 0;
        for (int row = 0; row < input_rows_count; row++) {
            std::string res_p;
            res_p.reserve(argument_size * 5);
            res_p += 'p';
            for (int col = 1; col < argument_size; col++) {
                const auto& current_offsets = *offsets_list[col];
                const auto& current_chars = *chars_list[col];
                const auto& current_nullmap = *null_list[col];

                if (current_nullmap[row]) {
                    res_p += 'X';
                } else {
                    auto idx = index_check_const(row, is_const_args[col]);

                    int size = current_offsets[idx] - current_offsets[idx - 1];
                    const char* raw_chars =
                            reinterpret_cast<const char*>(&current_chars[current_offsets[idx - 1]]);
                    // convert string to u16string in order to convert to unicode strings
                    const std::string raw_str(raw_chars, size);
                    auto u16string = _string_to_u16string(raw_str);
                    res_p += _string_to_unicode(u16string) + std::to_string(u16string.size());
                }
            }

            // check the name of length
            int len = res_p.size();
            if (len > 50) {
                res_p = std::format("{}_{:08x}", res_p.substr(0, 50), to_hash_code(res_p));
                len = res_p.size();
            }
            curr_len += len;
            res_data.resize(curr_len);
            memcpy(&res_data[res_offset[row - 1]], res_p.c_str(), len);
            res_offset[row] = res_offset[row - 1] + len;
        }
        block.get_by_position(result).column = std::move(res);
        return Status::OK();
    }

    size_t _copy_date_str_of_len_to_res_data(auto& res_data, auto& res_offset,
                                             std::vector<std::string>& date_str, size_t row,
                                             size_t len) const {
        size_t curr_len = 1;
        for (int j = 0; j < len; j++) {
            memcpy(&res_data[res_offset[row - 1]] + curr_len, date_str[j].c_str(),
                   date_str[j].size());
            curr_len += date_str[j].size();
        }
        return curr_len;
    }

    Status _auto_partition_type_of_range(std::vector<const ColumnString::Chars*>& chars_list,
                                         std::vector<const ColumnString::Offsets*>& offsets_list,
                                         std::vector<bool>& is_const_args, auto& res_data,
                                         auto& res_offset, size_t input_rows_count,
                                         size_t argument_size, Block& block, uint32_t result,
                                         auto& res) const {
        const char* range_type = chars_list[1]->raw_data();

        res_data.resize(15 * input_rows_count);
        for (int i = 0; i < input_rows_count; i++) {
            const auto& current_offsets = *offsets_list[2];
            const auto& current_chars = *chars_list[2];

            auto idx = index_check_const(i, is_const_args[2]);
            int size = current_offsets[idx] - current_offsets[idx - 1];
            const char* tmp =
                    reinterpret_cast<const char*>(&current_chars[current_offsets[idx - 1]]);
            std::string to_split_s(tmp, size);

            // check the str if it is date|datetime
            RE2 date_regex(R"(^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$)");
            if (!RE2::FullMatch(to_split_s, date_regex)) {
                return Status::InvalidArgument("The range partition only support DATE|DATETIME");
            }

            // split date_str from (yyyy-mm-dd hh:mm:ss) to ([yyyy, mm, dd, hh, mm, ss])
            std::vector<std::string> date_str(6);
            date_str[0] = to_split_s.substr(0, 4);
            for (int ni = 5, j = 1; ni <= size; ni += 3, j++) {
                date_str[j] = to_split_s.substr(ni, 2);
            }
            int curr_len = 0;

            res_data[res_offset[i - 1]] = 'p';
            // raw => 2022-12-12 11:30:20
            // year => 2022 01 01 00 00 00
            // month => 2022 12 01 00 00 00
            // day => 2022 12 12 00 00 00
            // hour => 2022 12 12 11 00 00
            // minute => 2022 12  11 30 00
            // second => 2022 12 12 12 30 20

            if (!strncmp(range_type, "year", 4)) {
                curr_len += _copy_date_str_of_len_to_res_data(res_data, res_offset, date_str, i, 1);
                memcpy(&res_data[res_offset[i - 1]] + curr_len, "0101", 4);
                curr_len += 4;
            } else if (!strncmp(range_type, "month", 5)) {
                curr_len += _copy_date_str_of_len_to_res_data(res_data, res_offset, date_str, i, 2);
                memcpy(&res_data[res_offset[i - 1]] + curr_len, "01", 2);
                curr_len += 2;
            } else if (!strncmp(range_type, "day", 3)) {
                curr_len += _copy_date_str_of_len_to_res_data(res_data, res_offset, date_str, i, 3);
            } else if (!strncmp(range_type, "hour", 4)) {
                curr_len += _copy_date_str_of_len_to_res_data(res_data, res_offset, date_str, i, 4);
            } else if (!strncmp(range_type, "minute", 6)) {
                curr_len += _copy_date_str_of_len_to_res_data(res_data, res_offset, date_str, i, 5);
            } else if (!strncmp(range_type, "second", 6)) {
                curr_len += _copy_date_str_of_len_to_res_data(res_data, res_offset, date_str, i, 6);
            }

            // fill in zero
            int zero = 15 - curr_len;
            std::fill_n(&res_data[res_offset[i - 1]] + curr_len, zero, '0');
            curr_len += zero;
            res_offset[i] = res_offset[i - 1] + curr_len;
        }
        block.get_by_position(result).column = std::move(res);
        return Status::OK();
    }

    int32_t to_hash_code(const std::string& str) const {
        uint64_t h = 0;
        for (uint8_t c : str) {
            h = (h * 31U + c) & 0xFFFFFFFFU;
        }
        return static_cast<int32_t>(h);
    }
};

class FunctionRandomBytes : public IFunction {
public:
    static constexpr auto name = "random_bytes";
    static FunctionPtr create() { return std::make_shared<FunctionRandomBytes>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    bool is_variadic() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    bool use_default_implementation_for_constants() const final { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res = ColumnString::create();
        auto& res_offsets = res->get_offsets();
        auto& res_chars = res->get_chars();
        res_offsets.resize(input_rows_count);

        auto [arg_col, arg_const] = unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto* length_col = assert_cast<const ColumnInt32*>(arg_col.get());

        if (arg_const) {
            res_chars.reserve(input_rows_count * (length_col->get_element(0) + 2));
        }

        std::vector<uint8_t, Allocator_<uint8_t>> random_bytes;
        std::random_device rd;
        std::mt19937 gen(rd());

        std::uniform_int_distribution<unsigned short> distribution(0, 255);
        for (size_t i = 0; i < input_rows_count; ++i) {
            size_t index = index_check_const(i, arg_const);
            if (length_col->get_element(index) < 0) [[unlikely]] {
                return Status::InvalidArgument("argument {} of function {} at row {} was invalid.",
                                               length_col->get_element(index), name, index);
            }
            random_bytes.resize(length_col->get_element(index));

            for (auto& byte : random_bytes) {
                byte = distribution(gen) & 0xFF;
            }

            std::basic_ostringstream<char, std::char_traits<char>, Allocator_<char>> oss;
            for (const auto& byte : random_bytes) {
                oss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(byte);
            }

            StringOP::push_value_string("0x" + oss.str(), i, res_chars, res_offsets);
            random_bytes.clear();
        }

        block.get_by_position(result).column = std::move(res);

        return Status::OK();
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
                        uint32_t result, size_t input_rows_count) const override {
        ColumnPtr argument_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnString* str_col = static_cast<const ColumnString*>(argument_column.get());
        const auto& str_offset = str_col->get_offsets();
        const auto& str_chars = str_col->get_chars();
        auto col_res = ColumnString::create();
        auto& res_offset = col_res->get_offsets();
        auto& res_chars = col_res->get_chars();
        res_offset.resize(input_rows_count);
        // max pinyin size is 6 + 1 (first '~') for utf8 chinese word 3
        size_t pinyin_size = (str_chars.size() + 2) / 3 * 7;
        ColumnString::check_chars_length(pinyin_size, 0);
        res_chars.resize(pinyin_size);

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
                    // max len for pinyin is 6
                    int len = MAX_PINYIN_LEN;
                    if (end != nullptr && end - buf < MAX_PINYIN_LEN) {
                        len = end - buf;
                    }
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
// | 多睿丝 Doris                                                                                 |
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
                        uint32_t result, size_t input_rows_count) const override {
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
                    const ColumnInt32* int_column;
                    if (auto* nullable = check_and_get_column<const ColumnNullable>(
                                const_column->get_data_column())) {
                        int_column = assert_cast<const ColumnInt32*>(
                                nullable->get_nested_column_ptr().get());
                    } else {
                        int_column =
                                assert_cast<const ColumnInt32*>(&const_column->get_data_column());
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
                    const auto* int_data =
                            assert_cast<const ColumnInt32*>(nullable->get_nested_column_ptr().get())
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
                    const auto* int_data = assert_cast<const ColumnInt32*>(
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
        ColumnString::check_chars_length(res_reserve_size, 0);
        res_data.resize(res_reserve_size);
        res_offset.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            int current_length = 0;
            for (size_t j = 0; j < argument_size - 1; ++j) {
                if (!str_columns[j]) {
                    continue;
                }
                if (auto const_column = check_and_get_column<const ColumnConst>(*str_columns[j])) {
                    auto str_column = assert_cast<const ColumnString*, TypeCheckOnRelease::DISABLE>(
                            &(const_column->get_data_column()));
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
            chars.push_back('\0');
            offsets[line_num] = offsets[line_num - 1] + 1;
            return;
        }
        const char* bytes = (const char*)(num);
        if constexpr (std::endian::native == std::endian::little) {
            int k = 3;
            for (; k >= 0; --k) {
                if (bytes[k]) {
                    break;
                }
            }
            offsets[line_num] = offsets[line_num - 1] + k + 1;
            for (; k >= 0; --k) {
                chars.push_back(bytes[k] ? bytes[k] : '\0');
            }
        } else if constexpr (std::endian::native == std::endian::big) {
            int k = 0;
            for (; k < 4; ++k) {
                if (bytes[k]) {
                    break;
                }
            }
            offsets[line_num] = offsets[line_num - 1] + 4 - k;
            for (; k < 4; ++k) {
                chars.push_back(bytes[k] ? bytes[k] : '\0');
            }
        } else {
            static_assert(std::endian::native == std::endian::big ||
                                  std::endian::native == std::endian::little,
                          "Unsupported endianness");
        }
    }
};

class FunctionNgramSearch : public IFunction {
public:
    static constexpr auto name = "ngram_search";
    static FunctionPtr create() { return std::make_shared<FunctionNgramSearch>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeFloat64>();
    }

    // ngram_search(text,pattern,gram_num)
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        CHECK_EQ(arguments.size(), 3);
        auto col_res = ColumnFloat64::create();
        bool col_const[3];
        ColumnPtr argument_columns[3];
        for (int i = 0; i < 3; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }
        auto pattern = assert_cast<const ColumnString*>(argument_columns[1].get())->get_data_at(0);
        auto gram_num = assert_cast<const ColumnInt32*>(argument_columns[2].get())->get_element(0);
        const auto* text_col = assert_cast<const ColumnString*>(argument_columns[0].get());

        if (col_const[0]) {
            _execute_impl<true>(text_col, pattern, gram_num, *col_res, input_rows_count);
        } else {
            _execute_impl<false>(text_col, pattern, gram_num, *col_res, input_rows_count);
        }

        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

private:
    using NgramMap = phmap::flat_hash_map<uint32_t, uint8_t>;
    constexpr static auto not_found = 0b00;
    constexpr static auto found_in_pattern = 0b01;
    constexpr static auto found_in_text = 0b10;
    constexpr static auto found_in_pattern_and_text = 0b11;

    uint32_t sub_str_hash(const char* data, int32_t length) const {
        constexpr static uint32_t seed = 0;
        return crc32c::Extend(seed, (const uint8_t*)data, length);
    }

    template <bool column_const>
    void _execute_impl(const ColumnString* text_col, StringRef& pattern, int gram_num,
                       ColumnFloat64& res, size_t size) const {
        auto& res_data = res.get_data();
        res_data.resize_fill(size, 0);
        // If the length of the pattern is less than gram_num, return 0.
        if (pattern.size < gram_num) {
            return;
        }

        // Build a map by pattern string, which will be used repeatedly in the following loop.
        NgramMap pattern_map;
        int pattern_count = get_pattern_set(pattern_map, pattern, gram_num);
        // Each time a loop is executed, the map will be modified, so it needs to be restored afterward.
        std::vector<uint32_t> restore_map;

        for (int i = 0; i < size; i++) {
            auto text = text_col->get_data_at(index_check_const<column_const>(i));
            if (text.size < gram_num) {
                // If the length of the text is less than gram_num, return 0.
                continue;
            }
            restore_map.reserve(text.size);
            auto [text_count, intersection_count] =
                    get_text_set(text, gram_num, pattern_map, restore_map);

            // 2 * |Intersection| / (|text substr set| + |pattern substr set|)
            res_data[i] = 2.0 * intersection_count / (text_count + pattern_count);
        }
    }

    size_t get_pattern_set(NgramMap& pattern_map, StringRef& pattern, int gram_num) const {
        size_t pattern_count = 0;
        for (int i = 0; i + gram_num <= pattern.size; i++) {
            uint32_t cur_hash = sub_str_hash(pattern.data + i, gram_num);
            if (!pattern_map.contains(cur_hash)) {
                pattern_map[cur_hash] = found_in_pattern;
                pattern_count++;
            }
        }
        return pattern_count;
    }

    std::pair<size_t, size_t> get_text_set(StringRef& text, int gram_num, NgramMap& pattern_map,
                                           std::vector<uint32_t>& restore_map) const {
        restore_map.clear();
        //intersection_count indicates a substring both in pattern and text.
        size_t text_count = 0, intersection_count = 0;
        for (int i = 0; i + gram_num <= text.size; i++) {
            uint32_t cur_hash = sub_str_hash(text.data + i, gram_num);
            auto& val = pattern_map[cur_hash];
            if (val == not_found) {
                val ^= found_in_text;
                DCHECK(val == found_in_text);
                // only found in text
                text_count++;
                restore_map.push_back(cur_hash);
            } else if (val == found_in_pattern) {
                val ^= found_in_text;
                DCHECK(val == found_in_pattern_and_text);
                // found in text and pattern
                text_count++;
                intersection_count++;
                restore_map.push_back(cur_hash);
            }
        }
        // Restore the pattern_map.
        for (auto& restore_hash : restore_map) {
            pattern_map[restore_hash] ^= found_in_text;
        }

        return {text_count, intersection_count};
    }
};

class FunctionTranslate : public IFunction {
public:
    static constexpr auto name = "translate";
    using AsciiMap = std::array<UInt8, 128>;
    constexpr static UInt8 DELETE_CHAR = 255; // 255 means delete this char
    static FunctionPtr create() { return std::make_shared<FunctionTranslate>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    };

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeString>()};
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        CHECK_EQ(arguments.size(), 3);
        auto col_res = ColumnString::create();
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

        const auto* col_source = assert_cast<const ColumnString*>(argument_columns[0].get());
        const auto* col_from = assert_cast<const ColumnString*>(argument_columns[1].get());
        const auto* col_to = assert_cast<const ColumnString*>(argument_columns[2].get());

        bool is_ascii = col_source->is_ascii() && col_from->is_ascii() && col_to->is_ascii();
        auto impl_vectors = impl_vectors_utf8<false>;
        if (col_const[1] && col_const[2] && is_ascii) {
            impl_vectors = impl_vectors_ascii<true>;
        } else if (col_const[1] && col_const[2]) {
            impl_vectors = impl_vectors_utf8<true>;
        } else if (is_ascii) {
            impl_vectors = impl_vectors_ascii<false>;
        }
        impl_vectors(col_source, col_from, col_to, col_res.get());
        block.get_by_position(result).column = std::move(col_res);
        return Status::OK();
    }

private:
    template <bool IsConst>
    static void impl_vectors_ascii(const ColumnString* col_source, const ColumnString* col_from,
                                   const ColumnString* col_to, ColumnString* col_res) {
        auto& res_chars = col_res->get_chars();
        auto& res_offsets = col_res->get_offsets();
        res_chars.reserve(col_source->get_chars().size());
        res_offsets.reserve(col_source->get_offsets().size());
        DCHECK_EQ(col_res->size(), 0);
        AsciiMap map;
        if (IsConst) {
            const auto& from_str = col_from->get_data_at(0);
            const auto& to_str = col_to->get_data_at(0);
            if (!build_translate_map_ascii(map, from_str, to_str)) {
                // if the map is not need delete char, we can directly copy the source string,then use map to translate
                res_offsets.insert(col_source->get_offsets().begin(),
                                   col_source->get_offsets().end());
                res_chars.insert(col_source->get_chars().begin(), col_source->get_chars().end());
                for (int i = 0; i < res_chars.size(); ++i) {
                    res_chars[i] = map[res_chars[i]]; // translate the chars
                }
                return; // no need to translate
            }
        }

        auto res_size = 0;
        auto* begin_data = col_res->get_chars().data();
        for (size_t i = 0; i < col_source->size(); ++i) {
            const auto& source_str = col_source->get_data_at(i);
            if (!IsConst) {
                const auto& from_str = col_from->get_data_at(i);
                const auto& to_str = col_to->get_data_at(i);
                build_translate_map_ascii(map, from_str, to_str);
            }
            auto* dst_data = begin_data + res_size;
            res_size += translate_ascii(source_str, map, dst_data);

            res_offsets.push_back(res_size);
        }
        DCHECK_GE(res_chars.capacity(), res_size);
        res_chars.resize(res_size);
    }

    // return true if no need delete char
    bool static build_translate_map_ascii(AsciiMap& map, const StringRef& from_str,
                                          const StringRef& to_str) {
        for (size_t i = 0; i < map.size(); ++i) {
            map[i] = i; // initialize map to identity
        }
        std::array<UInt8, 128> set_map {0};
        const auto min_size = std::min(from_str.size, to_str.size);
        // all ascii characters are in the range [0, 127]
        for (size_t i = 0; i < min_size; ++i) {
            auto from_char = from_str.data[i];
            auto to_char = to_str.data[i];
            if (set_map[from_char] == 0) {
                set_map[from_char] = 1;
                map[from_char] = to_char;
            }
        }

        bool need_delete_char = false;

        for (size_t i = min_size; i < from_str.size; ++i) {
            auto from_char = from_str.data[i];
            if (set_map[from_char] == 0) {
                set_map[from_char] = 1;
                map[from_char] = DELETE_CHAR; // delete this char
                need_delete_char = true;
            }
        }
        return need_delete_char;
    }

    static size_t translate_ascii(const StringRef& source_str, AsciiMap& map, UInt8* dst_data) {
        auto* begin_data = dst_data;
        for (size_t i = 0; i < source_str.size; ++i) {
            auto c = source_str.data[i];
            if (map[c] == DELETE_CHAR) {
                continue; // delete this char
            }
            *dst_data++ = map[c];
        }
        return dst_data - begin_data;
    }

    template <bool IsConst>
    static void impl_vectors_utf8(const ColumnString* col_source, const ColumnString* col_from,
                                  const ColumnString* col_to, ColumnString* col_res) {
        col_res->get_chars().reserve(col_source->get_chars().size());
        col_res->get_offsets().reserve(col_source->get_offsets().size());
        std::unordered_map<std::string_view, std::string_view> translate_map;
        if (IsConst) {
            const auto& from_str = col_from->get_data_at(0);
            const auto& to_str = col_to->get_data_at(0);
            translate_map =
                    build_translate_map_utf8(from_str.to_string_view(), to_str.to_string_view());
        }
        for (size_t i = 0; i < col_source->size(); ++i) {
            const auto& source_str = col_source->get_data_at(i);
            if (!IsConst) {
                const auto& from_str = col_from->get_data_at(i);
                const auto& to_str = col_to->get_data_at(i);
                translate_map = build_translate_map_utf8(from_str.to_string_view(),
                                                         to_str.to_string_view());
            }
            auto translated_str = translate_utf8(source_str.to_string_view(), translate_map);
            col_res->insert_data(translated_str.data(), translated_str.size());
        }
    }

    static std::unordered_map<std::string_view, std::string_view> build_translate_map_utf8(
            const std::string_view& from_str, const std::string_view& to_str) {
        std::unordered_map<std::string_view, std::string_view> translate_map;
        for (size_t i = 0, from_char_size = 0, j = 0, to_char_size = 0; i < from_str.size();
             i += from_char_size, j += to_char_size) {
            from_char_size = get_utf8_byte_length(from_str[i]);
            to_char_size = j < to_str.size() ? get_utf8_byte_length(to_str[j]) : 0;
            auto from_char = from_str.substr(i, from_char_size);
            if (translate_map.find(from_char) == translate_map.end()) {
                translate_map[from_char] =
                        j < to_str.size() ? to_str.substr(j, to_char_size) : std::string_view();
            }
        }
        return translate_map;
    }

    static std::string translate_utf8(
            const std::string_view& source_str,
            std::unordered_map<std::string_view, std::string_view>& translate_map) {
        std::string result;
        result.reserve(source_str.size());
        for (size_t i = 0, char_size = 0; i < source_str.size(); i += char_size) {
            char_size = get_utf8_byte_length(source_str[i]);
            auto c = source_str.substr(i, char_size);
            if (translate_map.find(c) != translate_map.end()) {
                if (!translate_map[c].empty()) {
                    result.append(translate_map[c]);
                }
            } else {
                result.append(c);
            }
        }
        return result;
    }
};

/// xpath_string(xml, xpath) -> String
/// Returns the text content of the first node that matches the XPath expression.
/// Returns NULL if either xml or xpath is NULL.
/// Returns empty string if the XPath expression matches no nodes.
/// The text content includes the node and all its descendants.
/// Example:
///   xpath_string('<a><b>b1</b><b>b2</b></a>', '/a/b[1]') = 'b1'
///   xpath_string('<a><b>b1</b><b>b2</b></a>', '/a/b[2]') = 'b2'
///   xpath_string('<a><b>b1</b><b>b2</b></a>', '/a/c') = ''
///   xpath_string('invalid xml', '/a/b[1]') = NULL
///   xpath_string(NULL, '/a/b[1]') = NULL
///   xpath_string('<a><b>b1</b><b>b2</b></a>', NULL) = NULL
class FunctionXPathString : public IFunction {
public:
    static constexpr auto name = "xpath_string";
    static FunctionPtr create() { return std::make_shared<FunctionXPathString>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        CHECK_EQ(arguments.size(), 2);
        auto col_res = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
        const auto& [left_col, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_col, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        const auto& xml_col = *assert_cast<const ColumnString*>(left_col.get());
        const auto& xpath_col = *assert_cast<const ColumnString*>(right_col.get());

        Status status;
        if (left_const && right_const) {
            status = execute_vector<true, true>(input_rows_count, xml_col, xpath_col, *col_res);
        } else if (left_const) {
            status = execute_vector<true, false>(input_rows_count, xml_col, xpath_col, *col_res);
        } else if (right_const) {
            status = execute_vector<false, true>(input_rows_count, xml_col, xpath_col, *col_res);
        } else {
            status = execute_vector<false, false>(input_rows_count, xml_col, xpath_col, *col_res);
        }
        if (!status.ok()) {
            return status;
        }

        block.get_by_position(result).column = std::move(col_res);
        return Status::OK();
    }

private:
    static Status parse_xml(const StringRef& xml_str, pugi::xml_document& xml_doc) {
        pugi::xml_parse_result result = xml_doc.load_buffer(xml_str.data, xml_str.size);
        if (!result) {
            return Status::InvalidArgument("Function {} failed to parse XML string: {}", name,
                                           result.description());
        }
        return Status::OK();
    }

    static Status build_xpath_query(const StringRef& xpath_str, pugi::xpath_query& xpath_query) {
        // xpath_query will throws xpath_exception on compilation errors.
        try {
            // NOTE!!!: don't use to_string_view(), because xpath_str maybe not null-terminated
            xpath_query = pugi::xpath_query(xpath_str.to_string().c_str());
        } catch (const pugi::xpath_exception& e) {
            return Status::InvalidArgument("Function {} failed to build XPath query: {}", name,
                                           e.what());
        }
        return Status::OK();
    }

    template <bool left_const, bool right_const>
    static Status execute_vector(const size_t input_rows_count, const ColumnString& xml_col,
                                 const ColumnString& xpath_col, ColumnNullable& res_col) {
        pugi::xml_document xml_doc;
        pugi::xpath_query xpath_query;
        // first check right_const, because we want to check empty input first
        if constexpr (right_const) {
            auto xpath_str = xpath_col.get_data_at(0);
            if (xpath_str.empty()) {
                // should return null if xpath_str is empty
                res_col.insert_many_defaults(input_rows_count);
                return Status::OK();
            }
            RETURN_IF_ERROR(build_xpath_query(xpath_str, xpath_query));
        }
        if constexpr (left_const) {
            auto xml_str = xml_col.get_data_at(0);
            if (xml_str.empty()) {
                // should return null if xml_str is empty
                res_col.insert_many_defaults(input_rows_count);
                return Status::OK();
            }
            RETURN_IF_ERROR(parse_xml(xml_str, xml_doc));
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if constexpr (!right_const) {
                auto xpath_str = xpath_col.get_data_at(i);
                if (xpath_str.empty()) {
                    // should return null if xpath_str is empty
                    res_col.insert_default();
                    continue;
                }
                RETURN_IF_ERROR(build_xpath_query(xpath_str, xpath_query));
            }
            if constexpr (!left_const) {
                auto xml_str = xml_col.get_data_at(i);
                if (xml_str.empty()) {
                    // should return null if xml_str is empty
                    res_col.insert_default();
                    continue;
                }
                RETURN_IF_ERROR(parse_xml(xml_str, xml_doc));
            }
            std::string text;
            try {
                text = xpath_query.evaluate_string(xml_doc);
            } catch (const pugi::xpath_exception& e) {
                return Status::InvalidArgument("Function {} failed to query XPath string: {}", name,
                                               e.what());
            }
            res_col.insert_data(text.data(), text.size());
        }
        return Status::OK();
    }
};

class MakeSetImpl {
public:
    static constexpr auto name = "make_set";

    static size_t get_number_of_arguments() { return 0; }
    static bool is_variadic() { return true; }
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        if (arguments[0].get()->is_nullable()) {
            return make_nullable(std::make_shared<DataTypeString>());
        }
        return std::make_shared<DataTypeString>();
    }

    static bool is_return_nullable(bool has_nullable,
                                   const std::vector<ColumnWithConstAndNullMap>& cols_info) {
        return cols_info[0].null_map != nullptr;
    }

    static bool execute_const_null(ColumnString::MutablePtr& res_col,
                                   PaddedPODArray<UInt8>& res_null_map_data,
                                   size_t input_rows_count, size_t null_index) {
        if (null_index == 1) {
            res_col->insert_many_defaults(input_rows_count);
            res_null_map_data.assign(input_rows_count, (UInt8)1);
            return true;
        }
        return false;
    }

    static void execute(const std::vector<ColumnWithConstAndNullMap>& column_infos,
                        ColumnString::MutablePtr& res_col, PaddedPODArray<UInt8>& res_null_map_data,
                        size_t input_rows_count) {
        static constexpr char SEPARATOR = ',';
        const auto& bit_data =
                assert_cast<const ColumnInt64&>(*column_infos[0].nested_col).get_data();
        std::vector<const ColumnString*> str_cols(column_infos.size());
        for (size_t i = 1; i < column_infos.size(); ++i) {
            str_cols[i] = assert_cast<const ColumnString*>(column_infos[i].nested_col);
        }

        for (size_t row = 0; row < input_rows_count; ++row) {
            if (column_infos[0].is_null_at(row)) {
                res_col->insert_default();
                res_null_map_data[row] = 1;
                continue;
            }

            uint64_t bit = bit_data[column_infos[0].is_const ? 0 : row];
            uint64_t col_pos = __builtin_ffsll(bit);
            ColumnString::Chars data;
            while (col_pos != 0 && col_pos < column_infos.size() && bit != 0) {
                if (!column_infos[col_pos].is_null_at(row)) {
                    /* Here insert `str,` directly to support the case below:
                     * SELECT MAKE_SET(3, '', 'a');
                     * the exception result should be ',a'.
                     */
                    auto s_ref = str_cols[col_pos]->get_data_at(
                            column_infos[col_pos].is_const ? 0 : row);
                    data.insert(s_ref.data, s_ref.data + s_ref.size);
                    data.push_back(SEPARATOR);
                }
                bit &= ~(1ULL << (col_pos - 1));
                col_pos = __builtin_ffsll(bit);
            }
            // remove the last ','
            if (!data.empty()) {
                data.pop_back();
            }
            res_col->insert_data(reinterpret_cast<const char*>(data.data()), data.size());
        }
    }
};

class FunctionExportSet : public IFunction {
public:
    static constexpr auto name = "export_set";
    static FunctionPtr create() { return std::make_shared<FunctionExportSet>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res_col = ColumnString::create();

        const size_t arg_size = arguments.size();
        bool col_const[5];
        ColumnPtr arg_cols[5];
        bool all_const = true;
        for (int i = 1; i < arg_size; ++i) {
            col_const[i] = is_column_const(*block.get_by_position(arguments[i]).column);
            all_const = all_const && col_const[i];
        }
        std::tie(arg_cols[0], col_const[0]) =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        if (arg_size == 3) {
            default_preprocess_parameter_columns(arg_cols, col_const, {1, 2}, block, arguments);
        } else if (arg_size == 4) {
            default_preprocess_parameter_columns(arg_cols, col_const, {1, 2, 3}, block, arguments);
        } else if (arg_size == 5) {
            default_preprocess_parameter_columns(arg_cols, col_const, {1, 2, 3, 4}, block,
                                                 arguments);
        }

        const auto* bit_col = assert_cast<const ColumnInt128*>(arg_cols[0].get());
        const auto* on_col = assert_cast<const ColumnString*>(arg_cols[1].get());
        const auto* off_col = assert_cast<const ColumnString*>(arg_cols[2].get());
        const ColumnString* sep_col = nullptr;
        const ColumnInt32* num_bits_col = nullptr;
        if (arg_size > 3) {
            sep_col = assert_cast<const ColumnString*>(arg_cols[3].get());
            if (arg_size == 5) {
                num_bits_col = assert_cast<const ColumnInt32*>(arg_cols[4].get());
            }
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            uint64_t bit =
                    check_and_get_bit(bit_col->get_element(index_check_const(i, col_const[0])));

            size_t idx_for_args = all_const ? 0 : i;
            StringRef on = on_col->get_data_at(idx_for_args);
            StringRef off = off_col->get_data_at(idx_for_args);
            StringRef separator(",", 1);
            int8_t num_of_bits = 64;

            if (arg_size > 3) {
                separator = sep_col->get_data_at(idx_for_args);
                if (arg_size == 5) {
                    num_of_bits =
                            check_and_get_num_of_bits(num_bits_col->get_element(idx_for_args));
                }
            }

            execute_single(bit, on, off, separator, num_of_bits, *res_col);
        }
        block.replace_by_position(result, std::move(res_col));
        return Status::OK();
    }

private:
    /* The valid range of the input `bit` parameter should be [-2^63, 2^64 - 1]
     * If it exceeds this range, the MAX/MIN values of the signed 64-bit integer are used for calculation
     * This behavior is consistent with MySQL.
     */
    uint64_t check_and_get_bit(__int128 col_bit_val) const {
        if (col_bit_val > ULLONG_MAX) {
            return LLONG_MAX;
        } else if (col_bit_val < LLONG_MIN) {
            return LLONG_MIN;
        }
        return static_cast<uint64_t>(col_bit_val);
    }

    // If the input value is not in the range [0, 64], return default value 64
    int8_t check_and_get_num_of_bits(int32_t col_num_of_bits_val) const {
        if (col_num_of_bits_val >= 0 && col_num_of_bits_val <= 64) {
            return static_cast<int8_t>(col_num_of_bits_val);
        }
        return 64;
    }

    void execute_single(uint64_t bit, const StringRef& on, const StringRef& off,
                        const StringRef& separator, int8_t num_of_bits,
                        ColumnString& res_col) const {
        ColumnString::Chars data;
        data.reserve(std::max(on.size, off.size) * num_of_bits +
                     separator.size * (num_of_bits - 1));

        while (bit && num_of_bits) {
            if (bit & 1) {
                data.insert(on.data, on.data + on.size);
            } else {
                data.insert(off.data, off.data + off.size);
            }
            bit >>= 1;
            if (--num_of_bits) {
                data.insert(separator.data, separator.data + separator.size);
            }
        }

        if (num_of_bits > 0) {
            ColumnString::Chars off_sep_combo;
            off_sep_combo.reserve(separator.size + off.size);
            off_sep_combo.insert(off_sep_combo.end(), off.data, off.data + off.size);
            off_sep_combo.insert(off_sep_combo.end(), separator.data,
                                 separator.data + separator.size);

            for (size_t i = 0; i < num_of_bits; ++i) {
                data.insert(off_sep_combo.data(), off_sep_combo.data() + off_sep_combo.size());
            }
            data.erase(data.end() - separator.size, data.end());
        }

        res_col.insert_data(reinterpret_cast<const char*>(data.data()), data.size());
    }
};

// ATTN: for debug only
// compute crc32 hash value as the same way in `VOlapTablePartitionParam::find_tablets()`
class FunctionCrc32Internal : public IFunction {
public:
    static constexpr auto name = "crc32_internal";
    static FunctionPtr create() { return std::make_shared<FunctionCrc32Internal>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }
    bool use_default_implementation_for_nulls() const override { return false; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 1);

        auto argument_size = arguments.size();
        std::vector<ColumnPtr> argument_columns(argument_size);
        std::vector<PrimitiveType> argument_primitive_types(argument_size);

        for (size_t i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            argument_primitive_types[i] =
                    block.get_by_position(arguments[i]).type->get_primitive_type();
        }

        auto res_col = ColumnInt64::create();
        auto& res_data = res_col->get_data();
        res_data.resize_fill(input_rows_count, 0);

        for (size_t i = 0; i < input_rows_count; ++i) {
            uint32_t hash_val = 0;
            for (size_t j = 0; j < argument_size; ++j) {
                const auto& column = argument_columns[j];
                auto primitive_type = argument_primitive_types[j];
                auto val = column->get_data_at(i);
                if (val.data != nullptr) {
                    hash_val = RawValue::zlib_crc32(val.data, val.size, primitive_type, hash_val);
                } else {
                    hash_val = HashUtil::zlib_crc_hash_null(hash_val);
                }
            }
            res_data[i] = hash_val;
        }

        block.replace_by_position(result, std::move(res_col));
        return Status::OK();
    }
};

class FunctionUnicodeNormalize : public IFunction {
public:
    static constexpr auto name = "unicode_normalize";

    static FunctionPtr create() { return std::make_shared<FunctionUnicodeNormalize>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() != 2 || !is_string_type(arguments[0]->get_primitive_type()) ||
            !is_string_type(arguments[1]->get_primitive_type())) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Illegal type {} and {} of arguments of function {}",
                                   arguments[0]->get_name(), arguments[1]->get_name(), get_name());
        }
        return arguments[0];
    }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }

        if (!context->is_col_constant(1)) {
            return Status::InvalidArgument(
                    "The second argument 'mode' of function {} must be constant", get_name());
        }

        auto* const_col = context->get_constant_col(1);
        auto mode_ref = const_col->column_ptr->get_data_at(0);
        std::string lower_mode = doris::to_lower(std::string(doris::trim(mode_ref.to_string())));

        UErrorCode status = U_ZERO_ERROR;
        const icu::Normalizer2* normalizer = nullptr;

        if (lower_mode == "nfc") {
            normalizer = icu::Normalizer2::getInstance(nullptr, "nfc", UNORM2_COMPOSE, status);
        } else if (lower_mode == "nfd") {
            normalizer = icu::Normalizer2::getNFDInstance(status);
        } else if (lower_mode == "nfkc") {
            normalizer = icu::Normalizer2::getInstance(nullptr, "nfkc", UNORM2_COMPOSE, status);
        } else if (lower_mode == "nfkd") {
            normalizer = icu::Normalizer2::getNFKDInstance(status);
        } else if (lower_mode == "nfkc_cf") {
            normalizer = icu::Normalizer2::getInstance(nullptr, "nfkc_cf", UNORM2_COMPOSE, status);
        } else {
            return Status::InvalidArgument(
                    "Invalid normalization mode '{}' for function {}. "
                    "Supported modes: NFC, NFD, NFKC, NFKD, NFKC_CF",
                    lower_mode, get_name());
        }

        if (U_FAILURE(status) || normalizer == nullptr) {
            return Status::InvalidArgument(
                    "Failed to get normalizer instance for mode '{}' in function {}: {}",
                    lower_mode, get_name(), u_errorName(status));
        }

        auto state = std::make_shared<UnicodeNormalizeState>();
        state->normalizer = normalizer;
        context->set_function_state(scope, state);
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto* state = reinterpret_cast<UnicodeNormalizeState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (state == nullptr || state->normalizer == nullptr) {
            return Status::RuntimeError("unicode_normalize function state is not initialized");
        }

        ColumnPtr col =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto* col_str = check_and_get_column<ColumnString>(col.get());
        if (col_str == nullptr) {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        }

        const auto& data = col_str->get_chars();
        const auto& offsets = col_str->get_offsets();

        auto res = ColumnString::create();
        auto& res_data = res->get_chars();
        auto& res_offsets = res->get_offsets();

        size_t rows = offsets.size();
        res_offsets.resize(rows);

        std::string tmp;
        for (size_t i = 0; i < rows; ++i) {
            const char* begin = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t len = offsets[i] - offsets[i - 1];

            normalize_one(state->normalizer, begin, len, tmp);
            StringOP::push_value_string(tmp, i, res_data, res_offsets);
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

private:
    struct UnicodeNormalizeState {
        const icu::Normalizer2* normalizer = nullptr;
    };

    static void normalize_one(const icu::Normalizer2* normalizer, const char* input, size_t length,
                              std::string& output) {
        if (length == 0) {
            output.clear();
            return;
        }

        icu::StringPiece sp(input, static_cast<int32_t>(length));
        icu::UnicodeString src16 = icu::UnicodeString::fromUTF8(sp);

        UErrorCode status = U_ZERO_ERROR;
        UNormalizationCheckResult quick = normalizer->quickCheck(src16, status);
        if (U_SUCCESS(status) && quick == UNORM_YES) {
            output.assign(input, length);
            return;
        }

        icu::UnicodeString result16;
        status = U_ZERO_ERROR;
        normalizer->normalize(src16, result16, status);
        if (U_FAILURE(status)) {
            output.assign(input, length);
            return;
        }

        output.clear();
        result16.toUTF8String(output);
    }
};

using FunctionMakeSet = FunctionNeedsToHandleNull<MakeSetImpl, PrimitiveType::TYPE_STRING>;

void register_function_string_misc(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionAutoPartitionName>();
    factory.register_function<FunctionConvertTo>();
    factory.register_function<FunctionIntToChar>();
    factory.register_function<FunctionRandomBytes>();
    factory.register_function<FunctionTranslate>();
    factory.register_function<FunctionNgramSearch>();
    factory.register_function<FunctionXPathString>();
    factory.register_function<FunctionCrc32Internal>();
    factory.register_function<FunctionMakeSet>();
    factory.register_function<FunctionExportSet>();
    factory.register_function<FunctionUnicodeNormalize>();
}

#include "common/compile_check_avoid_end.h"
} // namespace doris

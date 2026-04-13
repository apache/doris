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

#include <cstddef>
#include <cstring>
#include <numeric>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/define_primitive_type.h"
#include "core/memcmp_small.h"
#include "core/memcpy_small.h"
#include "core/pod_array_fwd.h"
#include "core/string_ref.h"
#include "exec/common/stringop_substring.h"
#include "exec/common/template_helpers.hpp"
#include "exec/common/util.hpp"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "util/simd/vstring_function.h"
#include "util/string_search.hpp"

namespace doris {
#include "common/compile_check_avoid_begin.h"

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
                        uint32_t result, size_t input_rows_count) const override {
        if (arguments.size() != 3) {
            return Status::InvalidArgument("Function {} requires 3 arguments, but got {}",
                                           get_name(), arguments.size());
        }
        bool col_const[3];
        ColumnPtr argument_columns[3];
        for (int i = 0; i < 3; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }

        const auto* col_left = assert_cast<const ColumnString*>(argument_columns[0].get());
        const auto* col_right = assert_cast<const ColumnString*>(argument_columns[1].get());
        const auto* col_pos = assert_cast<const ColumnInt32*>(argument_columns[2].get());

        ColumnInt32::MutablePtr col_res = ColumnInt32::create();
        auto& vec_res = col_res->get_data();
        vec_res.resize(block.rows());

        const bool is_ascii = col_left->is_ascii() && col_right->is_ascii();

        if (col_const[0]) {
            std::visit(
                    [&](auto is_ascii, auto str_const, auto pos_const) {
                        scalar_search<is_ascii, str_const, pos_const>(
                                col_left->get_data_at(0), col_right, col_pos->get_data(), vec_res,
                                input_rows_count);
                    },
                    make_bool_variant(is_ascii), make_bool_variant(col_const[1]),
                    make_bool_variant(col_const[2]));

        } else {
            std::visit(
                    [&](auto is_ascii, auto str_const, auto pos_const) {
                        vector_search<is_ascii, str_const, pos_const>(col_left, col_right,
                                                                      col_pos->get_data(), vec_res,
                                                                      input_rows_count);
                    },
                    make_bool_variant(is_ascii), make_bool_variant(col_const[1]),
                    make_bool_variant(col_const[2]));
        }
        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

private:
    template <bool is_ascii, bool str_const, bool pos_const>
    void scalar_search(const StringRef& ldata, const ColumnString* col_right,
                       const PaddedPODArray<Int32>& posdata, PaddedPODArray<Int32>& res,
                       size_t size) const {
        res.resize(size);
        StringRef substr(ldata.data, ldata.size);
        StringSearch search {&substr};

        for (int i = 0; i < size; ++i) {
            res[i] = locate_pos<is_ascii>(substr,
                                          col_right->get_data_at(index_check_const<str_const>(i)),
                                          search, posdata[index_check_const<pos_const>(i)]);
        }
    }

    template <bool is_ascii, bool str_const, bool pos_const>
    void vector_search(const ColumnString* col_left, const ColumnString* col_right,
                       const PaddedPODArray<Int32>& posdata, PaddedPODArray<Int32>& res,
                       size_t size) const {
        res.resize(size);
        StringSearch search;
        for (int i = 0; i < size; ++i) {
            StringRef substr = col_left->get_data_at(i);
            search.set_pattern(&substr);
            res[i] = locate_pos<is_ascii>(substr,
                                          col_right->get_data_at(index_check_const<str_const>(i)),
                                          search, posdata[index_check_const<pos_const>(i)]);
        }
    }

    template <bool is_ascii>
    int locate_pos(StringRef substr, StringRef str, StringSearch& search, int start_pos) const {
        if (str.size == 0 && substr.size == 0 && start_pos == 1) {
            // BEHAVIOR COMPATIBLE WITH MYSQL
            // locate('','')	locate('','',1)	locate('','',2)
            // 1	1	0
            return 1;
        }
        if (is_ascii) {
            return locate_pos_ascii(substr, str, search, start_pos);
        } else {
            return locate_pos_utf8(substr, str, search, start_pos);
        }
    }

    int locate_pos_utf8(StringRef substr, StringRef str, StringSearch& search,
                        int start_pos) const {
        std::vector<size_t> index;
        size_t char_len = simd::VStringFunctions::get_char_len(str.data, str.size, index);
        if (start_pos <= 0 || start_pos > char_len) {
            return 0;
        }
        if (substr.size == 0) {
            return start_pos;
        }
        // Input start_pos starts from 1.
        StringRef adjusted_str(str.data + index[start_pos - 1], str.size - index[start_pos - 1]);
        int32_t match_pos = search.search(&adjusted_str);
        if (match_pos >= 0) {
            // Hive returns the position in the original string starting from 1.
            return start_pos + simd::VStringFunctions::get_char_len(adjusted_str.data, match_pos);
        } else {
            return 0;
        }
    }

    int locate_pos_ascii(StringRef substr, StringRef str, StringSearch& search,
                         int start_pos) const {
        if (start_pos <= 0 || start_pos > str.size) {
            return 0;
        }
        if (substr.size == 0) {
            return start_pos;
        }
        // Input start_pos starts from 1.
        StringRef adjusted_str(str.data + start_pos - 1, str.size - start_pos + 1);
        int32_t match_pos = search.search(&adjusted_str);
        if (match_pos >= 0) {
            // Hive returns the position in the original string starting from 1.
            return start_pos + match_pos;
        } else {
            return 0;
        }
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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
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
        std::vector<ColumnPtr> argument_columns(argument_size);
        for (size_t i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (const auto* nullable =
                        check_and_get_column<const ColumnNullable>(*argument_columns[i])) {
                // Danger: Here must dispose the null map data first! Because
                // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
                // of column nullable mem of null map
                VectorizedUtils::update_null_map(null_map->get_data(),
                                                 nullable->get_null_map_data());
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        const auto* str_col = assert_cast<const ColumnString*>(argument_columns[0].get());

        const auto* delimiter_col = assert_cast<const ColumnString*>(argument_columns[1].get());

        const auto* part_num_col = assert_cast<const ColumnInt32*>(argument_columns[2].get());
        const auto& part_num_col_data = part_num_col->get_data();

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
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 3);

        // Create a zero column to simply implement
        auto res = ColumnString::create();

        auto& res_offsets = res->get_offsets();
        auto& res_chars = res->get_chars();
        res_offsets.resize(input_rows_count);
        ColumnPtr content_column;
        bool content_const = false;
        std::tie(content_column, content_const) =
                unpack_if_const(block.get_by_position(arguments[0]).column);

        const auto* str_col = assert_cast<const ColumnString*>(content_column.get());

        // Handle both constant and non-constant delimiter parameters
        ColumnPtr delimiter_column_ptr;
        bool delimiter_const = false;
        std::tie(delimiter_column_ptr, delimiter_const) =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        const auto* delimiter_col = assert_cast<const ColumnString*>(delimiter_column_ptr.get());

        ColumnPtr part_num_column_ptr;
        bool part_num_const = false;
        std::tie(part_num_column_ptr, part_num_const) =
                unpack_if_const(block.get_by_position(arguments[2]).column);
        const ColumnInt32* part_num_col =
                assert_cast<const ColumnInt32*>(part_num_column_ptr.get());

        // For constant multi-character delimiters, create StringRef and StringSearch only once
        std::optional<StringRef> const_delimiter_ref;
        std::optional<StringSearch> const_search;
        if (delimiter_const && delimiter_col->get_data_at(0).size > 1) {
            const_delimiter_ref.emplace(delimiter_col->get_data_at(0));
            const_search.emplace(&const_delimiter_ref.value());
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            auto str = str_col->get_data_at(content_const ? 0 : i);
            auto delimiter = delimiter_col->get_data_at(delimiter_const ? 0 : i);
            int32_t delimiter_size = delimiter.size;

            auto part_number = part_num_col->get_element(part_num_const ? 0 : i);

            if (part_number == 0 || delimiter_size == 0) {
                StringOP::push_empty_string(i, res_chars, res_offsets);
                continue;
            }

            if (part_number > 0) {
                if (delimiter_size == 1) {
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
                } else {
                    // For multi-character delimiters
                    // Use pre-created StringRef and StringSearch for constant delimiters
                    StringRef delimiter_ref = const_delimiter_ref ? const_delimiter_ref.value()
                                                                  : StringRef(delimiter);
                    const StringSearch* search_ptr = const_search ? &const_search.value() : nullptr;
                    StringSearch local_search(&delimiter_ref);
                    if (!search_ptr) {
                        search_ptr = &local_search;
                    }

                    int32_t offset = -delimiter_size;
                    int32_t num = 0;
                    while (num < part_number) {
                        size_t n = str.size - offset - delimiter_size;
                        // search first match delimter_ref index from src string among str_offset to end
                        const char* pos = search_ptr->search(str.data + offset + delimiter_size, n);
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
            } else {
                int neg_part_number = -part_number;
                auto str_str = str.to_string();
                int32_t offset = str.size;
                int32_t pre_offset = offset;
                int32_t num = 0;
                auto substr = str_str;

                // Use pre-created StringRef for constant delimiters
                StringRef delimiter_str =
                        const_delimiter_ref
                                ? const_delimiter_ref.value()
                                : StringRef(reinterpret_cast<const char*>(delimiter.data),
                                            delimiter.size);

                while (num <= neg_part_number && offset >= 0) {
                    offset = (int)substr.rfind(delimiter_str, offset);
                    if (offset != -1) {
                        if (++num == neg_part_number) {
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

                if (num == neg_part_number) {
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

        block.get_by_position(result).column = std::move(res);
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
        DCHECK(is_string_type(arguments[0]->get_primitive_type()))
                << "first argument for function: " << name << " should be string"
                << " and arguments[0] is " << arguments[0]->get_name();
        DCHECK(is_string_type(arguments[1]->get_primitive_type()))
                << "second argument for function: " << name << " should be string"
                << " and arguments[1] is " << arguments[1]->get_name();
        return std::make_shared<DataTypeArray>(make_nullable(arguments[0]));
    }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 2);

        const auto& [src_column, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_column, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        DataTypePtr right_column_type = block.get_by_position(arguments[1]).type;
        DataTypePtr src_column_type = block.get_by_position(arguments[0]).type;
        auto dest_column_ptr = ColumnArray::create(make_nullable(src_column_type)->create_column(),
                                                   ColumnArray::ColumnOffsets::create());

        dest_column_ptr->resize(0);
        auto& dest_offsets = dest_column_ptr->get_offsets();

        auto& dest_nullable_col = assert_cast<ColumnNullable&>(dest_column_ptr->get_data());
        auto* dest_nested_column = dest_nullable_col.get_nested_column_ptr().get();

        const auto* col_str = assert_cast<const ColumnString*>(src_column.get());

        const auto* col_delimiter = assert_cast<const ColumnString*>(right_column.get());

        std::visit(
                [&](auto src_const, auto delimiter_const) {
                    _execute<src_const, delimiter_const>(*col_str, *col_delimiter,
                                                         *dest_nested_column, dest_offsets,
                                                         input_rows_count);
                },
                make_bool_variant(left_const), make_bool_variant(right_const));

        // all elements in dest_nested_column are not null
        dest_nullable_col.get_null_map_column().get_data().resize_fill(dest_nested_column->size(),
                                                                       false);
        block.replace_by_position(result, std::move(dest_column_ptr));

        return Status::OK();
    }

private:
    template <bool src_const, bool delimiter_const>
    void _execute(const ColumnString& src_column_string, const ColumnString& delimiter_column,
                  IColumn& dest_nested_column, ColumnArray::Offsets64& dest_offsets,
                  size_t size) const {
        auto& dest_column_string = assert_cast<ColumnString&>(dest_nested_column);
        ColumnString::Chars& column_string_chars = dest_column_string.get_chars();
        ColumnString::Offsets& column_string_offsets = dest_column_string.get_offsets();
        column_string_chars.reserve(0);

        ColumnArray::Offset64 string_pos = 0;
        ColumnArray::Offset64 dest_pos = 0;

        StringSearch search;
        StringRef delimiter_ref_for_search;

        if constexpr (delimiter_const) {
            delimiter_ref_for_search = delimiter_column.get_data_at(0);
            search.set_pattern(&delimiter_ref_for_search);
        }

        for (size_t i = 0; i < size; i++) {
            const StringRef str_ref =
                    src_column_string.get_data_at(index_check_const<src_const>(i));
            const StringRef delimiter_ref =
                    delimiter_column.get_data_at(index_check_const<delimiter_const>(i));

            if (str_ref.size == 0) {
                dest_offsets.push_back(dest_pos);
                continue;
            }
            if (delimiter_ref.size == 0) {
                split_empty_delimiter(str_ref, column_string_chars, column_string_offsets,
                                      string_pos, dest_pos);
            } else {
                if constexpr (!delimiter_const) {
                    search.set_pattern(&delimiter_ref);
                }
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
                    // array offset + 1
                    dest_pos++;
                    // add src string str_pos to next search start
                    str_pos += split_part_size + delimiter_ref.size;
                }
            }
            dest_offsets.push_back(dest_pos);
        }
    }

    void split_empty_delimiter(const StringRef& str_ref, ColumnString::Chars& column_string_chars,
                               ColumnString::Offsets& column_string_offsets,
                               ColumnArray::Offset64& string_pos,
                               ColumnArray::Offset64& dest_pos) const {
        const size_t old_size = column_string_chars.size();
        const size_t new_size = old_size + str_ref.size;
        column_string_chars.resize(new_size);
        memcpy(column_string_chars.data() + old_size, str_ref.data, str_ref.size);
        if (simd::VStringFunctions::is_ascii(str_ref)) {
            const auto size = str_ref.size;

            const auto nested_old_size = column_string_offsets.size();
            const auto nested_new_size = nested_old_size + size;
            column_string_offsets.resize(nested_new_size);
            std::iota(column_string_offsets.data() + nested_old_size,
                      column_string_offsets.data() + nested_new_size, string_pos + 1);

            string_pos += size;
            dest_pos += size;
            // The above code is equivalent to the code in the following comment.
            // for (size_t i = 0; i < str_ref.size; i++) {
            //     string_pos++;
            //     column_string_offsets.push_back(string_pos);
            //     (*dest_nested_null_map).push_back(false);
            //     dest_pos++;
            // }
        } else {
            for (size_t i = 0, utf8_char_len = 0; i < str_ref.size; i += utf8_char_len) {
                utf8_char_len = UTF8_BYTE_LENGTH[(unsigned char)str_ref.data[i]];

                string_pos += utf8_char_len;
                column_string_offsets.push_back(string_pos);
                dest_pos++;
            }
        }
    }
};

enum class FunctionCountSubStringType { TWO_ARGUMENTS, THREE_ARGUMENTS };

template <FunctionCountSubStringType type>
class FunctionCountSubString : public IFunction {
public:
    static constexpr auto name = "count_substrings";
    static constexpr auto arg_count = (type == FunctionCountSubStringType::TWO_ARGUMENTS) ? 2 : 3;

    static FunctionPtr create() { return std::make_shared<FunctionCountSubString>(); }
    using NullMapType = PaddedPODArray<UInt8>;

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return arg_count; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt32>();
    }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (type == FunctionCountSubStringType::TWO_ARGUMENTS) {
            return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
        } else {
            return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                    std::make_shared<DataTypeInt32>()};
        }
    }

    bool is_variadic() const override { return true; }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK(arg_count);
        bool col_const[arg_count];
        ColumnPtr argument_columns[arg_count];
        for (int i = 0; i < arg_count; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }

        auto dest_column_ptr = ColumnInt32::create(input_rows_count);
        auto& dest_column_data = dest_column_ptr->get_data();

        if constexpr (type == FunctionCountSubStringType::TWO_ARGUMENTS) {
            const auto& src_column_string = assert_cast<const ColumnString&>(*argument_columns[0]);
            const auto& pattern_column = assert_cast<const ColumnString&>(*argument_columns[1]);
            std::visit(
                    [&](auto str_const, auto pattern_const) {
                        _execute<str_const, pattern_const>(src_column_string, pattern_column,
                                                           dest_column_data, input_rows_count);
                    },
                    make_bool_variant(col_const[0]), make_bool_variant(col_const[1]));
        } else {
            const auto& src_column_string = assert_cast<const ColumnString&>(*argument_columns[0]);
            const auto& pattern_column = assert_cast<const ColumnString&>(*argument_columns[1]);
            const auto& start_pos_column = assert_cast<const ColumnInt32&>(*argument_columns[2]);
            std::visit(
                    [&](auto str_const, auto pattern_const, auto start_pos_const) {
                        _execute<str_const, pattern_const, start_pos_const>(
                                src_column_string, pattern_column, start_pos_column,
                                dest_column_data, input_rows_count);
                    },
                    make_bool_variant(col_const[0]), make_bool_variant(col_const[1]),
                    make_bool_variant(col_const[2]));
        }

        block.replace_by_position(result, std::move(dest_column_ptr));
        return Status::OK();
    }

private:
    template <bool src_const, bool pattern_const>
    void _execute(const ColumnString& src_column_string, const ColumnString& pattern_column,
                  ColumnInt32::Container& dest_column_data, size_t size) const {
        for (size_t i = 0; i < size; i++) {
            const StringRef str_ref =
                    src_column_string.get_data_at(index_check_const<src_const>(i));

            const StringRef pattern_ref =
                    pattern_column.get_data_at(index_check_const<pattern_const>(i));
            dest_column_data[i] = find_str_count(str_ref, pattern_ref);
        }
    }

    template <bool src_const, bool pattern_const, bool start_pos_const>
    void _execute(const ColumnString& src_column_string, const ColumnString& pattern_column,
                  const ColumnInt32& start_pos_column, ColumnInt32::Container& dest_column_data,
                  size_t size) const {
        for (size_t i = 0; i < size; i++) {
            const StringRef str_ref =
                    src_column_string.get_data_at(index_check_const<src_const>(i));
            const StringRef pattern_ref =
                    pattern_column.get_data_at(index_check_const<pattern_const>(i));
            // 1-based index
            int32_t start_pos =
                    start_pos_column.get_element(index_check_const<start_pos_const>(i)) - 1;

            const char* p = str_ref.begin();
            const char* end = str_ref.end();
            int char_size = 0;
            for (size_t j = 0; j < start_pos && p < end; ++j, p += char_size) {
                char_size = UTF8_BYTE_LENGTH[static_cast<uint8_t>(*p)];
            }
            const auto start_byte_len = p - str_ref.begin();

            if (start_pos < 0 || start_byte_len >= str_ref.size) {
                dest_column_data[i] = 0;
            } else {
                dest_column_data[i] =
                        find_str_count(str_ref.substring(start_byte_len), pattern_ref);
            }
        }
    }

    size_t find_pos(size_t pos, const StringRef str_ref, const StringRef pattern_ref) const {
        size_t old_size = pos;
        size_t str_size = str_ref.size;
        while (pos < str_size &&
               memcmp_small_allow_overflow15((const uint8_t*)str_ref.data + pos,
                                             (const uint8_t*)pattern_ref.data, pattern_ref.size)) {
            pos++;
        }
        return pos - old_size;
    }

    int find_str_count(const StringRef str_ref, StringRef pattern_ref) const {
        int count = 0;
        if (str_ref.size == 0 || pattern_ref.size == 0) {
            return 0;
        } else {
            for (size_t str_pos = 0; str_pos <= str_ref.size;) {
                const size_t res_pos = find_pos(str_pos, str_ref, pattern_ref);
                if (res_pos == (str_ref.size - str_pos)) {
                    break; // not find
                }
                count++;
                str_pos = str_pos + res_pos + pattern_ref.size;
            }
        }
        return count;
    }
};

void register_function_string_search(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStringLocatePos>();
    factory.register_function<FunctionSplitPart>();
    factory.register_function<FunctionSplitByString>();
    factory.register_function<FunctionCountSubString<FunctionCountSubStringType::TWO_ARGUMENTS>>();
    factory.register_function<
            FunctionCountSubString<FunctionCountSubStringType::THREE_ARGUMENTS>>();
    factory.register_function<FunctionSubstringIndex>();

    factory.register_alias(FunctionStringLocatePos::name, "position");
}

#include "common/compile_check_avoid_end.h"
} // namespace doris

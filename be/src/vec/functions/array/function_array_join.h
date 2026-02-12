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

#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_execute_util.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/array/function_array_utils.h"

namespace doris::vectorized {

#include "common/compile_check_begin.h"
struct NameArrayJoin {
    static constexpr auto name = "array_join";
};

struct ArrayJoinImpl {
public:
    using column_type = ColumnArray;
    using NullMapType = PaddedPODArray<UInt8>;

    static bool _is_variadic() { return true; }

    static size_t _get_number_of_arguments() { return 0; }

    static DataTypePtr get_return_type(const DataTypes& arguments) {
        DCHECK(arguments[0]->get_primitive_type() == TYPE_ARRAY)
                << "first argument for function: array_join should be DataTypeArray"
                << " and arguments[0] is " << arguments[0]->get_name();
        DCHECK(is_string_type(arguments[1]->get_primitive_type()))
                << "second argument for function: array_join should be DataTypeString"
                << ", and arguments[1] is " << arguments[1]->get_name();
        if (arguments.size() > 2) {
            DCHECK(is_string_type(arguments[2]->get_primitive_type()))
                    << "third argument for function: array_join should be DataTypeString"
                    << ", and arguments[2] is " << arguments[2]->get_name();
        }

        return std::make_shared<DataTypeString>();
    }

    static Status execute(Block& block, const ColumnNumbers& arguments, uint32_t result,
                          const DataTypeArray* data_type_array, const ColumnArray& array) {
        ColumnPtr src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        ColumnArrayExecutionData src;
        if (!extract_column_array_info(*src_column, src)) {
            return Status::RuntimeError(fmt::format(
                    "execute failed, unsupported types for function {}({})", "array_join",
                    block.get_by_position(arguments[0]).type->get_name()));
        }

        auto nested_type = data_type_array->get_nested_type();
        auto dest_column_ptr = ColumnString::create();

        auto& dest_chars = dest_column_ptr->get_chars();
        auto& dest_offsets = dest_column_ptr->get_offsets();

        dest_offsets.resize_fill(src_column->size(), 0);

        auto sep_column =
                ColumnView<TYPE_STRING>::create(block.get_by_position(arguments[1]).column);

        if (arguments.size() > 2) {
            auto null_replace_column =
                    ColumnView<TYPE_STRING>::create(block.get_by_position(arguments[2]).column);

            _execute_string(*src.nested_col, *src.offsets_ptr, src.nested_nullmap_data, sep_column,
                            null_replace_column, dest_chars, dest_offsets);

        } else {
            auto tmp_column_string = ColumnString::create();
            // insert default value for null replacement, which is empty string
            tmp_column_string->insert_default();
            ColumnPtr tmp_const_column =
                    ColumnConst::create(std::move(tmp_column_string), sep_column.size());

            auto null_replace_column = ColumnView<TYPE_STRING>::create(tmp_const_column);

            _execute_string(*src.nested_col, *src.offsets_ptr, src.nested_nullmap_data, sep_column,
                            null_replace_column, dest_chars, dest_offsets);
        }

        block.replace_by_position(result, std::move(dest_column_ptr));
        return Status::OK();
    }

private:
    // same as ColumnString::insert_data
    static void insert_to_chars(int64_t i, ColumnString::Chars& chars, uint32_t& total_size,
                                const char* pos, size_t length) {
        const size_t old_size = chars.size();
        const size_t new_size = old_size + length;

        if (length) {
            ColumnString::check_chars_length(new_size, i);
            chars.resize(new_size);
            memcpy(chars.data() + old_size, pos, length);
            total_size += length;
        }
    }

    static void _fill_result_string(int64_t i, const StringRef& input_str, const StringRef& sep_str,
                                    ColumnString::Chars& dest_chars, uint32_t& total_size,
                                    bool& is_first_elem) {
        if (is_first_elem) {
            insert_to_chars(i, dest_chars, total_size, input_str.data, input_str.size);
            is_first_elem = false;
        } else {
            insert_to_chars(i, dest_chars, total_size, sep_str.data, sep_str.size);
            insert_to_chars(i, dest_chars, total_size, input_str.data, input_str.size);
        }
    }

    static void _execute_string(const IColumn& src_column,
                                const ColumnArray::Offsets64& src_offsets,
                                const UInt8* src_null_map, ColumnView<TYPE_STRING>& sep_column,
                                ColumnView<TYPE_STRING>& null_replace_column,
                                ColumnString::Chars& dest_chars,
                                ColumnString::Offsets& dest_offsets) {
        const auto& src_data = assert_cast<const ColumnString&>(src_column);

        uint32_t total_size = 0;

        for (int64_t i = 0; i < src_offsets.size(); ++i) {
            auto begin = src_offsets[i - 1];
            auto end = src_offsets[i];

            auto sep_str = sep_column.value_at(i);
            auto null_replace_str = null_replace_column.value_at(i);

            bool is_first_elem = true;

            for (size_t j = begin; j < end; ++j) {
                if (src_null_map && src_null_map[j]) {
                    if (null_replace_str.size != 0) {
                        _fill_result_string(i, null_replace_str, sep_str, dest_chars, total_size,
                                            is_first_elem);
                    }
                    continue;
                }

                StringRef src_str_ref = src_data.get_data_at(j);
                _fill_result_string(i, src_str_ref, sep_str, dest_chars, total_size, is_first_elem);
            }

            dest_offsets[i] = total_size;
        }
    }
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized

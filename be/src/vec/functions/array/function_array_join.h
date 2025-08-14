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

        ColumnPtr sep_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        ColumnPtr null_replace_column =
                (arguments.size() > 2 ? block.get_by_position(arguments[2])
                                                .column->convert_to_full_column_if_const()
                                      : nullptr);

        std::string sep_str = _get_string_from_column(sep_column);
        std::string null_replace_str = _get_string_from_column(null_replace_column);

        auto nested_type = data_type_array->get_nested_type();
        auto dest_column_ptr = ColumnString::create();
        DCHECK(dest_column_ptr);

        auto res_val = _execute_string(*src.nested_col, *src.offsets_ptr, src.nested_nullmap_data,
                                       sep_str, null_replace_str, dest_column_ptr.get());
        if (!res_val) {
            return Status::RuntimeError(fmt::format(
                    "execute failed or unsupported types for function {}({},{},{})", "array_join",
                    block.get_by_position(arguments[0]).type->get_name(),
                    block.get_by_position(arguments[1]).type->get_name(),
                    (arguments.size() > 2 ? block.get_by_position(arguments[2]).type->get_name()
                                          : "")));
        }

        block.replace_by_position(result, std::move(dest_column_ptr));
        return Status::OK();
    }

private:
    static std::string _get_string_from_column(const ColumnPtr& column_ptr) {
        if (!column_ptr) {
            return std::string("");
        }
        const ColumnString* column_string_ptr = check_and_get_column<ColumnString>(*column_ptr);
        StringRef str_ref = column_string_ptr->get_data_at(0);
        std::string str(str_ref.data, str_ref.size);
        return str;
    }

    static void _fill_result_string(const std::string& input_str, const std::string& sep_str,
                                    std::string& result_str, bool& is_first_elem) {
        if (is_first_elem) {
            result_str.append(input_str);
            is_first_elem = false;
        } else {
            result_str.append(sep_str);
            result_str.append(input_str);
        }
        return;
    }


    static bool _execute_string(const IColumn& src_column,
                                const ColumnArray::Offsets64& src_offsets,
                                const UInt8* src_null_map, const std::string& sep_str,
                                const std::string& null_replace_str,
                                ColumnString* dest_column_ptr) {
        const ColumnString* src_data_concrete = assert_cast<const ColumnString*>(&src_column);
        if (!src_data_concrete) {
            return false;
        }

        size_t prev_src_offset = 0;
        for (auto curr_src_offset : src_offsets) {
            std::string result_str;
            bool is_first_elem = true;
            for (size_t j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_null_map && src_null_map[j]) {
                    if (null_replace_str.size() == 0) {
                        continue;
                    } else {
                        _fill_result_string(null_replace_str, sep_str, result_str, is_first_elem);
                        continue;
                    }
                }

                StringRef src_str_ref = src_data_concrete->get_data_at(j);
                std::string elem_str(src_str_ref.data, src_str_ref.size);
                _fill_result_string(elem_str, sep_str, result_str, is_first_elem);
            }

            dest_column_ptr->insert_data(result_str.c_str(), result_str.size());
            prev_src_offset = curr_src_offset;
        }
        return true;
    }
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized

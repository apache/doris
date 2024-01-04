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
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/array/function_array_utils.h"

namespace doris::vectorized {

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
        DCHECK(is_array(arguments[0]))
                << "first argument for function: array_join should be DataTypeArray"
                << " and arguments[0] is " << arguments[0]->get_name();
        DCHECK(is_string_or_fixed_string(arguments[1]))
                << "second argument for function: array_join should be DataTypeString"
                << ", and arguments[1] is " << arguments[1]->get_name();
        if (arguments.size() > 2) {
            DCHECK(is_string_or_fixed_string(arguments[2]))
                    << "third argument for function: array_join should be DataTypeString"
                    << ", and arguments[2] is " << arguments[2]->get_name();
        }

        return std::make_shared<DataTypeString>();
    }

    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result,
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
        DCHECK(dest_column_ptr != nullptr);

        auto res_val = _execute_by_type(*src.nested_col, *src.offsets_ptr, src.nested_nullmap_data,
                                        sep_str, null_replace_str, nested_type, dest_column_ptr);
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

    template <typename ColumnType>
    static bool _execute_number(const IColumn& src_column,
                                const ColumnArray::Offsets64& src_offsets,
                                const UInt8* src_null_map, const std::string& sep_str,
                                const std::string& null_replace_str, DataTypePtr& nested_type,
                                ColumnString* dest_column_ptr) {
        using NestType = typename ColumnType::value_type;
        bool is_decimal = IsDecimalNumber<NestType>;

        const ColumnType* src_data_concrete = reinterpret_cast<const ColumnType*>(&src_column);
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

                if (is_decimal) {
                    DecimalV2Value decimal_value =
                            (DecimalV2Value)(src_data_concrete->get_data()[j]);
                    std::string decimal_str = decimal_value.to_string();
                    _fill_result_string(decimal_str, sep_str, result_str, is_first_elem);
                } else {
                    std::string elem_str = remove_nullable(nested_type)->to_string(src_column, j);
                    _fill_result_string(elem_str, sep_str, result_str, is_first_elem);
                }
            }

            dest_column_ptr->insert_data(result_str.c_str(), result_str.size());
            prev_src_offset = curr_src_offset;
        }

        return true;
    }

    static bool _execute_string(const IColumn& src_column,
                                const ColumnArray::Offsets64& src_offsets,
                                const UInt8* src_null_map, const std::string& sep_str,
                                const std::string& null_replace_str,
                                ColumnString* dest_column_ptr) {
        const ColumnString* src_data_concrete = reinterpret_cast<const ColumnString*>(&src_column);
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

    static bool _execute_by_type(const IColumn& src_column,
                                 const ColumnArray::Offsets64& src_offsets,
                                 const UInt8* src_null_map, const std::string& sep_str,
                                 const std::string& null_replace_str, DataTypePtr& nested_type,
                                 ColumnString* dest_column_ptr) {
        bool res = false;
        WhichDataType which(remove_nullable(nested_type));
        if (which.is_uint8()) {
            res = _execute_number<ColumnUInt8>(src_column, src_offsets, src_null_map, sep_str,
                                               null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_int8()) {
            res = _execute_number<ColumnInt8>(src_column, src_offsets, src_null_map, sep_str,
                                              null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_int16()) {
            res = _execute_number<ColumnInt16>(src_column, src_offsets, src_null_map, sep_str,
                                               null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_int32()) {
            res = _execute_number<ColumnInt32>(src_column, src_offsets, src_null_map, sep_str,
                                               null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_int64()) {
            res = _execute_number<ColumnInt64>(src_column, src_offsets, src_null_map, sep_str,
                                               null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_int128()) {
            res = _execute_number<ColumnInt128>(src_column, src_offsets, src_null_map, sep_str,
                                                null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_float32()) {
            res = _execute_number<ColumnFloat32>(src_column, src_offsets, src_null_map, sep_str,
                                                 null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_float64()) {
            res = _execute_number<ColumnFloat64>(src_column, src_offsets, src_null_map, sep_str,
                                                 null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_date()) {
            res = _execute_number<ColumnDate>(src_column, src_offsets, src_null_map, sep_str,
                                              null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_date_time()) {
            res = _execute_number<ColumnDateTime>(src_column, src_offsets, src_null_map, sep_str,
                                                  null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_date_v2()) {
            res = _execute_number<ColumnDateV2>(src_column, src_offsets, src_null_map, sep_str,
                                                null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_date_time_v2()) {
            res = _execute_number<ColumnDateTimeV2>(src_column, src_offsets, src_null_map, sep_str,
                                                    null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_decimal32()) {
            res = _execute_number<ColumnDecimal32>(src_column, src_offsets, src_null_map, sep_str,
                                                   null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_decimal64()) {
            res = _execute_number<ColumnDecimal64>(src_column, src_offsets, src_null_map, sep_str,
                                                   null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_decimal128v3()) {
            res = _execute_number<ColumnDecimal128V3>(src_column, src_offsets, src_null_map,
                                                      sep_str, null_replace_str, nested_type,
                                                      dest_column_ptr);
        } else if (which.is_decimal256()) {
            res = _execute_number<ColumnDecimal256>(src_column, src_offsets, src_null_map, sep_str,
                                                    null_replace_str, nested_type, dest_column_ptr);
        } else if (which.is_decimal128v2()) {
            res = _execute_number<ColumnDecimal128V2>(src_column, src_offsets, src_null_map,
                                                      sep_str, null_replace_str, nested_type,
                                                      dest_column_ptr);
        } else if (which.is_string()) {
            res = _execute_string(src_column, src_offsets, src_null_map, sep_str, null_replace_str,
                                  dest_column_ptr);
        }
        return res;
    }
};

} // namespace doris::vectorized

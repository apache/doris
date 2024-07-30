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

#include <fmt/format.h>
#include <glog/logging.h>

#include <type_traits>

#include "common/status.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionArrayContainsAll : public IFunction {
public:
    static constexpr auto name = "array_contains_all";
    static FunctionPtr create() { return std::make_shared<FunctionArrayContainsAll>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_nulls() const override { return true; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        auto left_data_type = remove_nullable(arguments[0]);
        auto right_data_type = remove_nullable(arguments[1]);
        DCHECK(is_array(left_data_type)) << arguments[0]->get_name();
        DCHECK(is_array(right_data_type)) << arguments[1]->get_name();
        auto left_nested_type = remove_nullable(
                assert_cast<const DataTypeArray&>(*left_data_type).get_nested_type());
        auto right_nested_type = remove_nullable(
                assert_cast<const DataTypeArray&>(*right_data_type).get_nested_type());
        DCHECK(left_nested_type->equals(*right_nested_type))
                << "data type " << arguments[0]->get_name() << " not equal with "
                << arguments[1]->get_name();
        return std::make_shared<DataTypeUInt8>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto& [left_column, left_is_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_column, right_is_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        ColumnArrayExecutionData left_exec_data;
        ColumnArrayExecutionData right_exec_data;
        Status ret = Status::OK();

        // extract array column
        if (!extract_column_array_info(*left_column, left_exec_data) ||
            !extract_column_array_info(*right_column, right_exec_data)) {
            return Status::InvalidArgument(
                    "execute failed, unsupported types for function {}({}, {})", get_name(),
                    block.get_by_position(arguments[0]).type->get_name(),
                    block.get_by_position(arguments[1]).type->get_name());
        }
        // prepare return column
        auto dst_nested_col = ColumnVector<UInt8>::create(input_rows_count, 0);
        auto dst_null_map = ColumnVector<UInt8>::create(input_rows_count, 0);
        UInt8* dst_null_map_data = dst_null_map->get_data().data();

        // execute check of contains all
        auto array_type = remove_nullable(block.get_by_position(arguments[0]).type);
        auto left_element_type =
                remove_nullable(assert_cast<const DataTypeArray&>(*array_type).get_nested_type());
        WhichDataType left_which_type(left_element_type);
        if (left_which_type.is_string()) {
            ret = _execute_internal<ColumnString>(left_exec_data, right_exec_data,
                                                  dst_null_map_data,
                                                  dst_nested_col->get_data().data(),
                                                  input_rows_count, left_is_const, right_is_const);
        } else if (left_which_type.is_date()) {
            ret = _execute_internal<ColumnDate>(left_exec_data, right_exec_data, dst_null_map_data,
                                                dst_nested_col->get_data().data(), input_rows_count,
                                                left_is_const, right_is_const);
        } else if (left_which_type.is_date_time()) {
            ret = _execute_internal<ColumnDateTime>(
                    left_exec_data, right_exec_data, dst_null_map_data,
                    dst_nested_col->get_data().data(), input_rows_count, left_is_const,
                    right_is_const);
        } else if (left_which_type.is_date_v2()) {
            ret = _execute_internal<ColumnDateV2>(left_exec_data, right_exec_data,
                                                  dst_null_map_data,
                                                  dst_nested_col->get_data().data(),
                                                  input_rows_count, left_is_const, right_is_const);
        } else if (left_which_type.is_date_time_v2()) {
            ret = _execute_internal<ColumnDateTimeV2>(
                    left_exec_data, right_exec_data, dst_null_map_data,
                    dst_nested_col->get_data().data(), input_rows_count, left_is_const,
                    right_is_const);
        } else if (left_which_type.is_uint8()) {
            ret = _execute_internal<ColumnUInt8>(left_exec_data, right_exec_data, dst_null_map_data,
                                                 dst_nested_col->get_data().data(),
                                                 input_rows_count, left_is_const, right_is_const);
        } else if (left_which_type.is_int8()) {
            ret = _execute_internal<ColumnInt8>(left_exec_data, right_exec_data, dst_null_map_data,
                                                dst_nested_col->get_data().data(), input_rows_count,
                                                left_is_const, right_is_const);
        } else if (left_which_type.is_int16()) {
            ret = _execute_internal<ColumnInt16>(left_exec_data, right_exec_data, dst_null_map_data,
                                                 dst_nested_col->get_data().data(),
                                                 input_rows_count, left_is_const, right_is_const);
        } else if (left_which_type.is_int32()) {
            ret = _execute_internal<ColumnInt32>(left_exec_data, right_exec_data, dst_null_map_data,
                                                 dst_nested_col->get_data().data(),
                                                 input_rows_count, left_is_const, right_is_const);
        } else if (left_which_type.is_int64()) {
            ret = _execute_internal<ColumnInt64>(left_exec_data, right_exec_data, dst_null_map_data,
                                                 dst_nested_col->get_data().data(),
                                                 input_rows_count, left_is_const, right_is_const);
        } else if (left_which_type.is_int128()) {
            ret = _execute_internal<ColumnInt128>(left_exec_data, right_exec_data,
                                                  dst_null_map_data,
                                                  dst_nested_col->get_data().data(),
                                                  input_rows_count, left_is_const, right_is_const);
        } else if (left_which_type.is_float32()) {
            ret = _execute_internal<ColumnFloat32>(left_exec_data, right_exec_data,
                                                   dst_null_map_data,
                                                   dst_nested_col->get_data().data(),
                                                   input_rows_count, left_is_const, right_is_const);
        } else if (left_which_type.is_float64()) {
            ret = _execute_internal<ColumnFloat64>(left_exec_data, right_exec_data,
                                                   dst_null_map_data,
                                                   dst_nested_col->get_data().data(),
                                                   input_rows_count, left_is_const, right_is_const);
        } else if (left_which_type.is_decimal32()) {
            ret = _execute_internal<ColumnDecimal32>(
                    left_exec_data, right_exec_data, dst_null_map_data,
                    dst_nested_col->get_data().data(), input_rows_count, left_is_const,
                    right_is_const);
        } else if (left_which_type.is_decimal64()) {
            ret = _execute_internal<ColumnDecimal64>(
                    left_exec_data, right_exec_data, dst_null_map_data,
                    dst_nested_col->get_data().data(), input_rows_count, left_is_const,
                    right_is_const);
        } else if (left_which_type.is_decimal128v3()) {
            ret = _execute_internal<ColumnDecimal128V3>(
                    left_exec_data, right_exec_data, dst_null_map_data,
                    dst_nested_col->get_data().data(), input_rows_count, left_is_const,
                    right_is_const);
        } else if (left_which_type.is_decimal128v2()) {
            ret = _execute_internal<ColumnDecimal128V2>(
                    left_exec_data, right_exec_data, dst_null_map_data,
                    dst_nested_col->get_data().data(), input_rows_count, left_is_const,
                    right_is_const);
        } else if (left_which_type.is_decimal256()) {
            ret = _execute_internal<ColumnDecimal256>(
                    left_exec_data, right_exec_data, dst_null_map_data,
                    dst_nested_col->get_data().data(), input_rows_count, left_is_const,
                    right_is_const);
        } else {
            ret = Status::RuntimeError(
                    fmt::format("execute failed about function {}, the argument not support {} ",
                                get_name(), block.get_by_position(arguments[0]).type->get_name()));
        }
        if (ret.ok()) {
            block.replace_by_position(result, std::move(dst_nested_col));
        }
        return ret;
    }

private:
    template <typename T>
    Status _execute_internal(const ColumnArrayExecutionData& left_data,
                             const ColumnArrayExecutionData& right_data,
                             const UInt8* dst_nullmap_data, UInt8* dst_data,
                             size_t input_rows_count, bool left_is_const,
                             bool right_is_const) const {
        for (ssize_t row = 0; row < input_rows_count; ++row) {
            auto left_index = index_check_const(row, left_is_const);
            auto right_index = index_check_const(row, right_is_const);
            ssize_t left_start = (*left_data.offsets_ptr)[left_index - 1];
            ssize_t left_end = (*left_data.offsets_ptr)[left_index];
            ssize_t left_size = left_end - left_start;
            ssize_t right_start = (*right_data.offsets_ptr)[right_index - 1];
            ssize_t right_end = (*right_data.offsets_ptr)[right_index];
            ssize_t right_size = right_end - right_start;
            // case: [1,2,3] : []
            if (right_size == 0) {
                dst_data[row] = 1;
                continue;
            }
            // case: [1,2,3] : [1,2,3,4,5]
            // case: [] : [1,2,3]
            if ((left_size < right_size) || (left_size == 0)) {
                dst_data[row] = 0;
                continue;
            }

            bool is_equal_value = false;
            auto left_pos = left_start;
            auto right_pos = right_start;
            while (left_pos < left_end) {
                // case: left elements size is smaller than right
                if (left_end - left_pos < right_size) {
                    is_equal_value = false;
                    break;
                }
                int left_nested_loop_pos = left_pos;
                right_pos = right_start;
                while (right_pos < right_end) {
                    bool left_nested_data_is_null =
                            left_data.nested_nullmap_data[left_nested_loop_pos];
                    bool right_nested_data_is_null = right_data.nested_nullmap_data[right_pos];
                    if (left_nested_data_is_null && right_nested_data_is_null) {
                        // null == null
                        is_equal_value = true;
                    } else if (left_nested_data_is_null || right_nested_data_is_null) {
                        // one is null, another is not null
                        is_equal_value = false;
                    } else {
                        // all is not null, check the data is equal
                        const auto* left_column = assert_cast<const T*>(left_data.nested_col);
                        const auto* right_column = assert_cast<const T*>(right_data.nested_col);
                        auto res = left_column->compare_at(left_nested_loop_pos, right_pos,
                                                           *right_column, -1);
                        is_equal_value = (res == 0);
                    }
                    if (is_equal_value) {
                        left_nested_loop_pos++;
                        right_pos++;
                    } else {
                        break;
                    }
                }
                if (right_pos == right_end) {
                    // have check all of value in right
                    is_equal_value = true;
                    break;
                }
                // move the left pos to check again
                left_pos++;
            }
            dst_data[row] = is_equal_value;
        }
        return Status::OK();
    }
};

void register_function_array_contains_all(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayContainsAll>();
    factory.register_alias("array_contains_all", "hasSubstr");
}

} // namespace doris::vectorized

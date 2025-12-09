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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_needs_to_handle_null.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
class FunctionIntervalImpl {
public:
    static constexpr auto name = "interval";
    static size_t get_number_of_arguments() { return 0; }
    static bool is_variadic() { return true; }
    static DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) {
        return std::make_shared<DataTypeInt32>();
    }

    static bool is_return_nullable(bool has_nullable,
                                   const std::vector<ColumnWithConstAndNullMap>& cols_info) {
        return false;
    }

    static bool execute_const_null(ColumnInt32::MutablePtr& res_col,
                                   PaddedPODArray<UInt8>& res_null_map_data,
                                   size_t input_rows_count, size_t null_index) {
        if (null_index == 0) {
            res_col->insert_many_vals(-1, input_rows_count);
            return true;
        }
        return false;
    }

    static void collect_columns_info(std::vector<ColumnWithConstAndNullMap>& columns_info,
                                     Block& block, const ColumnNumbers& arguments,
                                     bool& has_nullable) {
        const size_t arg_size = arguments.size();
        std::vector<ColumnPtr> arg_col(arg_size);
        std::vector<uint8_t> is_const(arg_size);
        std::vector<size_t> param_idx(arg_size - 1);
        bool all_const = true;
        for (size_t i = 0; i < arg_size; ++i) {
            is_const[i] = is_column_const(*block.get_by_position(arguments[i]).column);
            if (i != 0) {
                param_idx[i - 1] = i;
                all_const = all_const & is_const[i];
            }
        }
        arg_col[0] = is_const[0] ? assert_cast<const ColumnConst&>(
                                           *block.get_by_position(arguments[0]).column)
                                           .get_data_column_ptr()
                                 : block.get_by_position(arguments[0]).column;
        default_preprocess_parameter_columns(arg_col.data(),
                                             reinterpret_cast<const bool*>(is_const.data()),
                                             param_idx, block, arguments);

        // The second parameter's `is_const` is used to indicate whether all the parameters are constants.
        columns_info[0].is_const = is_const[0];
        columns_info[1].is_const = all_const;
        for (size_t i = 0; i < arguments.size(); ++i) {
            ColumnPtr col_ptr = arg_col[i];
            columns_info[i].type = block.get_by_position(arguments[i]).type;
            columns_info[i].column = col_ptr;

            if (const auto* nullable = check_and_get_column<ColumnNullable>(col_ptr.get())) {
                has_nullable = true;
                columns_info[i].nested_col = &nullable->get_nested_column();
                columns_info[i].null_map = &nullable->get_null_map_data();
            } else {
                columns_info[i].nested_col = col_ptr.get();
            }
        }
    }

    static void execute(const std::vector<ColumnWithConstAndNullMap>& columns_info,
                        ColumnInt32::MutablePtr& res_col, PaddedPODArray<UInt8>& res_null_map_data,
                        size_t input_rows_count) {
        res_col->resize(input_rows_count);
        const auto& compare_data =
                assert_cast<const ColumnInt64&>(*columns_info[0].nested_col).get_data();
        const size_t num_thresholds = columns_info.size() - 1;

        for (size_t row = 0; row < input_rows_count; ++row) {
            if (columns_info[0].is_null_at(row)) {
                res_col->get_data()[row] = -1;
                continue;
            }

            // Determine whether all the threshold columns are constant columns.
            size_t row_idx = columns_info[1].is_const ? 0 : row;
            auto compare_val = compare_data[index_check_const(row, columns_info[0].is_const)];
            std::vector<int64_t> thresholds;
            thresholds.reserve(num_thresholds);

            for (size_t i = 1; i < columns_info.size(); ++i) {
                const auto& th_col = assert_cast<const ColumnInt64&>(*columns_info[i].nested_col);
                thresholds.push_back(
                        columns_info[i].is_null_at(row_idx) ? 0 : th_col.get_data()[row_idx]);
            }

            auto it = std::upper_bound(thresholds.begin(), thresholds.end(), compare_val);
            res_col->get_data()[row] = static_cast<Int32>(it - thresholds.begin());
        }
    }
};

void register_function_interval(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionNeedsToHandleNull<FunctionIntervalImpl, TYPE_INT>>();
}

} // namespace doris::vectorized

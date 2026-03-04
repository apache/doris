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
#include "vec/utils/util.hpp"

namespace doris::vectorized {
class FunctionInterval : public IFunction {
public:
    static constexpr auto name = "interval";
    static FunctionPtr create() { return std::make_shared<FunctionInterval>(); }
    String get_name() const override { return name; }
    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }
    bool use_default_implementation_for_nulls() const override { return false; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt32>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res_col = ColumnInt32::create();

        const size_t arg_size = arguments.size();
        std::vector<ColumnPtr> arg_cols(arg_size);
        std::vector<uint8_t> is_const(arg_size);
        std::vector<size_t> param_idx(arg_size - 1);
        bool all_const = true;
        for (int i = 0; i < arg_size; ++i) {
            is_const[i] = is_column_const(*block.get_by_position(arguments[i]).column);
            if (i != 0) {
                param_idx[i - 1] = i;
                all_const = all_const & is_const[i];
            }
        }
        arg_cols[0] = is_const[0] ? assert_cast<const ColumnConst&>(
                                            *block.get_by_position(arguments[0]).column)
                                            .get_data_column_ptr()
                                  : block.get_by_position(arguments[0]).column;
        default_preprocess_parameter_columns(arg_cols.data(),
                                             reinterpret_cast<const bool*>(is_const.data()),
                                             param_idx, block, arguments);
        for (int i = 1; i < arg_size; ++i) {
            arg_cols[i] = remove_nullable(arg_cols[i]);
        }

        const NullMap* compare_null_map = VectorizedUtils::get_null_map(arg_cols[0]);
        arg_cols[0] = remove_nullable(arg_cols[0]);
        const auto& compare_data = assert_cast<const ColumnInt64&>(*arg_cols[0]).get_data();
        for (int row = 0; row < input_rows_count; ++row) {
            const size_t compare_idx = index_check_const(row, is_const[0]);
            const size_t arr_idx = all_const ? 0 : row;
            if (compare_null_map && (*compare_null_map)[compare_idx]) {
                res_col->insert_value(-1);
                continue;
            }

            res_col->insert_value(compute_interval(compare_data[compare_idx], arg_cols, is_const,
                                                   arr_idx, arg_size));
        }
        block.get_by_position(result).column = std::move(res_col);
        return Status::OK();
    }

private:
    int32_t compute_interval(int64_t compare_val, const std::vector<ColumnPtr>& arg_cols,
                             std::vector<uint8_t>& is_const, size_t row_idx,
                             size_t arg_size) const {
        size_t l = 1, r = arg_size;
        while (l < r) {
            size_t mid = (l + r) >> 1;
            const auto mid_val =
                    assert_cast<const ColumnInt64&>(*arg_cols[mid]).get_data()[row_idx];
            if (mid_val <= compare_val) {
                l = mid + 1;
            } else {
                r = mid;
            }
        }
        return static_cast<int32_t>(l - 1);
    }
};

void register_function_interval(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionInterval>();
}

} // namespace doris::vectorized

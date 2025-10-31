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

#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "vec/columns/column.h"
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
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionInterval : public IFunction {
public:
    static constexpr auto name = "interval";
    static FunctionPtr create() { return std::make_shared<FunctionInterval>(); }

    String get_name() const override { return name; }
    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }
    bool use_default_implementation_for_nulls() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return std::make_shared<DataTypeInt32>();
    }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        if (arguments.size() < 2) {
            return Status::InvalidArgument("interval requires at least 2 arguments");
        }

        auto res_col = ColumnInt32::create();
        auto& res_data = res_col->get_data();
        res_data.resize(input_rows_count);

        auto compare_cwn = block.get_by_position(arguments[0]);
        auto compare_col_ptr = compare_cwn.column;
        bool compare_is_const = false;
        std::tie(compare_col_ptr, compare_is_const) = unpack_if_const(compare_col_ptr);
        
        switch (compare_cwn.type->get_primitive_type()) {
        case PrimitiveType::TYPE_TINYINT:
            compute_interval<ColumnInt8>(block, arguments, *compare_col_ptr, compare_is_const,
                                         res_data);
            break;
        case PrimitiveType::TYPE_SMALLINT:
            compute_interval<ColumnInt16>(block, arguments, *compare_col_ptr, compare_is_const,
                                          res_data);
            break;
        case PrimitiveType::TYPE_INT:
            compute_interval<ColumnInt32>(block, arguments, *compare_col_ptr, compare_is_const,
                                          res_data);
            break;
        case PrimitiveType::TYPE_BIGINT:
            compute_interval<ColumnInt64>(block, arguments, *compare_col_ptr, compare_is_const,
                                          res_data);
            break;
        case PrimitiveType::TYPE_LARGEINT:
            compute_interval<ColumnInt128>(block, arguments, *compare_col_ptr, compare_is_const,
                                           res_data);
            break;
        default:
            return Status::InvalidArgument(
                    "interval only supports integer numeric types for the first argument");
        }

        block.replace_by_position(result, std::move(res_col));
        return Status::OK();
    }

private:
    template <typename ColType>
    static void compute_interval(Block& block, const ColumnNumbers& arguments, const IColumn& compare_col,
                                 bool compare_is_const, PaddedPODArray<Int32>& res) {
        const auto& compare_data = assert_cast<const ColType&>(compare_col).get_data();
        const size_t rows = res.size();
        const size_t num_thresholds = arguments.size() - 1;

        for (size_t row = 0; row < rows; ++row) {
            auto compare_val = compare_data[index_check_const(row, compare_is_const)];

            std::vector<typename ColType::value_type> thresholds;
            thresholds.reserve(num_thresholds);

            for (size_t i = 1; i < arguments.size(); ++i) {
                const auto& col_cwn = block.get_by_position(arguments[i]);
                ColumnPtr col_ptr = col_cwn.column;
                bool is_const = false;
                std::tie(col_ptr, is_const) = unpack_if_const(col_ptr);
                const auto& th_col = assert_cast<const ColType&>(*col_ptr);
                thresholds.push_back(th_col.get_data()[index_check_const(row, is_const)]);
            }

            size_t left = 0;
            size_t right = num_thresholds;
            size_t result_idx = num_thresholds;

            while (left < right) {
                size_t mid = left + (right - left) / 2;
                if (thresholds[mid] > compare_val) {
                    result_idx = mid;
                    right = mid;
                } else {
                    left = mid + 1;
                }
            }

            res[row] = static_cast<Int32>(result_idx);
        }
    }
};

void register_function_interval(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionInterval>();
}

} // namespace doris::vectorized

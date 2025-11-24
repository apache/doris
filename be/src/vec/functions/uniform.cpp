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

#include <boost/iterator/iterator_facade.hpp>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <random>
#include <utility>

#include "common/status.h"
#include "runtime/primitive_type.h"
#include "udf/udf.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

// Integer uniform implementation
struct UniformIntImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>(),
                std::make_shared<DataTypeInt64>()};
    }

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        return std::make_shared<DataTypeInt64>();
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto res_column = ColumnInt64::create(input_rows_count);
        auto& res_data = static_cast<ColumnInt64&>(*res_column).get_data();

        // Get min and max values (constants)
        const auto& left =
                assert_cast<const ColumnConst&>(*block.get_by_position(arguments[0]).column)
                        .get_data_column();
        const auto& right =
                assert_cast<const ColumnConst&>(*block.get_by_position(arguments[1]).column)
                        .get_data_column();
        Int64 min = assert_cast<const ColumnInt64&>(left).get_element(0);
        Int64 max = assert_cast<const ColumnInt64&>(right).get_element(0);

        if (min >= max) {
            return Status::InvalidArgument(
                    "uniform's min should be less than max, but got [{}, {})", min, max);
        }

        // Get gen column (seed values)
        const auto& gen_column = block.get_by_position(arguments[2]).column;

        for (int i = 0; i < input_rows_count; i++) {
            // Use gen value as seed for each row
            auto seed = (*gen_column)[i].get<Int64>();
            std::mt19937_64 generator(seed);
            std::uniform_int_distribution<int64_t> distribution(min, max);
            res_data[i] = distribution(generator);
        }

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

// Double uniform implementation
struct UniformDoubleImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>(),
                std::make_shared<DataTypeInt64>()};
    }

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        return std::make_shared<DataTypeFloat64>();
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto res_column = ColumnFloat64::create(input_rows_count);
        auto& res_data = static_cast<ColumnFloat64&>(*res_column).get_data();

        // Get min and max values (constants)
        const auto& left =
                assert_cast<const ColumnConst&>(*block.get_by_position(arguments[0]).column)
                        .get_data_column();
        const auto& right =
                assert_cast<const ColumnConst&>(*block.get_by_position(arguments[1]).column)
                        .get_data_column();
        double min = assert_cast<const ColumnFloat64&>(left).get_element(0);
        double max = assert_cast<const ColumnFloat64&>(right).get_element(0);

        if (min >= max) {
            return Status::InvalidArgument(
                    "uniform's min should be less than max, but got [{}, {})", min, max);
        }

        // Get gen column (seed values)
        const auto& gen_column = block.get_by_position(arguments[2]).column;

        for (int i = 0; i < input_rows_count; i++) {
            // Use gen value as seed for each row
            auto seed = (*gen_column)[i].get<Int64>();
            std::mt19937_64 generator(seed);
            std::uniform_real_distribution<double> distribution(min, max);
            res_data[i] = distribution(generator);
        }

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

template <typename Impl>
class FunctionUniform : public IFunction {
public:
    static constexpr auto name = "uniform";

    static FunctionPtr create() { return std::make_shared<FunctionUniform<Impl>>(); }
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        // init_function_context do set_constant_cols for FRAGMENT_LOCAL scope
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            if (!context->is_col_constant(0)) {
                return Status::InvalidArgument(
                        "The first parameter (min) of uniform function must be literal");
            }
            if (!context->is_col_constant(1)) {
                return Status::InvalidArgument(
                        "The second parameter (max) of uniform function must be literal");
            }
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

void register_function_uniform(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionUniform<UniformIntImpl>>();
    factory.register_function<FunctionUniform<UniformDoubleImpl>>();
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized

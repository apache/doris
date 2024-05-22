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

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <random>
#include <utility>

#include "common/status.h"
#include "udf/udf.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
class Random : public IFunction {
public:
    static constexpr auto name = "random";

    static FunctionPtr create() { return std::make_shared<Random>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_constants() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() == 2) {
            return std::make_shared<DataTypeInt64>();
        }
        return std::make_shared<DataTypeFloat64>();
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        std::shared_ptr<std::mt19937_64> generator(new std::mt19937_64());
        context->set_function_state(scope, generator);
        if (scope == FunctionContext::THREAD_LOCAL) {
            if (context->get_num_args() == 1) {
                // This is a call to RandSeed, initialize the seed
                if (!context->is_col_constant(0)) {
                    return Status::InvalidArgument("Seed argument to rand() must be constant.");
                }
                uint32_t seed = 0;
                if (!context->get_constant_col(0)->column_ptr->is_null_at(0)) {
                    seed = (*context->get_constant_col(0)->column_ptr)[0].get<int64_t>();
                }
                generator->seed(seed);
            } else {
                // 0 or 2 args
                generator->seed(std::random_device()());
            }
        }

        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        if (arguments.size() == 2) {
            return _execute_int_range(context, block, arguments, result, input_rows_count);
        }
        return _execute_float(context, block, arguments, result, input_rows_count);
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }

private:
    static Status _execute_int_range(FunctionContext* context, Block& block,
                                     const ColumnNumbers& arguments, size_t result,
                                     size_t input_rows_count) {
        auto res_column = ColumnInt64::create(input_rows_count);
        auto& res_data = static_cast<ColumnInt64&>(*res_column).get_data();

        auto* generator = reinterpret_cast<std::mt19937_64*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        DCHECK(generator != nullptr);

        Int64 min = assert_cast<const ColumnInt64*>(
                            assert_cast<const ColumnConst*>(
                                    block.get_by_position(arguments[0]).column.get())
                                    ->get_data_column_ptr()
                                    .get())
                            ->get_element(0);
        Int64 max = assert_cast<const ColumnInt64*>(
                            assert_cast<const ColumnConst*>(
                                    block.get_by_position(arguments[1]).column.get())
                                    ->get_data_column_ptr()
                                    .get())
                            ->get_element(0);
        if (min >= max) {
            return Status::InvalidArgument(fmt::format(
                    "random's lower bound should less than upper bound, but got [{}, {})", min,
                    max));
        }

        std::uniform_int_distribution<int64_t> distribution(min, max);
        for (int i = 0; i < input_rows_count; i++) {
            res_data[i] = distribution(*generator);
        }

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

    static Status _execute_float(FunctionContext* context, Block& block,
                                 const ColumnNumbers& arguments, size_t result,
                                 size_t input_rows_count) {
        static const double min = 0.0;
        static const double max = 1.0;
        auto res_column = ColumnFloat64::create(input_rows_count);
        auto& res_data = static_cast<ColumnFloat64&>(*res_column).get_data();

        auto* generator = reinterpret_cast<std::mt19937_64*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        DCHECK(generator != nullptr);

        std::uniform_real_distribution<double> distribution(min, max);
        for (int i = 0; i < input_rows_count; i++) {
            res_data[i] = distribution(*generator);
        }

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

void register_function_random(SimpleFunctionFactory& factory) {
    factory.register_function<Random>();
    factory.register_alias(Random::name, "rand");
}

} // namespace doris::vectorized

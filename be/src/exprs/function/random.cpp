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
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"

namespace doris {

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
        std::shared_ptr<State> state = std::make_shared<State>();
        context->set_function_state(scope, state);
        if (scope == FunctionContext::THREAD_LOCAL) {
            if (context->get_num_args() == 1) {
                // This is a call to RandSeed, initialize the seed. The seed is a constant, but a
                // constant expression such as an arithmetic one is not evaluated in open. Then the
                // generator is seeded with the first row in execute.
                if (context->is_col_constant(0)) {
                    state->generator.seed(get_seed(*context->get_constant_col(0)->column_ptr));
                } else {
                    state->seed_in_execute = true;
                }
            } else { // zero args, or the range bounds
                state->generator.seed(std::random_device()());
            }
        }

        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        if (arguments.size() == 2) {
            return _execute_int_range(context, block, arguments, result, input_rows_count);
        }
        return _execute_float(context, block, arguments, result, input_rows_count);
    }

private:
    struct State {
        std::mt19937_64 generator;
        bool seed_in_execute = false;
    };

    static uint32_t get_seed(const IColumn& seed_column) {
        return seed_column.is_null_at(0) ? 0 : (uint32_t)seed_column[0].get<TYPE_BIGINT>();
    }

    static Status _execute_int_range(FunctionContext* context, Block& block,
                                     const ColumnNumbers& arguments, uint32_t result,
                                     size_t input_rows_count) {
        auto res_column = ColumnInt64::create(input_rows_count);
        auto& res_data = res_column->get_data();

        auto* state = reinterpret_cast<State*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        DCHECK(state != nullptr);
        if (input_rows_count == 0) {
            block.replace_by_position(result, std::move(res_column));
            return Status::OK();
        }

        // The bounds are constants checked in FE, so the first row holds their values. A constant
        // expression such as an arithmetic one is not a ColumnConst.
        Int64 min = block.get_by_position(arguments[0]).column->get_int(0);
        Int64 max = block.get_by_position(arguments[1]).column->get_int(0);
        if (min >= max) {
            return Status::InvalidArgument(fmt::format(
                    "random's lower bound should less than upper bound, but got [{}, {})", min,
                    max));
        }

        std::uniform_int_distribution<int64_t> distribution(min, max);
        for (int i = 0; i < input_rows_count; i++) {
            res_data[i] = distribution(state->generator);
        }

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

    static Status _execute_float(FunctionContext* context, Block& block,
                                 const ColumnNumbers& arguments, uint32_t result,
                                 size_t input_rows_count) {
        static const double min = 0.0;
        static const double max = 1.0;
        auto res_column = ColumnFloat64::create(input_rows_count);
        auto& res_data = res_column->get_data();

        auto* state = reinterpret_cast<State*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        DCHECK(state != nullptr);
        if (state->seed_in_execute && input_rows_count > 0) {
            state->generator.seed(get_seed(*block.get_by_position(arguments[0]).column));
            state->seed_in_execute = false;
        }

        std::uniform_real_distribution<double> distribution(min, max);
        for (int i = 0; i < input_rows_count; i++) {
            res_data[i] = distribution(state->generator);
        }

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

void register_function_random(SimpleFunctionFactory& factory) {
    factory.register_function<Random>();
    factory.register_alias(Random::name, "rand");
}
} // namespace doris

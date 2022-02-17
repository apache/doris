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

#include <random>

#include "udf/udf.h"
#include "vec/data_types/data_type_number.h"
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
        return std::make_shared<DataTypeFloat64>();
    }

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        std::mt19937_64* generator =
                reinterpret_cast<std::mt19937_64*>(context->allocate(sizeof(std::mt19937_64)));
        if (UNLIKELY(generator == nullptr)) {
            return Status::MemoryAllocFailed("allocate random seed generator failed.");
        }

        context->set_function_state(scope, generator);
        new (generator) std::mt19937_64();
        if (scope == FunctionContext::THREAD_LOCAL) {
            if (context->get_num_args() == 1) {
                // This is a call to RandSeed, initialize the seed
                // TODO: should we support non-constant seed?
                if (!context->is_col_constant(0)) {
                    return Status::InvalidArgument("Seed argument to rand() must be constant.");
                }
                uint32_t seed = 0;
                if (!context->get_constant_col(0)->column_ptr->is_null_at(0)) {
                    seed = context->get_constant_col(0)->column_ptr->get64(0);
                }
                generator->seed(seed);
            } else {
                generator->seed(std::random_device()());
            }
        }

        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        static const double min = 0.0;
        static const double max = 1.0;
        auto res_column = ColumnFloat64::create(input_rows_count);
        auto& res_data = assert_cast<ColumnFloat64&>(*res_column).get_data();

        std::mt19937_64* generator = reinterpret_cast<std::mt19937_64*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        DCHECK(generator != nullptr);

        std::uniform_real_distribution<double> distribution(min, max);
        for (int i = 0; i < input_rows_count; i++) {
            res_data[i] = distribution(*generator);
        }

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            uint8_t* generator = reinterpret_cast<uint8_t*>(
                    context->get_function_state(FunctionContext::THREAD_LOCAL));
            context->free(generator);
            context->set_function_state(FunctionContext::THREAD_LOCAL, nullptr);
        }
        return Status::OK();
    }
};

void register_function_random(SimpleFunctionFactory& factory) {
    factory.register_function<Random>();
    factory.register_alias(Random::name, "rand");
}

} // namespace doris::vectorized

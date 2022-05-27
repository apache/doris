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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionNull.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_null.h"

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_combinator.h"
#include "vec/aggregate_functions/aggregate_function_count.h"
#include "vec/aggregate_functions/aggregate_function_nothing.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

class AggregateFunctionCombinatorNull final : public IAggregateFunctionCombinator {
public:
    String get_name() const override { return "Null"; }

    bool is_for_internal_usage_only() const override { return true; }

    DataTypes transform_arguments(const DataTypes& arguments) const override {
        size_t size = arguments.size();
        DataTypes res(size);
        for (size_t i = 0; i < size; ++i) {
            res[i] = remove_nullable(arguments[i]);
        }
        return res;
    }

    AggregateFunctionPtr transform_aggregate_function(
            const AggregateFunctionPtr& nested_function, const DataTypes& arguments,
            const Array& params, const bool result_is_nullable) const override {
        if (nested_function == nullptr) {
            return nullptr;
        }

        bool has_null_types = false;
        for (const auto& arg_type : arguments) {
            if (arg_type->only_null()) {
                has_null_types = true;
                break;
            }
        }

        if (has_null_types) {
            return std::make_shared<AggregateFunctionNothing>(arguments, params);
        }

        if (arguments.size() == 1) {
            if (result_is_nullable) {
                return std::make_shared<AggregateFunctionNullUnary<true>>(nested_function,
                                                                          arguments, params);
            } else {
                return std::make_shared<AggregateFunctionNullUnary<false>>(nested_function,
                                                                           arguments, params);
            }
        } else {
            if (result_is_nullable) {
                return std::make_shared<AggregateFunctionNullVariadic<true>>(nested_function,
                                                                             arguments, params);
            } else {
                return std::make_shared<AggregateFunctionNullVariadic<false>>(nested_function,
                                                                              arguments, params);
            }
        }
    }
};

void register_aggregate_function_combinator_null(AggregateFunctionSimpleFactory& factory) {
    // factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorNull>());
    AggregateFunctionCreator creator = [&](const std::string& name, const DataTypes& types,
                                           const Array& params, const bool result_is_nullable) {
        auto function_combinator = std::make_shared<AggregateFunctionCombinatorNull>();
        auto transform_arguments = function_combinator->transform_arguments(types);
        auto nested_function = factory.get(name, transform_arguments, params);
        return function_combinator->transform_aggregate_function(nested_function, types, params,
                                                                 result_is_nullable);
    };
    factory.register_nullable_function_combinator(creator);
}

} // namespace doris::vectorized

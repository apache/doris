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
#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_regr_intercept.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

template <typename T>
AggregateFunctionPtr type_dispatch_for_aggregate_function_regr_intercept(const DataTypes& argument_types,
                                                                         const bool& result_is_nullable,
                                                                         bool nullable_input) {
    using StatFunctionTemplate = RegrInterceptFuncTwoArg<T>;
    if (nullable_input) {
        return creator_without_type::create_ignore_nullable<
                AggregateFunctionRegrInterceptSimple<StatFunctionTemplate, true>>(
                argument_types, result_is_nullable);
    } else {
        return creator_without_type::create_ignore_nullable<
                AggregateFunctionRegrInterceptSimple<StatFunctionTemplate, false>>(
                argument_types, result_is_nullable);
    }
}

AggregateFunctionPtr create_aggregate_function_regr_intercept(const std::string& name,
                                                              const DataTypes& argument_types,
                                                              const bool result_is_nullable) {
    if (argument_types.size() != 2) {
        LOG(WARNING) << "aggregate function " << name << " requires exactly 2 arguments";
        return nullptr;
    }
    if (!result_is_nullable) {
        LOG(WARNING) << "aggregate function " << name << " requires nullable result type";
        return nullptr;
    }
    const bool nullable_input = argument_types[0]->is_nullable() || argument_types[1]->is_nullable();
    WhichDataType y_type(remove_nullable(argument_types[0]));
    WhichDataType x_type(remove_nullable(argument_types[1]));

#define DISPATCH(T)                                                                                   \
    if (x_type.idx == TypeIndex::T && y_type.idx == TypeIndex::T)                                        \
        return type_dispatch_for_aggregate_function_regr_intercept<T>(argument_types, result_is_nullable, \
                                                                           nullable_input);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    LOG(WARNING) << "Unsupported input types " << argument_types[0]->get_name()
                 << " and " << argument_types[1]->get_name()
                 << " for aggregate function " << name;
    return nullptr;
}

void register_aggregate_function_regr_intercept(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("regr_intercept", create_aggregate_function_regr_intercept);
}

} // namespace doris::vectorized
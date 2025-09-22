// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/aggregate_functions/aggregate_function_regr_union.h"

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <template <PrimitiveType> class StatFunctionTemplate>
AggregateFunctionPtr create_aggregate_function_regr(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const bool result_is_nullable,
                                                    const AggregateFunctionAttr& attr) {
    bool y_nullable_input = argument_types[0]->is_nullable();
    bool x_nullable_input = argument_types[1]->is_nullable();
    if (y_nullable_input) {
        if (x_nullable_input) {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegrSimple<StatFunctionTemplate<TYPE_DOUBLE>, true, true>>(
                    argument_types, result_is_nullable, attr);
        } else {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegrSimple<StatFunctionTemplate<TYPE_DOUBLE>, true, false>>(
                    argument_types, result_is_nullable, attr);
        }
    } else {
        if (x_nullable_input) {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegrSimple<StatFunctionTemplate<TYPE_DOUBLE>, false, true>>(
                    argument_types, result_is_nullable, attr);
        } else {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegrSimple<StatFunctionTemplate<TYPE_DOUBLE>, false, false>>(
                    argument_types, result_is_nullable, attr);
        }
    }
}

void register_aggregate_function_regr_union(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("regr_slope", create_aggregate_function_regr<RegrSlopeFunc>);
    factory.register_function_both("regr_intercept",
                                   create_aggregate_function_regr<RegrInterceptFunc>);
}
} // namespace doris::vectorized
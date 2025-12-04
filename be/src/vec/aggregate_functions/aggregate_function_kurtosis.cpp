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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_statistic.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

AggregateFunctionPtr create_aggregate_function_kurt(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const bool result_is_nullable,
                                                    const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 1, 1);
    if (!result_is_nullable) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Aggregate function {} requires result_is_nullable", name);
    }

    const bool nullable_input = argument_types[0]->is_nullable();
    using StatFunctionTemplate = StatFuncOneArg<TYPE_DOUBLE, 4>;

    if (nullable_input) {
        return creator_without_type::create_ignore_nullable<
                AggregateFunctionVarianceSimple<StatFunctionTemplate, true>>(
                argument_types, result_is_nullable, attr, STATISTICS_FUNCTION_KIND::KURT_POP);
    } else {
        return creator_without_type::create_ignore_nullable<
                AggregateFunctionVarianceSimple<StatFunctionTemplate, false>>(
                argument_types, result_is_nullable, attr, STATISTICS_FUNCTION_KIND::KURT_POP);
    }
}

void register_aggregate_function_kurtosis(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("kurt", create_aggregate_function_kurt);
    factory.register_alias("kurt", "kurt_pop");
    factory.register_alias("kurt", "kurtosis");
}

} // namespace doris::vectorized
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

#include "vec/aggregate_functions/aggregate_function_percentile.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/core/types.h"

namespace doris::vectorized {

template <bool is_nullable>
AggregateFunctionPtr create_aggregate_function_percentile_approx(const std::string& name,
                                                                 const DataTypes& argument_types,
                                                                 const bool result_is_nullable) {
    const DataTypePtr& argument_type = remove_nullable(argument_types[0]);
    WhichDataType which(argument_type);
    if (which.idx != TypeIndex::Float64) {
        return nullptr;
    }
    if (argument_types.size() == 2) {
        return creator_without_type::create<
                AggregateFunctionPercentileApproxTwoParams<is_nullable>>(
                remove_nullable(argument_types), result_is_nullable);
    }
    if (argument_types.size() == 3) {
        return creator_without_type::create<
                AggregateFunctionPercentileApproxThreeParams<is_nullable>>(
                remove_nullable(argument_types), result_is_nullable);
    }
    return nullptr;
}

template <bool is_nullable>
AggregateFunctionPtr create_aggregate_function_percentile_approx_weighted(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable) {
    const DataTypePtr& argument_type = remove_nullable(argument_types[0]);
    WhichDataType which(argument_type);
    if (which.idx != TypeIndex::Float64) {
        return nullptr;
    }
    if (argument_types.size() == 3) {
        return creator_without_type::create<
                AggregateFunctionPercentileApproxWeightedThreeParams<is_nullable>>(
                remove_nullable(argument_types), result_is_nullable);
    }
    if (argument_types.size() == 4) {
        return creator_without_type::create<
                AggregateFunctionPercentileApproxWeightedFourParams<is_nullable>>(
                remove_nullable(argument_types), result_is_nullable);
    }
    return nullptr;
}

void register_aggregate_function_percentile(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("percentile",
                                   creator_with_integer_type::creator<AggregateFunctionPercentile>);
    factory.register_function_both(
            "percentile_array",
            creator_with_integer_type::creator<AggregateFunctionPercentileArray>);
}

void register_aggregate_function_percentile_approx(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("percentile_approx",
                              create_aggregate_function_percentile_approx<false>, false);
    factory.register_function("percentile_approx",
                              create_aggregate_function_percentile_approx<true>, true);
    factory.register_function("percentile_approx_weighted",
                              create_aggregate_function_percentile_approx_weighted<false>, false);
    factory.register_function("percentile_approx_weighted",
                              create_aggregate_function_percentile_approx_weighted<true>, true);
}
} // namespace doris::vectorized
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

#include "exprs/aggregate/aggregate_function_percentile.h"

#include "core/types.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

AggregateFunctionPtr create_aggregate_function_percentile_approx(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr) {
    const DataTypePtr& argument_type = remove_nullable(argument_types[0]);
    if (argument_type->get_primitive_type() != PrimitiveType::TYPE_DOUBLE) {
        return nullptr;
    }
    if (argument_types.size() == 2) {
        return creator_without_type::create<AggregateFunctionPercentileApproxTwoParams>(
                argument_types, result_is_nullable, attr);
    }
    if (argument_types.size() == 3) {
        return creator_without_type::create<AggregateFunctionPercentileApproxThreeParams>(
                argument_types, result_is_nullable, attr);
    }
    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_percentile_approx_weighted(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr) {
    const DataTypePtr& argument_type = remove_nullable(argument_types[0]);
    if (argument_type->get_primitive_type() != PrimitiveType::TYPE_DOUBLE) {
        return nullptr;
    }
    if (argument_types.size() == 3) {
        return creator_without_type::create<AggregateFunctionPercentileApproxWeightedThreeParams>(
                argument_types, result_is_nullable, attr);
    }
    if (argument_types.size() == 4) {
        return creator_without_type::create<AggregateFunctionPercentileApproxWeightedFourParams>(
                argument_types, result_is_nullable, attr);
    }
    return nullptr;
}

void register_aggregate_function_percentile(AggregateFunctionSimpleFactory& factory) {
    using creator = creator_with_type_list<TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT,
                                           TYPE_LARGEINT, TYPE_FLOAT, TYPE_DOUBLE>;
    factory.register_function_both("percentile", creator::creator<AggregateFunctionPercentile>);
    factory.register_function_both("percentile_array",
                                   creator::creator<AggregateFunctionPercentileArray>);
    factory.register_function_both("percentile_v2",
                                   creator::creator<AggregateFunctionPercentileV2>);
    factory.register_function_both("percentile_array_v2",
                                   creator::creator<AggregateFunctionPercentileArrayV2>);
    factory.register_alias("percentile", "percentile_cont");
}

void register_percentile_approx_old_function(AggregateFunctionSimpleFactory& factory) {
    BeExecVersionManager::registe_restrict_function_compatibility("percentile_approx");
    BeExecVersionManager::registe_restrict_function_compatibility("percentile_approx_weighted");
}

void register_aggregate_function_percentile_old(AggregateFunctionSimpleFactory& factory) {
    BeExecVersionManager::registe_restrict_function_compatibility("percentile");
    BeExecVersionManager::registe_restrict_function_compatibility("percentile_array");
}

void register_aggregate_function_percentile_approx(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("percentile_approx",
                                   create_aggregate_function_percentile_approx);
    factory.register_function_both("percentile_approx_weighted",
                                   create_aggregate_function_percentile_approx_weighted);

    register_percentile_approx_old_function(factory);
}

} // namespace doris

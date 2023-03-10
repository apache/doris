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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionMinMaxAny.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_min_max.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
/// min, max, any
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data>
static IAggregateFunction* create_aggregate_function_single_value(const String& name,
                                                                  const DataTypes& argument_types,
                                                                  const bool result_is_nullable) {
    assert_unary(name, argument_types);

    IAggregateFunction* res(creator_with_numeric_type::create<AggregateFunctionTemplate, Data,
                                                              SingleValueDataFixed>(
            result_is_nullable, argument_types));
    if (res) {
        return res;
    }
    res = creator_with_decimal_type::create<AggregateFunctionTemplate, Data,
                                            SingleValueDataDecimal>(result_is_nullable,
                                                                    argument_types);
    if (res) {
        return res;
    }
    const DataTypePtr& argument_type = remove_nullable(argument_types[0]);
    WhichDataType which(argument_type);

    if (which.idx == TypeIndex::String) {
        return creator_without_type::create<AggregateFunctionTemplate<Data<SingleValueDataString>>>(
                result_is_nullable, argument_types);
    }
    if (which.idx == TypeIndex::DateTime || which.idx == TypeIndex::Date) {
        return creator_without_type::create<
                AggregateFunctionTemplate<Data<SingleValueDataFixed<Int64>>>>(result_is_nullable,
                                                                              argument_types);
    }
    if (which.idx == TypeIndex::DateV2) {
        return creator_without_type::create<
                AggregateFunctionTemplate<Data<SingleValueDataFixed<UInt32>>>>(result_is_nullable,
                                                                               argument_types);
    }
    if (which.idx == TypeIndex::DateTimeV2) {
        return creator_without_type::create<
                AggregateFunctionTemplate<Data<SingleValueDataFixed<UInt64>>>>(result_is_nullable,
                                                                               argument_types);
    }
    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_max(const std::string& name,
                                                   const DataTypes& argument_types,
                                                   const bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_aggregate_function_single_value<AggregateFunctionsSingleValue,
                                                   AggregateFunctionMaxData>(name, argument_types,
                                                                             result_is_nullable));
}

AggregateFunctionPtr create_aggregate_function_min(const std::string& name,
                                                   const DataTypes& argument_types,
                                                   const bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_aggregate_function_single_value<AggregateFunctionsSingleValue,
                                                   AggregateFunctionMinData>(name, argument_types,
                                                                             result_is_nullable));
}

AggregateFunctionPtr create_aggregate_function_any(const std::string& name,
                                                   const DataTypes& argument_types,
                                                   const bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_aggregate_function_single_value<AggregateFunctionsSingleValue,
                                                   AggregateFunctionAnyData>(name, argument_types,
                                                                             result_is_nullable));
}

void register_aggregate_function_minmax(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("max", create_aggregate_function_max);
    factory.register_function_both("min", create_aggregate_function_min);
    factory.register_function_both("any", create_aggregate_function_any);
    factory.register_alias("any", "any_value");
}

} // namespace doris::vectorized

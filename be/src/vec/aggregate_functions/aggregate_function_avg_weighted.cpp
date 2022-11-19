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

#include "vec/aggregate_functions/aggregate_function_avg_weighted.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_avg_weight(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const Array& parameters,
                                                          const bool result_is_nullable) {
    auto type = argument_types[0].get();
    if (type->is_nullable()) {
        type = assert_cast<const DataTypeNullable*>(type)->get_nested_type().get();
    }

    WhichDataType which(*type);

#define DISPATCH(TYPE)                \
    if (which.idx == TypeIndex::TYPE) \
        return AggregateFunctionPtr(new AggregateFunctionAvgWeight<TYPE>(argument_types));
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.is_decimal128()) {
        return AggregateFunctionPtr(new AggregateFunctionAvgWeight<Decimal128>(argument_types));
    }
    if (which.is_decimal()) {
        return AggregateFunctionPtr(new AggregateFunctionAvgWeight<Decimal128I>(argument_types));
    }

    LOG(WARNING) << fmt::format("Illegal argument  type for aggregate function topn_array is: {}",
                                type->get_name());
    return nullptr;
}

void register_aggregate_function_avg_weighted(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("avg_weighted", create_aggregate_function_avg_weight);
}
} // namespace doris::vectorized

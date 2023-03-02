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
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_avg_weight(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const bool result_is_nullable) {
    WhichDataType which(remove_nullable(argument_types[0]));

#define DISPATCH(TYPE)                \
    if (which.idx == TypeIndex::TYPE) \
        return AggregateFunctionPtr(new AggregateFunctionAvgWeight<TYPE>(argument_types));
    FOR_NUMERIC_TYPES(DISPATCH)
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH

    LOG(WARNING) << fmt::format("Illegal argument  type for aggregate function topn_array is: {}",
                                argument_types[0]->get_name());
    return nullptr;
}

void register_aggregate_function_avg_weighted(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("avg_weighted", create_aggregate_function_avg_weight);
}
} // namespace doris::vectorized

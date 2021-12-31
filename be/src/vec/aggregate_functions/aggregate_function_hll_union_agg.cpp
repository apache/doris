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

#include "vec/aggregate_functions/aggregate_function_hll_union_agg.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"

namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_HLL_union_agg(const std::string& name,
                                                             const DataTypes& argument_types,
                                                             const Array& parameters,
                                                             const bool result_is_nullable) {
    assert_no_parameters(name, parameters);
    assert_arity_at_most<1>(name, argument_types);

    return std::make_shared<AggregateFunctionHLLUnionAgg>(argument_types);
}

AggregateFunctionPtr create_aggregate_function_HLL_union(const std::string& name,
                                                         const DataTypes& argument_types,
                                                         const Array& parameters,
                                                         const bool result_is_nullable) {
    assert_no_parameters(name, parameters);
    assert_arity_at_most<1>(name, argument_types);

    return std::make_shared<AggregateFunctionHLLUnion>(argument_types);
}

void register_aggregate_function_HLL_union_agg(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("hll_union_agg", create_aggregate_function_HLL_union_agg);
    factory.register_function("hll_union", create_aggregate_function_HLL_union);
    factory.register_function("hll_raw_agg", create_aggregate_function_HLL_union);
}

} // namespace doris::vectorized

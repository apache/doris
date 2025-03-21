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

#include "vec/aggregate_functions/aggregate_function_quantile_state.h"

#include <algorithm>

#include "vec/aggregate_functions//aggregate_function_simple_factory.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

AggregateFunctionPtr create_aggregate_function_quantile_state_union(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    const bool arg_is_nullable = argument_types[0]->is_nullable();
    if (arg_is_nullable) {
        return std::make_shared<
                AggregateFunctionQuantileStateOp<true, AggregateFunctionQuantileStateUnionOp>>(
                argument_types);
    } else {
        return std::make_shared<
                AggregateFunctionQuantileStateOp<false, AggregateFunctionQuantileStateUnionOp>>(
                argument_types);
    }
}

void register_aggregate_function_quantile_state(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("quantile_union",
                                   create_aggregate_function_quantile_state_union);
}

} // namespace doris::vectorized
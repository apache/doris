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

#include "exprs/aggregate/aggregate_function_collect.h"

#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/factory_helpers.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

// Forward declarations — template instantiations live in separate TUs
AggregateFunctionPtr create_aggregate_function_collect_no_limit(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const bool result_is_nullable,
                                                                const AggregateFunctionAttr& attr);
AggregateFunctionPtr create_aggregate_function_collect_with_limit(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr);

AggregateFunctionPtr create_aggregate_function_collect(const std::string& name,
                                                       const DataTypes& argument_types,
                                                       const DataTypePtr& result_type,
                                                       const bool result_is_nullable,
                                                       const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 1, 2);
    if (argument_types.size() == 1) {
        return create_aggregate_function_collect_no_limit(name, argument_types, result_is_nullable,
                                                          attr);
    }
    if (argument_types.size() == 2) {
        return create_aggregate_function_collect_with_limit(name, argument_types,
                                                            result_is_nullable, attr);
    }
    return nullptr;
}

void register_aggregate_function_collect_list(AggregateFunctionSimpleFactory& factory) {
    // notice: array_agg only differs from collect_list in that array_agg will show null elements in array
    factory.register_function_both("collect_list", create_aggregate_function_collect);
    factory.register_function_both("collect_set", create_aggregate_function_collect);
    factory.register_alias("collect_list", "group_array");
    factory.register_alias("collect_set", "group_uniq_array");
}
} // namespace doris
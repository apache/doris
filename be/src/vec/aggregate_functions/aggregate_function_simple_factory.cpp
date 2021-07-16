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

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"

namespace doris::vectorized {
class AggregateFunctionSimpleFactory;
void register_aggregate_function_sum(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_combinator_null(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_minmax(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_avg(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_count(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_HLL_union_agg(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_uniq(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_combinator_distinct(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_bitmap(AggregateFunctionSimpleFactory& factory);

AggregateFunctionSimpleFactory& AggregateFunctionSimpleFactory::instance() {
    static std::once_flag oc;
    static AggregateFunctionSimpleFactory instance;
    std::call_once(oc, [&]() {
        register_aggregate_function_sum(instance);
        register_aggregate_function_minmax(instance);
        register_aggregate_function_avg(instance);
        register_aggregate_function_count(instance);
        register_aggregate_function_uniq(instance);
        register_aggregate_function_bitmap(instance);
        register_aggregate_function_combinator_distinct(instance);
        register_aggregate_function_HLL_union_agg(instance);
        register_aggregate_function_combinator_null(instance);
    });
    return instance;
}

} // namespace doris::vectorized
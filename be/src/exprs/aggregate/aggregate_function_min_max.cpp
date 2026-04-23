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

#include "exprs/aggregate/aggregate_function_min_max.h"

#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

// Template definition lives in aggregate_function_min_max_impl.h
// Explicit instantiations provided by:
//   aggregate_function_min_max_max.cpp  (AggregateFunctionMaxData)
//   aggregate_function_min_max_min.cpp  (AggregateFunctionMinData)
//   aggregate_function_min_max_any.cpp  (AggregateFunctionAnyData)

void register_aggregate_function_minmax(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both(
            "max", create_aggregate_function_single_value<AggregateFunctionMaxData>);
    factory.register_function_both(
            "min", create_aggregate_function_single_value<AggregateFunctionMinData>);
    factory.register_function_both(
            "any", create_aggregate_function_single_value<AggregateFunctionAnyData>);
    factory.register_alias("any", "any_value");
}

} // namespace doris

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

#include "vec/aggregate_functions/aggregate_function_percentile_approx.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

void register_aggregate_function_percentile_old(AggregateFunctionSimpleFactory& factory) {
    factory.register_alternative_function(
            "percentile", creator_without_type::creator<AggregateFunctionPercentileOld>);
    factory.register_alternative_function(
            "percentile", creator_without_type::creator<AggregateFunctionPercentileOld>, true);
    factory.register_alternative_function(
            "percentile_array", creator_without_type::creator<AggregateFunctionPercentileArrayOld>);
    factory.register_alternative_function(
            "percentile_array", creator_without_type::creator<AggregateFunctionPercentileArrayOld>,
            true);
}
} // namespace doris::vectorized
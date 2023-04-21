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

#include <algorithm>

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

void register_aggregate_function_HLL_union_agg(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both(
            "hll_union_agg", creator_without_type::creator<AggregateFunctionHLLUnion<
                                     AggregateFunctionHLLUnionAggImpl<AggregateFunctionHLLData>>>);

    factory.register_function_both(
            "hll_union", creator_without_type::creator<AggregateFunctionHLLUnion<
                                 AggregateFunctionHLLUnionImpl<AggregateFunctionHLLData>>>);
    factory.register_alias("hll_union", "hll_raw_agg");
}

} // namespace doris::vectorized

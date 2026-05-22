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

#include "exprs/aggregate/aggregate_function_reader.h"

#include <algorithm>
#include <string>

#include "exprs/aggregate/aggregate_function_bitmap.h"
#include "exprs/aggregate/aggregate_function_hll_union_agg.h"
#include "exprs/aggregate/aggregate_function_min_max.h"
#include "exprs/aggregate/aggregate_function_quantile_state.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/aggregate_function_sum.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

// auto spread at nullable condition, null value do not participate aggregate
void register_aggregate_function_reader_load(AggregateFunctionSimpleFactory& factory) {
    // add a suffix to the function name here to distinguish special functions of agg reader
    auto register_function_both = [&](const std::string& name,
                                      const AggregateFunctionCreator& creator) {
        factory.register_function_both(name + AGG_READER_SUFFIX, creator);
        factory.register_function_both(name + AGG_LOAD_SUFFIX, creator);
    };

    register_function_both(
            "sum",
            creator_with_type_list<TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT,
                                   TYPE_LARGEINT, TYPE_FLOAT, TYPE_DOUBLE, TYPE_DECIMAL32,
                                   TYPE_DECIMAL64, TYPE_DECIMAL128I, TYPE_DECIMAL256,
                                   TYPE_DECIMALV2>::creator<AggregateFunctionSumSimpleReader>);
    register_function_both("max", create_aggregate_function_single_value<AggregateFunctionMaxData>);
    register_function_both("min", create_aggregate_function_single_value<AggregateFunctionMinData>);
    register_function_both("bitmap_union",
                           creator_without_type::creator<
                                   AggregateFunctionBitmapOp<AggregateFunctionBitmapUnionOp>>);
    register_function_both("hll_union",
                           creator_without_type::creator<AggregateFunctionHLLUnion<
                                   AggregateFunctionHLLUnionImpl<AggregateFunctionHLLData>>>);
    register_function_both("quantile_union", create_aggregate_function_quantile_state_union);
}

} // namespace doris
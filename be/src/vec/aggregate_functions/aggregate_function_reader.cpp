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

#include "vec/aggregate_functions/aggregate_function_reader.h"

#include <algorithm>
#include <string>

#include "vec/aggregate_functions/aggregate_function_bitmap.h"
#include "vec/aggregate_functions/aggregate_function_hll_union_agg.h"
#include "vec/aggregate_functions/aggregate_function_min_max.h"
#include "vec/aggregate_functions/aggregate_function_quantile_state.h"
#include "vec/aggregate_functions/aggregate_function_reader_first_last.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_sum.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

// auto spread at nullable condition, null value do not participate aggregate
void register_aggregate_function_reader_load(AggregateFunctionSimpleFactory& factory) {
    // add a suffix to the function name here to distinguish special functions of agg reader
    auto register_function_both = [&](const std::string& name,
                                      const AggregateFunctionCreator& creator) {
        factory.register_function_both(name + AGG_READER_SUFFIX, creator);
        factory.register_function_both(name + AGG_LOAD_SUFFIX, creator);
    };

    register_function_both("sum", creator_with_type::creator<AggregateFunctionSumSimpleReader>);
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

// only replace function in load/reader do different agg operation.
// because Doris can ensure that the data is globally ordered in reader, but cannot in load
// 1. reader, get the first value of input data.
// 2. load, get the last value of input data.
void register_aggregate_function_replace_reader_load(AggregateFunctionSimpleFactory& factory) {
    auto register_function = [&](const std::string& name, const std::string& suffix,
                                 const AggregateFunctionCreator& creator, bool nullable) {
        factory.register_function(name + suffix, creator, nullable);
    };

    register_function("replace", AGG_READER_SUFFIX, create_aggregate_function_first<true>, false);
    register_function("replace", AGG_READER_SUFFIX, create_aggregate_function_first<true>, true);
    register_function("replace", AGG_LOAD_SUFFIX, create_aggregate_function_last<false>, false);
    register_function("replace", AGG_LOAD_SUFFIX, create_aggregate_function_last<false>, true);

    register_function("replace_if_not_null", AGG_READER_SUFFIX,
                      create_aggregate_function_first_non_null_value<true>, false);
    register_function("replace_if_not_null", AGG_READER_SUFFIX,
                      create_aggregate_function_first_non_null_value<true>, true);
    register_function("replace_if_not_null", AGG_LOAD_SUFFIX,
                      create_aggregate_function_last_non_null_value<false>, false);
    register_function("replace_if_not_null", AGG_LOAD_SUFFIX,
                      create_aggregate_function_last_non_null_value<false>, true);
}

} // namespace doris::vectorized
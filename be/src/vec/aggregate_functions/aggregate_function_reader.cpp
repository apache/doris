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

namespace doris::vectorized {

// auto spread at nullable condition, null value do not participate aggregate
void register_aggregate_function_reader_load(AggregateFunctionSimpleFactory& factory) {
    // add a suffix to the function name here to distinguish special functions of agg reader
    auto register_function = [&](const std::string& name, const AggregateFunctionCreator& creator) {
        factory.register_function(name + AGG_READER_SUFFIX, creator, false);
        factory.register_function(name + AGG_LOAD_SUFFIX, creator, false);
    };

    register_function("sum", create_aggregate_function_sum_reader);
    register_function("max", create_aggregate_function_max);
    register_function("min", create_aggregate_function_min);
    register_function("bitmap_union", create_aggregate_function_bitmap_union);
    register_function("hll_union", create_aggregate_function_HLL_union<false>);
}

// only replace funtion in load/reader do different agg operation.
// because Doris can ensure that the data is globally ordered in reader, but cannot in load
// 1. reader, get the first value of input data.
// 2. load, get the last value of input data.
void register_aggregate_function_replace_reader_load(AggregateFunctionSimpleFactory& factory) {
    auto register_function = [&](const std::string& name, const std::string& suffix,
                                 const AggregateFunctionCreator& creator, bool nullable) {
        factory.register_function(name + suffix, creator, nullable);
    };

    register_function("replace", AGG_READER_SUFFIX, create_aggregate_function_first<false, true>,
                      false);
    register_function("replace", AGG_READER_SUFFIX, create_aggregate_function_first<true, true>,
                      true);
    register_function("replace", AGG_LOAD_SUFFIX, create_aggregate_function_last<false, true>,
                      false);
    register_function("replace", AGG_LOAD_SUFFIX, create_aggregate_function_last<true, true>, true);

    register_function("replace_if_not_null", AGG_READER_SUFFIX,
                      create_aggregate_function_first<false, true>, false);
    register_function("replace_if_not_null", AGG_LOAD_SUFFIX,
                      create_aggregate_function_last<false, true>, false);
}

} // namespace doris::vectorized

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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/registerFunctionsArray.cpp
// and modified by Doris

#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

void register_function_array_shuffle(SimpleFunctionFactory&);
void register_function_array_exists(SimpleFunctionFactory&);
void register_function_array_element(SimpleFunctionFactory&);
void register_function_array_index(SimpleFunctionFactory&);
void register_function_array_aggregation(SimpleFunctionFactory&);
void register_function_array_distance(SimpleFunctionFactory&);
void register_function_array_distinct(SimpleFunctionFactory&);
void register_function_array_remove(SimpleFunctionFactory&);
void register_function_array_sort(SimpleFunctionFactory&);
void register_function_array_sortby(SimpleFunctionFactory&);
void register_function_arrays_overlap(SimpleFunctionFactory&);
void register_function_array_union(SimpleFunctionFactory&);
void register_function_array_except(SimpleFunctionFactory&);
void register_function_array_intersect(SimpleFunctionFactory&);
void register_function_array_slice(SimpleFunctionFactory&);
void register_function_array_difference(SimpleFunctionFactory&);
void register_function_array_enumerate(SimpleFunctionFactory&);
void register_function_array_enumerate_uniq(SimpleFunctionFactory&);
void register_function_array_range(SimpleFunctionFactory&);
void register_function_array_compact(SimpleFunctionFactory&);
void register_function_array_popback(SimpleFunctionFactory&);
void register_function_array_popfront(SimpleFunctionFactory&);
void register_function_array_with_constant(SimpleFunctionFactory&);
void register_function_array_constructor(SimpleFunctionFactory&);
void register_function_array_apply(SimpleFunctionFactory&);
void register_function_array_concat(SimpleFunctionFactory&);
void register_function_array_zip(SimpleFunctionFactory&);
void register_function_array_pushfront(SimpleFunctionFactory& factory);
void register_function_array_pushback(SimpleFunctionFactory& factory);
void register_function_array_first_or_last_index(SimpleFunctionFactory& factory);
void register_function_array_cum_sum(SimpleFunctionFactory& factory);
void register_function_array_count(SimpleFunctionFactory&);
void register_function_array_filter_function(SimpleFunctionFactory&);
void register_function_array_splits(SimpleFunctionFactory&);
void register_function_array_contains_all(SimpleFunctionFactory&);
void register_function_array_match(SimpleFunctionFactory&);

void register_function_array(SimpleFunctionFactory& factory) {
    register_function_array_shuffle(factory);
    register_function_array_exists(factory);
    register_function_array_element(factory);
    register_function_array_index(factory);
    register_function_array_aggregation(factory);
    register_function_array_distance(factory);
    register_function_array_distinct(factory);
    register_function_array_remove(factory);
    register_function_array_sort(factory);
    register_function_array_sortby(factory);
    register_function_arrays_overlap(factory);
    register_function_array_union(factory);
    register_function_array_except(factory);
    register_function_array_intersect(factory);
    register_function_array_slice(factory);
    register_function_array_difference(factory);
    register_function_array_enumerate(factory);
    register_function_array_enumerate_uniq(factory);
    register_function_array_range(factory);
    register_function_array_compact(factory);
    register_function_array_popback(factory);
    register_function_array_popfront(factory);
    register_function_array_with_constant(factory);
    register_function_array_constructor(factory);
    register_function_array_apply(factory);
    register_function_array_concat(factory);
    register_function_array_zip(factory);
    register_function_array_pushfront(factory);
    register_function_array_pushback(factory);
    register_function_array_first_or_last_index(factory);
    register_function_array_cum_sum(factory);
    register_function_array_count(factory);
    register_function_array_filter_function(factory);
    register_function_array_splits(factory);
    register_function_array_contains_all(factory);
    register_function_array_match(factory);
}

} // namespace doris::vectorized

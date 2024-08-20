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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionFactory.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"

#include <mutex>

#include "vec/aggregate_functions/aggregate_function_reader.h"

namespace doris::vectorized {

void register_aggregate_function_combinator_distinct(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_combinator_foreach(AggregateFunctionSimpleFactory& factory);

void register_aggregate_function_sum(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_sum0(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_minmax(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_min_by(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_max_by(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_avg(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_count(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_count_by_enum(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_HLL_union_agg(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_uniq(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_uniq_distribute_key(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_bit(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_bitmap(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_quantile_state(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_window_rank(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_window_lead_lag_first_last(
        AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_stddev_variance_pop(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_stddev_variance_samp(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_topn(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_approx_count_distinct(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_group_array_intersect(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_group_concat(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_percentile(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_percentile_old(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_window_funnel(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_window_funnel_old(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_retention(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_percentile_approx(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_orthogonal_bitmap(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_collect_list(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_sequence_match(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_avg_weighted(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_histogram(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_map_agg(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_bitmap_agg(AggregateFunctionSimpleFactory& factory);
void register_aggregate_functions_corr(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_covar_pop(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_covar_samp(AggregateFunctionSimpleFactory& factory);

AggregateFunctionSimpleFactory& AggregateFunctionSimpleFactory::instance() {
    static std::once_flag oc;
    static AggregateFunctionSimpleFactory instance;
    std::call_once(oc, [&]() {
        register_aggregate_function_sum(instance);
        register_aggregate_function_sum0(instance);
        register_aggregate_function_minmax(instance);
        register_aggregate_function_min_by(instance);
        register_aggregate_function_max_by(instance);
        register_aggregate_function_avg(instance);
        register_aggregate_function_count(instance);
        register_aggregate_function_count_by_enum(instance);
        register_aggregate_function_uniq(instance);
        register_aggregate_function_uniq_distribute_key(instance);
        register_aggregate_function_bit(instance);
        register_aggregate_function_bitmap(instance);
        register_aggregate_function_group_array_intersect(instance);
        register_aggregate_function_group_concat(instance);
        register_aggregate_function_quantile_state(instance);
        register_aggregate_function_combinator_distinct(instance);
        register_aggregate_function_reader_load(
                instance); // register aggregate function for agg reader
        register_aggregate_function_window_rank(instance);
        register_aggregate_function_stddev_variance_pop(instance);
        register_aggregate_function_topn(instance);
        register_aggregate_function_approx_count_distinct(instance);
        register_aggregate_function_percentile(instance);
        register_aggregate_function_percentile_old(instance);
        register_aggregate_function_percentile_approx(instance);
        register_aggregate_function_window_funnel(instance);
        register_aggregate_function_window_funnel_old(instance);
        register_aggregate_function_retention(instance);
        register_aggregate_function_orthogonal_bitmap(instance);
        register_aggregate_function_collect_list(instance);
        register_aggregate_function_sequence_match(instance);
        register_aggregate_function_avg_weighted(instance);
        register_aggregate_function_histogram(instance);
        register_aggregate_function_map_agg(instance);
        register_aggregate_function_bitmap_agg(instance);

        register_aggregate_function_stddev_variance_samp(instance);
        register_aggregate_function_replace_reader_load(instance);
        register_aggregate_function_window_lead_lag_first_last(instance);
        register_aggregate_function_HLL_union_agg(instance);

        register_aggregate_functions_corr(instance);
        register_aggregate_function_covar_pop(instance);
        register_aggregate_function_covar_samp(instance);

        register_aggregate_function_combinator_foreach(instance);
    });
    return instance;
}

} // namespace doris::vectorized

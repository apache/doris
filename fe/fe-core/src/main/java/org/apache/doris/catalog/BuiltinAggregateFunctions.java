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

package org.apache.doris.catalog;

import org.apache.doris.nereids.trees.expressions.functions.agg.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * Builtin aggregate functions.
 * <p>
 * Note: Please ensure that this class only has some lists and no procedural code.
 * It helps to be clear and concise.
 */
public class BuiltinAggregateFunctions implements FunctionHelper {
    public final List<AggregateFunc> aggregateFunctions = ImmutableList.of(
            agg(AnyValue.class, "any", "any_value"),
            agg(ArrayAgg.class, "array_agg"),
            agg(Avg.class, "avg"),
            agg(AvgWeighted.class, "avg_weighted"),
            agg(BitmapAgg.class, "bitmap_agg"),
            agg(BitmapIntersect.class, "bitmap_intersect"),
            agg(BitmapUnion.class, "bitmap_union"),
            agg(BitmapUnionCount.class, "bitmap_union_count"),
            agg(BitmapUnionInt.class, "bitmap_union_int"),
            agg(CollectList.class, "collect_list", "group_array"),
            agg(CollectSet.class, "collect_set", "group_uniq_array"),
            agg(Corr.class, "corr"),
            agg(Count.class, "count"),
            agg(CountByEnum.class, "count_by_enum"),
            agg(Covar.class, "covar", "covar_pop"),
            agg(CovarSamp.class, "covar_samp"),
            agg(GroupBitAnd.class, "group_bit_and"),
            agg(GroupBitOr.class, "group_bit_or"),
            agg(GroupBitXor.class, "group_bit_xor"),
            agg(GroupBitmapXor.class, "group_bitmap_xor"),
            agg(GroupConcat.class, "group_concat"),
            agg(Histogram.class, "hist", "histogram"),
            agg(HllUnion.class, "hll_raw_agg", "hll_union"),
            agg(HllUnionAgg.class, "hll_union_agg"),
            agg(IntersectCount.class, "intersect_count"),
            agg(MapAgg.class, "map_agg"),
            agg(Max.class, "max"),
            agg(MaxBy.class, "max_by"),
            agg(Min.class, "min"),
            agg(MinBy.class, "min_by"),
            agg(MultiDistinctCount.class, "multi_distinct_count"),
            agg(MultiDistinctGroupConcat.class, "multi_distinct_group_concat"),
            agg(MultiDistinctSum.class, "multi_distinct_sum"),
            agg(MultiDistinctSum0.class, "multi_distinct_sum0"),
            agg(Ndv.class, "approx_count_distinct", "ndv"),
            agg(OrthogonalBitmapIntersect.class, "orthogonal_bitmap_intersect"),
            agg(OrthogonalBitmapIntersectCount.class, "orthogonal_bitmap_intersect_count"),
            agg(OrthogonalBitmapUnionCount.class, "orthogonal_bitmap_union_count"),
            agg(Percentile.class, "percentile"),
            agg(PercentileApprox.class, "percentile_approx"),
            agg(PercentileArray.class, "percentile_array"),
            agg(QuantileUnion.class, "quantile_union"),
            agg(Retention.class, "retention"),
            agg(SequenceCount.class, "sequence_count"),
            agg(SequenceMatch.class, "sequence_match"),
            agg(Stddev.class, "stddev_pop", "stddev"),
            agg(StddevSamp.class, "stddev_samp"),
            agg(Sum.class, "sum"),
            agg(Sum0.class, "sum0"),
            agg(TopN.class, "topn"),
            agg(TopNArray.class, "topn_array"),
            agg(TopNWeighted.class, "topn_weighted"),
            agg(Variance.class, "var_pop", "variance_pop", "variance"),
            agg(VarianceSamp.class, "var_samp", "variance_samp"),
            agg(WindowFunnel.class, "window_funnel")
    );

    public final Set<String> aggFuncNames = aggregateFunctions.stream()
            .flatMap(fun -> fun.names.stream())
            .collect(ImmutableSet.toImmutableSet());

    public static final BuiltinAggregateFunctions INSTANCE = new BuiltinAggregateFunctions();

    // Note: Do not add any code here!
    private BuiltinAggregateFunctions() {}
}

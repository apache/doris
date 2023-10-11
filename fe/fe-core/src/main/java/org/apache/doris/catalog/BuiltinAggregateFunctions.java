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

import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.AvgWeighted;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapIntersect;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionInt;
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectList;
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectSet;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.CountByEnum;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupBitAnd;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupBitOr;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupBitXor;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupBitmapXor;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.Histogram;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnionAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.IntersectCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.MaxBy;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.MinBy;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctSum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Ndv;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapIntersect;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapIntersectCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Percentile;
import org.apache.doris.nereids.trees.expressions.functions.agg.PercentileApprox;
import org.apache.doris.nereids.trees.expressions.functions.agg.PercentileArray;
import org.apache.doris.nereids.trees.expressions.functions.agg.QuantileUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Retention;
import org.apache.doris.nereids.trees.expressions.functions.agg.SequenceCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.SequenceMatch;
import org.apache.doris.nereids.trees.expressions.functions.agg.Stddev;
import org.apache.doris.nereids.trees.expressions.functions.agg.StddevSamp;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopN;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopNArray;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopNWeighted;
import org.apache.doris.nereids.trees.expressions.functions.agg.Variance;
import org.apache.doris.nereids.trees.expressions.functions.agg.VarianceSamp;
import org.apache.doris.nereids.trees.expressions.functions.agg.WindowFunnel;

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
            agg(Avg.class, "avg"),
            agg(AvgWeighted.class, "avg_weighted"),
            agg(BitmapIntersect.class, "bitmap_intersect"),
            agg(BitmapUnion.class, "bitmap_union"),
            agg(BitmapUnionCount.class, "bitmap_union_count"),
            agg(BitmapUnionInt.class, "bitmap_union_int"),
            agg(CollectList.class, "collect_list"),
            agg(CollectSet.class, "collect_set"),
            agg(Count.class, "count"),
            agg(CountByEnum.class, "count_by_enum"),
            agg(GroupBitAnd.class, "group_bit_and"),
            agg(GroupBitOr.class, "group_bit_or"),
            agg(GroupBitXor.class, "group_bit_xor"),
            agg(GroupBitmapXor.class, "group_bitmap_xor"),
            agg(GroupConcat.class, "group_concat"),
            agg(Histogram.class, "hist", "histogram"),
            agg(HllUnion.class, "hll_raw_agg", "hll_union"),
            agg(HllUnionAgg.class, "hll_union_agg"),
            agg(IntersectCount.class, "intersect_count"),
            agg(Max.class, "max"),
            agg(MaxBy.class, "max_by"),
            agg(Min.class, "min"),
            agg(MinBy.class, "min_by"),
            agg(MultiDistinctCount.class, "multi_distinct_count"),
            agg(MultiDistinctGroupConcat.class, "multi_distinct_group_concat"),
            agg(MultiDistinctSum.class, "multi_distinct_sum"),
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

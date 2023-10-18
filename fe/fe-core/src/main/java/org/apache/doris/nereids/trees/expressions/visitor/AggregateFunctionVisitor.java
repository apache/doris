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

package org.apache.doris.nereids.trees.expressions.visitor;

import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.AvgWeighted;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapAgg;
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
import org.apache.doris.nereids.trees.expressions.functions.agg.MapAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.MaxBy;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.MinBy;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctSum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Ndv;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
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
import org.apache.doris.nereids.trees.expressions.functions.combinator.MergeCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.UnionCombinator;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdaf;

/** AggregateFunctionVisitor. */
public interface AggregateFunctionVisitor<R, C> {

    R visitAggregateFunction(AggregateFunction function, C context);

    default R visitNullableAggregateFunction(NullableAggregateFunction nullableAggregateFunction, C context) {
        return visitAggregateFunction(nullableAggregateFunction, context);
    }

    default R visitAnyValue(AnyValue anyValue, C context) {
        return visitAggregateFunction(anyValue, context);
    }

    default R visitAvg(Avg avg, C context) {
        return visitNullableAggregateFunction(avg, context);
    }

    default R visitAvgWeighted(AvgWeighted avgWeighted, C context) {
        return visitAggregateFunction(avgWeighted, context);
    }

    default R visitBitmapAgg(BitmapAgg bitmapAgg, C context) {
        return visitAggregateFunction(bitmapAgg, context);
    }

    default R visitBitmapIntersect(BitmapIntersect bitmapIntersect, C context) {
        return visitAggregateFunction(bitmapIntersect, context);
    }

    default R visitBitmapUnion(BitmapUnion bitmapUnion, C context) {
        return visitAggregateFunction(bitmapUnion, context);
    }

    default R visitBitmapUnionCount(BitmapUnionCount bitmapUnionCount, C context) {
        return visitAggregateFunction(bitmapUnionCount, context);
    }

    default R visitBitmapUnionInt(BitmapUnionInt bitmapUnionInt, C context) {
        return visitAggregateFunction(bitmapUnionInt, context);
    }

    default R visitCollectList(CollectList collectList, C context) {
        return visitAggregateFunction(collectList, context);
    }

    default R visitCollectSet(CollectSet collectSet, C context) {
        return visitAggregateFunction(collectSet, context);
    }

    default R visitCount(Count count, C context) {
        return visitAggregateFunction(count, context);
    }

    default R visitCountByEnum(CountByEnum count, C context) {
        return visitAggregateFunction(count, context);
    }

    default R visitMultiDistinctCount(MultiDistinctCount multiDistinctCount, C context) {
        return visitAggregateFunction(multiDistinctCount, context);
    }

    default R visitMultiDistinctGroupConcat(MultiDistinctGroupConcat multiDistinctGroupConcat, C context) {
        return visitAggregateFunction(multiDistinctGroupConcat, context);
    }

    default R visitMultiDistinctSum(MultiDistinctSum multiDistinctSum, C context) {
        return visitAggregateFunction(multiDistinctSum, context);
    }

    default R visitGroupBitAnd(GroupBitAnd groupBitAnd, C context) {
        return visitNullableAggregateFunction(groupBitAnd, context);
    }

    default R visitGroupBitOr(GroupBitOr groupBitOr, C context) {
        return visitNullableAggregateFunction(groupBitOr, context);
    }

    default R visitGroupBitXor(GroupBitXor groupBitXor, C context) {
        return visitNullableAggregateFunction(groupBitXor, context);
    }

    default R visitGroupBitmapXor(GroupBitmapXor groupBitmapXor, C context) {
        return visitNullableAggregateFunction(groupBitmapXor, context);
    }

    default R visitGroupConcat(GroupConcat groupConcat, C context) {
        return visitNullableAggregateFunction(groupConcat, context);
    }

    default R visitHistogram(Histogram histogram, C context) {
        return visitAggregateFunction(histogram, context);
    }

    default R visitHllUnion(HllUnion hllUnion, C context) {
        return visitAggregateFunction(hllUnion, context);
    }

    default R visitHllUnionAgg(HllUnionAgg hllUnionAgg, C context) {
        return visitAggregateFunction(hllUnionAgg, context);
    }

    default R visitIntersectCount(IntersectCount intersectCount, C context) {
        return visitAggregateFunction(intersectCount, context);
    }

    default R visitMapAgg(MapAgg mapAgg, C context) {
        return visitAggregateFunction(mapAgg, context);
    }

    default R visitMax(Max max, C context) {
        return visitNullableAggregateFunction(max, context);
    }

    default R visitMaxBy(MaxBy maxBy, C context) {
        return visitNullableAggregateFunction(maxBy, context);
    }

    default R visitMin(Min min, C context) {
        return visitNullableAggregateFunction(min, context);
    }

    default R visitMinBy(MinBy minBy, C context) {
        return visitNullableAggregateFunction(minBy, context);
    }

    default R visitNdv(Ndv ndv, C context) {
        return visitAggregateFunction(ndv, context);
    }

    default R visitOrthogonalBitmapIntersect(OrthogonalBitmapIntersect function, C context) {
        return visitAggregateFunction(function, context);
    }

    default R visitOrthogonalBitmapIntersectCount(OrthogonalBitmapIntersectCount function, C context) {
        return visitAggregateFunction(function, context);
    }

    default R visitOrthogonalBitmapUnionCount(OrthogonalBitmapUnionCount function, C context) {
        return visitAggregateFunction(function, context);
    }

    default R visitPercentile(Percentile percentile, C context) {
        return visitAggregateFunction(percentile, context);
    }

    default R visitPercentileApprox(PercentileApprox percentileApprox, C context) {
        return visitAggregateFunction(percentileApprox, context);
    }

    default R visitPercentileArray(PercentileArray percentileArray, C context) {
        return visitAggregateFunction(percentileArray, context);
    }

    default R visitQuantileUnion(QuantileUnion quantileUnion, C context) {
        return visitAggregateFunction(quantileUnion, context);
    }

    default R visitRetention(Retention retention, C context) {
        return visitAggregateFunction(retention, context);
    }

    default R visitSequenceCount(SequenceCount sequenceCount, C context) {
        return visitAggregateFunction(sequenceCount, context);
    }

    default R visitSequenceMatch(SequenceMatch sequenceMatch, C context) {
        return visitAggregateFunction(sequenceMatch, context);
    }

    default R visitStddev(Stddev stddev, C context) {
        return visitAggregateFunction(stddev, context);
    }

    default R visitStddevSamp(StddevSamp stddevSamp, C context) {
        return visitAggregateFunction(stddevSamp, context);
    }

    default R visitSum(Sum sum, C context) {
        return visitNullableAggregateFunction(sum, context);
    }

    default R visitTopN(TopN topN, C context) {
        return visitAggregateFunction(topN, context);
    }

    default R visitTopNArray(TopNArray topnArray, C context) {
        return visitAggregateFunction(topnArray, context);
    }

    default R visitTopNWeighted(TopNWeighted topnWeighted, C context) {
        return visitAggregateFunction(topnWeighted, context);
    }

    default R visitVariance(Variance variance, C context) {
        return visitAggregateFunction(variance, context);
    }

    default R visitVarianceSamp(VarianceSamp varianceSamp, C context) {
        return visitAggregateFunction(varianceSamp, context);
    }

    default R visitWindowFunnel(WindowFunnel windowFunnel, C context) {
        return visitAggregateFunction(windowFunnel, context);
    }

    default R visitMergeCombinator(MergeCombinator combinator, C context) {
        return visitAggregateFunction(combinator, context);
    }

    default R visitUnionCombinator(UnionCombinator combinator, C context) {
        return visitAggregateFunction(combinator, context);
    }

    default R visitJavaUdaf(JavaUdaf javaUdaf, C context) {
        return visitAggregateFunction(javaUdaf, context);
    }

}

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
import org.apache.doris.nereids.trees.expressions.functions.agg.ApproxCountDistinct;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupBitAnd;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupBitOr;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupBitXor;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnionAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctSum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Ndv;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Variance;
import org.apache.doris.nereids.trees.expressions.functions.agg.VarianceSamp;

/**
 * AggregateFunctionVisitor.
 */
public interface AggregateFunctionVisitor<R, C> {
    R visitAggregateFunction(AggregateFunction aggregateFunction, C context);

    default R visitNullableAggregateFunction(NullableAggregateFunction nullableAggregateFunction, C context) {
        return visitAggregateFunction(nullableAggregateFunction, context);
    }

    default R visitAvg(Avg avg, C context) {
        return visitNullableAggregateFunction(avg, context);
    }

    default R visitCount(Count count, C context) {
        return visitAggregateFunction(count, context);
    }

    default R visitMax(Max max, C context) {
        return visitNullableAggregateFunction(max, context);
    }

    default R visitMin(Min min, C context) {
        return visitNullableAggregateFunction(min, context);
    }

    default R visitMultiDistinctCount(MultiDistinctCount multiDistinctCount, C context) {
        return visitAggregateFunction(multiDistinctCount, context);
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

    default R visitSum(Sum sum, C context) {
        return visitNullableAggregateFunction(sum, context);
    }

    default R visitBitmapUnionCount(BitmapUnionCount bitmapUnionCount, C context) {
        return visitAggregateFunction(bitmapUnionCount, context);
    }

    default R visitVariance(Variance variance, C context) {
        return visitAggregateFunction(variance, context);
    }

    default R visitVarianceSamp(VarianceSamp varianceSamp, C context) {
        return visitAggregateFunction(varianceSamp, context);
    }

    default R visitNdv(Ndv ndv, C context) {
        return visitAggregateFunction(ndv, context);
    }

    default R visitHllUnionAgg(HllUnionAgg hllUnionAgg, C context) {
        return visitAggregateFunction(hllUnionAgg, context);
    }

    default R visitHllUnion(HllUnion hllUnion, C context) {
        return visitAggregateFunction(hllUnion, context);
    }

    default R visitApproxCountDistinct(ApproxCountDistinct approxCountDistinct, C context) {
        return visitAggregateFunction(approxCountDistinct, context);
    }

    default R visitAnyValue(AnyValue anyValue, C context) {
        return visitAggregateFunction(anyValue, context);
    }
}

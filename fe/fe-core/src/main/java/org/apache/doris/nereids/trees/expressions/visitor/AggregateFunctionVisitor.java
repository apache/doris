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
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.CountByEnum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;

/** AggregateFunctionVisitor. */
public interface AggregateFunctionVisitor<R, C> {
    R visitAggregateFunction(AggregateFunction aggregateFunction, C context);

    default R visitAvg(Avg avg, C context) {
        return visitAggregateFunction(avg, context);
    }

    default R visitCount(Count count, C context) {
        return visitAggregateFunction(count, context);
    }

    default R visitCountByEnum(CountByEnum count, C context) {
        return visitAggregateFunction(count, context);
    }

    default R visitMax(Max max, C context) {
        return visitAggregateFunction(max, context);
    }

    default R visitMin(Min min, C context) {
        return visitAggregateFunction(min, context);
    }

    default R visitSum(Sum sum, C context) {
        return visitAggregateFunction(sum, context);
    }
}

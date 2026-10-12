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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.DataSketchesHllUnionAgg;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class EliminateAggCaseWhenTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testEliminateSingleArgumentDataSketchesHllCaseWhen() {
        Expression sketch = scan1.getOutput().get(1);
        Expression predicate = new EqualTo(scan1.getOutput().get(0), new IntegerLiteral(1));
        If conditionalSketch = new If(predicate, sketch, new NullLiteral(StringType.INSTANCE));
        DataSketchesHllUnionAgg function = new DataSketchesHllUnionAgg(conditionalSketch);
        LogicalAggregate<?> aggregate = new LogicalAggregate<>(
                ImmutableList.of(), ImmutableList.of(new Alias(function, "estimate")), scan1);

        PlanChecker.from(MemoTestUtils.createConnectContext(), aggregate)
                .applyTopDown(new EliminateAggCaseWhen())
                .matches(
                        logicalAggregate(
                                logicalFilter(logicalOlapScan()).when(filter ->
                                        filter.getConjuncts().equals(ImmutableSet.of(predicate)))
                        ).when(agg -> agg.getAggregateFunctions()
                                .equals(ImmutableSet.of(new DataSketchesHllUnionAgg(sketch))))
                );
    }

    @Test
    void testKeepDataSketchesHllCaseWhenWithLgMaxK() {
        Expression sketch = scan1.getOutput().get(1);
        Expression predicate = new EqualTo(scan1.getOutput().get(0), new IntegerLiteral(1));
        If conditionalSketch = new If(predicate, sketch, new NullLiteral(StringType.INSTANCE));
        DataSketchesHllUnionAgg function =
                new DataSketchesHllUnionAgg(conditionalSketch, new IntegerLiteral(8));
        LogicalAggregate<?> aggregate = new LogicalAggregate<>(
                ImmutableList.of(), ImmutableList.of(new Alias(function, "estimate")), scan1);

        PlanChecker.from(MemoTestUtils.createConnectContext(), aggregate)
                .applyTopDown(new EliminateAggCaseWhen())
                .matches(
                        logicalAggregate(logicalOlapScan())
                                .when(agg -> agg.getAggregateFunctions().equals(ImmutableSet.of(function)))
                );
    }
}

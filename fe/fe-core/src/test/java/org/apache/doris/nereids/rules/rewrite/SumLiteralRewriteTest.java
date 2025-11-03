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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class SumLiteralRewriteTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testSimpleSum() {
        Slot slot1 = scan1.getOutput().get(0);
        Alias add1 = new Alias(new Sum(new Add(slot1, Literal.of(1))));
        Alias add2 = new Alias(new Sum(new Add(slot1, Literal.of(2))));
        Alias sub1 = new Alias(new Sum(new Subtract(slot1, Literal.of(1))));
        Alias sub2 = new Alias(new Sum(new Subtract(slot1, Literal.of(2))));
        LogicalAggregate<?> agg = new LogicalAggregate<>(
                ImmutableList.of(scan1.getOutput().get(0)), ImmutableList.of(add1, add2, sub1, sub2), scan1);
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyTopDown(ImmutableList.of(new SumLiteralRewrite().build()))
                .printlnTree()
                .matches(logicalAggregate().when(p -> p.getOutputs().size() == 2));

        Alias sum = new Alias(new Sum(slot1));
        agg = new LogicalAggregate<>(
                ImmutableList.of(scan1.getOutput().get(0)), ImmutableList.of(sum, add1, add2, sub1, sub2), scan1);
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyTopDown(ImmutableList.of(new SumLiteralRewrite().build()))
                .printlnTree()
                .matches(logicalAggregate().when(p -> p.getOutputs().size() == 2));
    }

    @Test
    void testSumNullable() {
        Slot slot1 = scan1.getOutput().get(0);
        Alias add1 = new Alias(new Sum(false, true, new Add(slot1, Literal.of(1))));
        Alias add2 = new Alias(new Sum(false, true, new Add(slot1, Literal.of(2))));
        Alias sub1 = new Alias(new Sum(false, true, new Subtract(slot1, Literal.of(1))));
        Alias sub2 = new Alias(new Sum(false, true, new Subtract(slot1, Literal.of(2))));
        LogicalAggregate<?> agg = new LogicalAggregate<>(
                ImmutableList.of(scan1.getOutput().get(0)), ImmutableList.of(add1, add2, sub1, sub2), scan1);
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyTopDown(ImmutableList.of(new SumLiteralRewrite().build()))
                .printlnTree()
                .matches(logicalAggregate().when(p -> p.getOutputs().size() == 2));

        Alias sum = new Alias(new Sum(false, true, slot1));
        agg = new LogicalAggregate<>(
                ImmutableList.of(scan1.getOutput().get(0)), ImmutableList.of(sum, add1, add2, sub1, sub2), scan1);
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyTopDown(ImmutableList.of(new SumLiteralRewrite().build()))
                .printlnTree()
                .matches(logicalAggregate().when(p -> p.getOutputs().size() == 2));
    }

    @Test
    void testSumDistinct() {
        Slot slot1 = scan1.getOutput().get(0);
        Alias add1 = new Alias(new Sum(true, true, new Add(slot1, Literal.of(1))));
        Alias add2 = new Alias(new Sum(false, true, new Add(slot1, Literal.of(2))));
        Alias sub1 = new Alias(new Sum(true, true, new Subtract(slot1, Literal.of(1))));
        Alias sub2 = new Alias(new Sum(false, true, new Subtract(slot1, Literal.of(2))));
        LogicalAggregate<?> agg = new LogicalAggregate<>(
                ImmutableList.of(scan1.getOutput().get(0)), ImmutableList.of(add1, add2, sub1, sub2), scan1);
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyTopDown(ImmutableList.of(new SumLiteralRewrite().build()))
                .printlnTree()
                .matches(logicalAggregate().when(p -> p.getOutputs().size() == 4));

        Alias sumDistinct = new Alias(new Sum(true, true, slot1));
        agg = new LogicalAggregate<>(
                ImmutableList.of(scan1.getOutput().get(0)), ImmutableList.of(sumDistinct, add1, add2, sub1, sub2), scan1);
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyTopDown(ImmutableList.of(new SumLiteralRewrite().build()))
                .printlnTree()
                .matches(logicalAggregate().when(p -> p.getOutputs().size() == 4));

        Alias sum = new Alias(new Sum(false, true, slot1));
        agg = new LogicalAggregate<>(
                ImmutableList.of(scan1.getOutput().get(0)), ImmutableList.of(sumDistinct, sum, add1, add2, sub1, sub2), scan1);
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyTopDown(ImmutableList.of(new SumLiteralRewrite().build()))
                .printlnTree()
                .matches(logicalAggregate().when(p -> p.getOutputs().size() == 4));
    }

    @Test
    void testSumOnce() {
        Slot slot1 = scan1.getOutput().get(0);
        Alias add1 = new Alias(new Sum(false, true, new Add(slot1, Literal.of(1))));
        LogicalAggregate<?> agg = new LogicalAggregate<>(
                ImmutableList.of(scan1.getOutput().get(0)), ImmutableList.of(add1), scan1);
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyTopDown(ImmutableList.of(new SumLiteralRewrite().build()))
                .printlnTree()
                .matches(logicalAggregate().when(p -> p.getOutputs().size() == 1));

        Slot slot2 = new Alias(scan1.getOutput().get(0)).toSlot();
        Alias add2 = new Alias(new Sum(false, true, new Add(slot2, Literal.of(2))));
        agg = new LogicalAggregate<>(
                ImmutableList.of(scan1.getOutput().get(0)), ImmutableList.of(add1, add2), scan1);
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyTopDown(ImmutableList.of(new SumLiteralRewrite().build()))
                .printlnTree()
                .matches(logicalAggregate().when(p -> p.getOutputs().size() == 2));

        Alias add3 = new Alias(new Sum(false, true, new Add(slot1, Literal.of(3))));
        Alias add4 = new Alias(new Sum(false, true, new Add(slot1, Literal.of(4))));
        agg = new LogicalAggregate<>(
                ImmutableList.of(scan1.getOutput().get(0)), ImmutableList.of(add1, add2, add3, add4), scan1);
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyTopDown(ImmutableList.of(new SumLiteralRewrite().build()))
                .printlnTree()
                .matches(logicalAggregate().when(p -> p.getOutputs().size() == 3));

    }
}

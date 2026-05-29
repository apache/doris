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
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

class PullUpProjectExprUnderTopNTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testPullUpAddExpression() {
        List<NamedExpression> exprs = ImmutableList.of(
                scan1.getOutput().get(0),
                new Alias(new Add(scan1.getOutput().get(1), new IntegerLiteral((byte) 1)), "b")
        );
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .projectExprs(exprs)
                .topN(0, 3, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                )
                        )
                );
    }

    @Test
    void testNotPullUpSimpleAlias() {
        List<NamedExpression> exprs = ImmutableList.of(
                scan1.getOutput().get(0).alias("a"),
                scan1.getOutput().get(1)
        );
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .projectExprs(exprs)
                .topN(0, 3, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matches(
                        logicalTopN(
                                logicalProject(
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testBlockedByFilterGoesToInnerTopN() {
        // project(y) -> topn1 -> filter(x>1) -> topn2 -> project(a+1 as x, b+1 as y)
        // x should go to topn2 (blocked by filter), y should go to topn1
        Slot id = scan1.getOutput().get(0);
        Slot a = scan1.getOutput().get(1);
        Slot b = scan1.getOutput().get(2);

        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");
        GreaterThan filter = new GreaterThan(x.toSlot(), new IntegerLiteral((byte) 1));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .projectExprs(ImmutableList.of(x, y))
                .topN(0, 10, ImmutableList.of(0))
                .filter(filter)
                .topN(0, 3, ImmutableList.of(0))
                .projectExprs(ImmutableList.of(y.toSlot()))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .printlnTree();
    }
}

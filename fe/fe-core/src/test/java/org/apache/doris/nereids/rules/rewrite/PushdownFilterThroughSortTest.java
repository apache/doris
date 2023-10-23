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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class PushdownFilterThroughSortTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
            ImmutableList.of(""));

    @Test
    void testPushdownFilterThroughSortTest() {
        Slot gender = scan.getOutput().get(1);

        Expression filterPredicate = new GreaterThan(gender, Literal.of(1));

        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .sort(scan.getOutput().stream().map(c -> new OrderKey(c, true, true)).collect(Collectors.toList()))
                .filter(filterPredicate)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushdownFilterThroughSort())
                .matchesFromRoot(
                    logicalSort(
                        logicalFilter(
                            logicalOlapScan()
                        )
                    )
                );
    }

    @Test
    void testFilterOneConstant() {
        Slot gender = scan.getOutput().get(1);
        Expression filterPredicate = new EqualTo(gender, Literal.of(1));

        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .sort(Lists.newArrayList(new OrderKey(gender, true, true)))
                .filter(filterPredicate)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushdownFilterThroughSort())
                .matchesFromRoot(logicalFilter(logicalOlapScan()));
    }

    @Test
    void testFilterTwoConstant() {
        Slot gender = scan.getOutput().get(1);
        Slot id = scan.getOutput().get(0);

        Expression filterPredicate = new EqualTo(gender, Literal.of(1));
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(gender, true, true),
                new OrderKey(id, true, true));

        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .sort(orderKeys)
                .filter(filterPredicate)
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushdownFilterThroughSort())
                .matchesFromRoot(logicalSort(logicalFilter(logicalOlapScan())));

        Expression filterPredicate2 = new EqualTo(id, Literal.of(1));
        plan = new LogicalPlanBuilder(scan)
                .sort(orderKeys)
                .filter(Sets.newHashSet(filterPredicate2, filterPredicate))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushdownFilterThroughSort())
                .matchesFromRoot(logicalFilter(logicalOlapScan()));
    }
}

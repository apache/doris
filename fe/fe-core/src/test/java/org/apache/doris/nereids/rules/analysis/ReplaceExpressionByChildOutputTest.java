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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ReplaceExpressionByChildOutputTest implements PatternMatchSupported {

    @Test
    void testSortProject() {
        SlotReference slotReference = new SlotReference("col1", IntegerType.INSTANCE);
        Alias alias = new Alias(slotReference, "a");
        LogicalOlapScan logicalOlapScan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalProject<Plan> logicalProject = new LogicalProject<>(ImmutableList.of(alias), logicalOlapScan);
        List<OrderKey> orderKeys = ImmutableList.of(new OrderKey(slotReference, true, true));
        LogicalSort<LogicalProject<Plan>> logicalSort = new LogicalSort<>(orderKeys, logicalProject);

        PlanChecker.from(MemoTestUtils.createConnectContext(), logicalSort)
                .applyBottomUp(new ReplaceExpressionByChildOutput())
                .matchesFromRoot(
                        logicalSort(logicalProject()).when(sort ->
                                ((Slot) (sort.getOrderKeys().get(0).getExpr())).getExprId().equals(alias.getExprId()))
                );
    }

    @Test
    void testSortAggregate() {
        SlotReference slotReference = new SlotReference("col1", IntegerType.INSTANCE);
        Alias alias = new Alias(slotReference, "a");
        LogicalOlapScan logicalOlapScan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalAggregate<Plan> logicalAggregate = new LogicalAggregate<>(
                ImmutableList.of(alias), ImmutableList.of(alias), logicalOlapScan);
        List<OrderKey> orderKeys = ImmutableList.of(new OrderKey(slotReference, true, true));
        LogicalSort<LogicalAggregate<Plan>> logicalSort = new LogicalSort<>(orderKeys, logicalAggregate);

        PlanChecker.from(MemoTestUtils.createConnectContext(), logicalSort)
                .applyBottomUp(new ReplaceExpressionByChildOutput())
                .matchesFromRoot(
                        logicalSort(logicalAggregate()).when(sort ->
                                ((Slot) (sort.getOrderKeys().get(0).getExpr())).getExprId().equals(alias.getExprId()))
                );
    }

    @Test
    void testSortHavingAggregate() {
        SlotReference slotReference = new SlotReference("col1", IntegerType.INSTANCE);
        Alias alias = new Alias(slotReference, "a");
        LogicalOlapScan logicalOlapScan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalAggregate<Plan> logicalAggregate = new LogicalAggregate<>(
                ImmutableList.of(alias), ImmutableList.of(alias), logicalOlapScan);
        LogicalHaving<Plan> logicalHaving = new LogicalHaving<>(BooleanLiteral.TRUE, logicalAggregate);
        List<OrderKey> orderKeys = ImmutableList.of(new OrderKey(slotReference, true, true));
        LogicalSort<Plan> logicalSort = new LogicalSort<>(orderKeys, logicalHaving);

        PlanChecker.from(MemoTestUtils.createConnectContext(), logicalSort)
                .applyBottomUp(new ReplaceExpressionByChildOutput())
                .matchesFromRoot(
                        logicalSort(logicalHaving(logicalAggregate())).when(sort ->
                                ((Slot) (sort.getOrderKeys().get(0).getExpr())).getExprId().equals(alias.getExprId()))
                );
    }
}

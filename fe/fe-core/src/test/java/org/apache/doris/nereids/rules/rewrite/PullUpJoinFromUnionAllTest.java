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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class PullUpJoinFromUnionAllTest {

    @Test
    void comparatorRejectsDifferentProjectOutputSizes() {
        LogicalOlapScan smallScan = newScan(1, "common_small");
        LogicalOlapScan largeScan = newScan(1, "common_large");
        LogicalProject<LogicalOlapScan> smallProject = project(selectSlots(smallScan.getOutput(), 0), smallScan);
        LogicalProject<LogicalOlapScan> largeProject = project(selectSlots(largeScan.getOutput(), 0, 1), largeScan);

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertFalse(comparator.isLogicalEqual(largeProject, smallProject));
    }

    @Test
    void ruleSkipsJoinChildrenWithDifferentCommonSideOutputs() {
        LogicalOlapScan commonSmallScan = newScan(10, "common_small");
        LogicalOlapScan commonLargeScan = newScan(10, "common_large");
        LogicalProject<LogicalOlapScan> commonSmall = project(selectSlots(commonSmallScan.getOutput(), 0),
                commonSmallScan);
        LogicalProject<LogicalOlapScan> commonLarge = project(selectSlots(commonLargeScan.getOutput(), 0, 1),
                commonLargeScan);
        LogicalOlapScan otherLeft = newScan(20, "other_left");
        LogicalOlapScan otherRight = newScan(30, "other_right");

        LogicalProject<Plan> unionChild1 = outputProject(
                join(commonSmall, otherLeft),
                "x", commonSmall.getOutput().get(0),
                "y", otherLeft.getOutput().get(1));
        LogicalProject<Plan> unionChild2 = outputProject(
                join(commonLarge, otherRight),
                "x", commonLarge.getOutput().get(0),
                "y", otherRight.getOutput().get(1));

        LogicalUnion union = union(unionChild1, unionChild2);

        Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), union)
                .applyTopDown(new PullUpJoinFromUnionAll())
                .getPlan();

        Assertions.assertInstanceOf(LogicalUnion.class, rewritten);
        LogicalUnion rewrittenUnion = (LogicalUnion) rewritten;
        Assertions.assertEquals(2, rewrittenUnion.children().size());
        Assertions.assertInstanceOf(LogicalProject.class, rewrittenUnion.child(0));
        Assertions.assertInstanceOf(LogicalProject.class, rewrittenUnion.child(1));
    }

    private static LogicalOlapScan newScan(long tableId, String tableName) {
        return PlanConstructor.newLogicalOlapScan(tableId, tableName, 0);
    }

    private static LogicalJoin<Plan, LogicalOlapScan> join(LogicalProject<? extends Plan> commonSide,
            LogicalOlapScan otherSide) {
        Expression joinCondition = new EqualTo(commonSide.getOutput().get(0), otherSide.getOutput().get(0));
        return new LogicalJoin<>(JoinType.INNER_JOIN, ImmutableList.of(joinCondition), ImmutableList.of(),
                commonSide, otherSide, null);
    }

    private static LogicalProject<Plan> outputProject(Plan child, String leftAlias, Slot leftSlot,
            String rightAlias, Slot rightSlot) {
        return new LogicalProject<>(ImmutableList.of(
                new Alias(leftSlot, leftAlias),
                new Alias(rightSlot, rightAlias)), child);
    }

    private static LogicalUnion union(LogicalProject<Plan> left, LogicalProject<Plan> right) {
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (Slot slot : left.getOutput()) {
            outputs.add(new SlotReference(slot.getName(), slot.getDataType(), slot.nullable()));
        }
        return new LogicalUnion(Qualifier.ALL, outputs.build(), ImmutableList.of(
                toSlotReferences(left.getOutput()),
                toSlotReferences(right.getOutput())), ImmutableList.of(), false, ImmutableList.of(left, right));
    }

    private static LogicalProject<LogicalOlapScan> project(List<NamedExpression> outputs, LogicalOlapScan child) {
        return new LogicalProject<>(outputs, child);
    }

    private static List<NamedExpression> selectSlots(List<Slot> output, int... indexes) {
        ImmutableList.Builder<NamedExpression> selected = ImmutableList.builder();
        for (int index : indexes) {
            selected.add(output.get(index));
        }
        return selected.build();
    }

    private static List<SlotReference> toSlotReferences(List<Slot> slots) {
        ImmutableList.Builder<SlotReference> references = ImmutableList.builder();
        for (Slot slot : slots) {
            references.add((SlotReference) slot);
        }
        return references.build();
    }
}

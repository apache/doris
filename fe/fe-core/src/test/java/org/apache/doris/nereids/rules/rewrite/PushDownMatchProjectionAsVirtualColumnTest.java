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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.MatchAll;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test for PushDownMatchProjectionAsVirtualColumn rule.
 */
public class PushDownMatchProjectionAsVirtualColumnTest implements MemoPatternMatchSupported {

    @Test
    void testPushDownMatchProjection() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> slots = scan.getOutput();
        Slot idSlot = slots.get(0);
        Slot nameSlot = slots.get(1);

        MatchAny matchExpr = new MatchAny(nameSlot, new StringLiteral("hello"));
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(idSlot, new Alias(matchExpr, "m")), scan);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushDownMatchProjectionAsVirtualColumn())
                .getPlan();

        // Verify plan structure
        Assertions.assertInstanceOf(LogicalProject.class, root);
        LogicalProject<?> resProject = (LogicalProject<?>) root;
        Assertions.assertInstanceOf(LogicalOlapScan.class, resProject.child());
        LogicalOlapScan resScan = (LogicalOlapScan) resProject.child();

        // Verify exactly 1 virtual column wrapping the MatchAny expression
        Assertions.assertEquals(1, resScan.getVirtualColumns().size());
        Alias vcAlias = (Alias) resScan.getVirtualColumns().get(0);
        Assertions.assertInstanceOf(MatchAny.class, vcAlias.child());
        Assertions.assertEquals(matchExpr, vcAlias.child());

        // Verify alias name "m" is preserved in the project output
        List<NamedExpression> projections = resProject.getProjects();
        Assertions.assertEquals(2, projections.size());
        NamedExpression mProjection = projections.get(1);
        Assertions.assertInstanceOf(Alias.class, mProjection);
        Assertions.assertEquals("m", ((Alias) mProjection).getName());

        // Verify the "m" alias now references the virtual column's slot, not the original MatchAny
        Expression mChild = ((Alias) mProjection).child();
        Assertions.assertInstanceOf(SlotReference.class, mChild);
        Assertions.assertEquals(vcAlias.toSlot().getExprId(), ((SlotReference) mChild).getExprId());
    }

    @Test
    void testPushDownMatchProjectionWithFilter() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> slots = scan.getOutput();
        Slot idSlot = slots.get(0);
        Slot nameSlot = slots.get(1);

        MatchAny matchExpr = new MatchAny(nameSlot, new StringLiteral("hello"));
        GreaterThan filterPred = new GreaterThan(idSlot, new IntegerLiteral(2));

        LogicalProject<LogicalFilter<LogicalOlapScan>> project = new LogicalProject<>(
                ImmutableList.of(idSlot, new Alias(matchExpr, "m")),
                new LogicalFilter<>(ImmutableSet.of(filterPred), scan));

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushDownMatchProjectionAsVirtualColumn())
                .getPlan();

        // Verify plan structure: Project -> Filter -> OlapScan
        Assertions.assertInstanceOf(LogicalProject.class, root);
        LogicalProject<?> resProject = (LogicalProject<?>) root;
        Assertions.assertInstanceOf(LogicalFilter.class, resProject.child());
        LogicalFilter<?> resFilter = (LogicalFilter<?>) resProject.child();
        Assertions.assertInstanceOf(LogicalOlapScan.class, resFilter.child());
        LogicalOlapScan resScan = (LogicalOlapScan) resFilter.child();

        // Verify virtual column
        Assertions.assertEquals(1, resScan.getVirtualColumns().size());
        Alias vcAlias = (Alias) resScan.getVirtualColumns().get(0);
        Assertions.assertEquals(matchExpr, vcAlias.child());

        // Verify filter is preserved
        Assertions.assertEquals(ImmutableSet.of(filterPred), resFilter.getConjuncts());

        // Verify slot replacement in project
        NamedExpression mProjection = resProject.getProjects().get(1);
        Assertions.assertEquals("m", ((Alias) mProjection).getName());
        Assertions.assertEquals(vcAlias.toSlot().getExprId(),
                ((SlotReference) ((Alias) mProjection).child()).getExprId());
    }

    @Test
    void testNoMatchExpressionNoChange() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> slots = scan.getOutput();
        Slot idSlot = slots.get(0);

        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.<NamedExpression>of(idSlot), scan);

        PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushDownMatchProjectionAsVirtualColumn())
                .matches(
                        logicalProject(
                                logicalOlapScan().when(s -> s.getVirtualColumns().isEmpty())
                        )
                );
    }

    @Test
    void testDuplicateMatchDedup() {
        // Same MATCH expression in two aliases should create only one virtual column
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> slots = scan.getOutput();
        Slot idSlot = slots.get(0);
        Slot nameSlot = slots.get(1);

        MatchAny matchExpr = new MatchAny(nameSlot, new StringLiteral("hello"));
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(idSlot, new Alias(matchExpr, "m1"), new Alias(matchExpr, "m2")),
                scan);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushDownMatchProjectionAsVirtualColumn())
                .getPlan();

        Assertions.assertInstanceOf(LogicalProject.class, root);
        LogicalProject<?> resProject = (LogicalProject<?>) root;
        LogicalOlapScan resScan = (LogicalOlapScan) resProject.child();

        // Only one virtual column for the duplicate MATCH expression
        Assertions.assertEquals(1, resScan.getVirtualColumns().size());

        // Both aliases should reference the same virtual column slot
        Alias vcAlias = (Alias) resScan.getVirtualColumns().get(0);
        List<NamedExpression> projections = resProject.getProjects();
        Assertions.assertEquals(3, projections.size());

        Alias m1 = (Alias) projections.get(1);
        Alias m2 = (Alias) projections.get(2);
        Assertions.assertEquals("m1", m1.getName());
        Assertions.assertEquals("m2", m2.getName());
        Assertions.assertEquals(vcAlias.toSlot().getExprId(), ((SlotReference) m1.child()).getExprId());
        Assertions.assertEquals(vcAlias.toSlot().getExprId(), ((SlotReference) m2.child()).getExprId());
    }

    @Test
    void testMultipleDistinctMatchExpressions() {
        // Two different MATCH expressions should create two virtual columns
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> slots = scan.getOutput();
        Slot idSlot = slots.get(0);
        Slot nameSlot = slots.get(1);

        MatchAny matchAny = new MatchAny(nameSlot, new StringLiteral("hello"));
        MatchAll matchAll = new MatchAll(nameSlot, new StringLiteral("world"));
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(idSlot, new Alias(matchAny, "ma"), new Alias(matchAll, "mb")),
                scan);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushDownMatchProjectionAsVirtualColumn())
                .getPlan();

        Assertions.assertInstanceOf(LogicalProject.class, root);
        LogicalProject<?> resProject = (LogicalProject<?>) root;
        LogicalOlapScan resScan = (LogicalOlapScan) resProject.child();

        // Two distinct virtual columns
        Assertions.assertEquals(2, resScan.getVirtualColumns().size());
        Set<Expression> vcExprs = resScan.getVirtualColumns().stream()
                .map(vc -> ((Alias) vc).child())
                .collect(Collectors.toSet());
        Assertions.assertTrue(vcExprs.contains(matchAny));
        Assertions.assertTrue(vcExprs.contains(matchAll));

        // Each alias references its own virtual column slot
        List<NamedExpression> projections = resProject.getProjects();
        Assertions.assertEquals(3, projections.size());
        Assertions.assertEquals("ma", ((Alias) projections.get(1)).getName());
        Assertions.assertEquals("mb", ((Alias) projections.get(2)).getName());

        // Slots should be different
        SlotReference maSlot = (SlotReference) ((Alias) projections.get(1)).child();
        SlotReference mbSlot = (SlotReference) ((Alias) projections.get(2)).child();
        Assertions.assertNotEquals(maSlot.getExprId(), mbSlot.getExprId());
    }

    @Test
    void testAppendToExistingVirtualColumns() {
        // MATCH rule should append to existing virtual columns, not skip
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> slots = scan.getOutput();
        Slot idSlot = slots.get(0);
        Slot nameSlot = slots.get(1);

        // Simulate CSE rule having already added a virtual column
        Alias existingVc = new Alias(new GreaterThan(idSlot, new IntegerLiteral(5)), "cse_vc");
        LogicalOlapScan scanWithVc = scan.withVirtualColumns(ImmutableList.of(existingVc));

        MatchAny matchExpr = new MatchAny(nameSlot, new StringLiteral("hello"));
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(idSlot, new Alias(matchExpr, "m")), scanWithVc);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushDownMatchProjectionAsVirtualColumn())
                .getPlan();

        Assertions.assertInstanceOf(LogicalProject.class, root);
        LogicalProject<?> resProject = (LogicalProject<?>) root;
        LogicalOlapScan resScan = (LogicalOlapScan) resProject.child();

        // Should have 2 virtual columns: existing CSE one + new MATCH one
        Assertions.assertEquals(2, resScan.getVirtualColumns().size());

        // Existing CSE virtual column is preserved
        Alias firstVc = (Alias) resScan.getVirtualColumns().get(0);
        Assertions.assertInstanceOf(GreaterThan.class, firstVc.child());

        // New MATCH virtual column is appended
        Alias secondVc = (Alias) resScan.getVirtualColumns().get(1);
        Assertions.assertInstanceOf(MatchAny.class, secondVc.child());
        Assertions.assertEquals(matchExpr, secondVc.child());
    }
}

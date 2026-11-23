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

package org.apache.doris.nereids.rules.exploration;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.cascades.ApplyRuleJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.MemoValidator;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

class MergeProjectsCBOTest {
    private final ConnectContext connectContext = MemoTestUtils.createConnectContext();
    private final LogicalOlapScan scan = new LogicalOlapScan(
            StatementScopeIdGenerator.newRelationId(), PlanConstructor.student);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIndirectDuplicateInEitherAlternativeOrder(boolean reverseAlternatives) {
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        LogicalProject<?> bottom = new LogicalProject<>(ImmutableList.of(slot), scan);
        LogicalProject<?> middle = new LogicalProject<>(
                ImmutableList.of(new Alias(new TinyIntLiteral((byte) 1)), slot), bottom);
        CascadesContext cascades = MemoTestUtils.createCascadesContext(connectContext,
                new LogicalProject<>(ImmutableList.of(slot), middle));
        Memo memo = cascades.getMemo();
        Group root = memo.getRoot();
        Group middleGroup = root.getLogicalExpression().child(0);
        Group bottomGroup = middleGroup.getLogicalExpression().child(0);
        Rule rule = new MergeProjectsCBO().build();

        new ApplyRuleJob(middleGroup.getLogicalExpression(), rule, cascades.getCurrentJobContext()).execute();
        Assertions.assertEquals(2, middleGroup.getLogicalExpressions().size());
        if (reverseAlternatives) {
            Collections.reverse(middleGroup.getLogicalExpressions());
        }
        new ApplyRuleJob(root.getLogicalExpression(), rule, cascades.getCurrentJobContext()).execute();

        MemoValidator.validate(memo);
        Assertions.assertSame(root, memo.getRoot());
        Assertions.assertEquals(4, memo.getGroups().size());
        Assertions.assertEquals(2, root.getLogicalExpressions().size());
        Assertions.assertTrue(root.getLogicalExpressions().stream()
                .anyMatch(expression -> expression.child(0) == bottomGroup));
        Assertions.assertEquals(6, memo.getGroupExpressionsSize());
    }

    @Test
    void testNewMergedProjectIsStillAdded() {
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        LogicalProject<?> middle = new LogicalProject<>(
                ImmutableList.of(new Alias(new TinyIntLiteral((byte) 1)), slot), scan);
        CascadesContext cascades = MemoTestUtils.createCascadesContext(connectContext,
                new LogicalProject<>(ImmutableList.of(slot), middle));
        Memo memo = cascades.getMemo();
        Group root = memo.getRoot();
        Group scanGroup = root.getLogicalExpression().child(0).getLogicalExpression().child(0);

        new ApplyRuleJob(root.getLogicalExpression(), new MergeProjectsCBO().build(),
                cascades.getCurrentJobContext()).execute();

        Assertions.assertEquals(2, root.getLogicalExpressions().size());
        Assertions.assertTrue(root.getLogicalExpressions().stream()
                .anyMatch(expression -> expression.child(0) == scanGroup));
        MemoValidator.validate(memo);
    }

    @Test
    void testRegisteredProjectDoesNotUnifyIndependentGroups() {
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        LogicalProject<?> middle = new LogicalProject<>(
                ImmutableList.of(new Alias(new TinyIntLiteral((byte) 1)), slot), scan);
        CascadesContext cascades = MemoTestUtils.createCascadesContext(connectContext,
                new LogicalProject<>(ImmutableList.of(slot), middle));
        Memo memo = cascades.getMemo();
        Group root = memo.getRoot();
        Group scanGroup = root.getLogicalExpression().child(0).getLogicalExpression().child(0);
        GroupExpression registered = memo.copyIn(new LogicalProject<>(ImmutableList.of(slot),
                new GroupPlan(scanGroup)), null, false).correspondingExpression;

        new ApplyRuleJob(root.getLogicalExpression(), new MergeProjectsCBO().build(),
                cascades.getCurrentJobContext()).execute();

        Assertions.assertSame(root, memo.getRoot());
        Assertions.assertEquals(1, root.getLogicalExpressions().size());
        Assertions.assertEquals(4, memo.getGroups().size());
        Assertions.assertSame(registered, memo.getGroupExpressions().get(registered));
        MemoValidator.validate(memo);
    }

    @Test
    void testDifferentProjectionWithSameHashIsNotSkipped() {
        LogicalProject<?> middle = new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), scan);
        LogicalProject<?> top = new LogicalProject<>(
                ImmutableList.of(scan.getOutput().get(0), scan.getOutput().get(3)), middle);
        CascadesContext cascades = MemoTestUtils.createCascadesContext(connectContext, top);
        Memo memo = cascades.getMemo();
        Group root = memo.getRoot();
        Group scanGroup = root.getLogicalExpression().child(0).getLogicalExpression().child(0);
        LogicalProject<?> collision = new LogicalProject<>(
                ImmutableList.of(scan.getOutput().get(1), scan.getOutput().get(2)), new GroupPlan(scanGroup));
        LogicalProject<?> merged = new LogicalProject<>(top.getProjects(), new GroupPlan(scanGroup));
        Assertions.assertEquals(collision.hashCode(), merged.hashCode());
        Assertions.assertNotEquals(collision, merged);
        memo.copyIn(collision, null, false);

        new ApplyRuleJob(root.getLogicalExpression(), new MergeProjectsCBO().build(),
                cascades.getCurrentJobContext()).execute();

        Assertions.assertEquals(2, root.getLogicalExpressions().size());
        Assertions.assertTrue(root.getLogicalExpressions().stream()
                .anyMatch(expression -> expression.child(0) == scanGroup));
        MemoValidator.validate(memo);
    }

    @Test
    void testOrdinaryCopyInStillMergesGroups() {
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        LogicalProject<?> middle = new LogicalProject<>(
                ImmutableList.of(new Alias(new TinyIntLiteral((byte) 1)), slot), scan);
        CascadesContext cascades = MemoTestUtils.createCascadesContext(connectContext,
                new LogicalProject<>(ImmutableList.of(slot), middle));
        Memo memo = cascades.getMemo();
        Group oldRoot = memo.getRoot();
        Group scanGroup = oldRoot.getLogicalExpression().child(0).getLogicalExpression().child(0);
        LogicalProject<?> merged = new LogicalProject<>(ImmutableList.of(slot), new GroupPlan(scanGroup));
        GroupExpression registered = memo.copyIn(merged, null, false).correspondingExpression;

        memo.copyIn(merged, oldRoot, false);

        Assertions.assertSame(registered.getOwnerGroup(), memo.getRoot());
        Assertions.assertNull(memo.getGroup(oldRoot.getGroupId()));
        MemoValidator.validate(memo);
    }
}

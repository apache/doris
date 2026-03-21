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

import org.apache.doris.catalog.KeysType;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Test for PushDownMatchPredicateAsVirtualColumn rule.
 */
public class PushDownMatchPredicateAsVirtualColumnTest implements MemoPatternMatchSupported {

    /**
     * Pattern 1: Filter -> Join -> Project -> OlapScan
     * WHERE (CAST(name) MATCH_ANY 'hello' OR right.col IS NOT NULL)
     * Should push MATCH as virtual column on scan and replace in filter predicate.
     */
    @Test
    void testPattern1FilterJoinProjectScan() {
        // Left side: scan with Project[id, CAST(name) as fn]
        LogicalOlapScan leftScan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> leftSlots = leftScan.getOutput();
        Slot idSlot = leftSlots.get(0);
        Slot nameSlot = leftSlots.get(1);

        // CAST(name AS STRING) as fn — this loses originalColumn metadata on the alias slot
        Cast castExpr = new Cast(nameSlot, StringType.INSTANCE);
        Alias fnAlias = new Alias(castExpr, "fn");
        LogicalProject<LogicalOlapScan> leftProject = new LogicalProject<>(
                ImmutableList.of(idSlot, fnAlias), leftScan);

        // Right side: another scan
        LogicalOlapScan rightScan = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        Slot rightIdSlot = rightScan.getOutput().get(0);

        // Join: LEFT_OUTER
        LogicalJoin<LogicalProject<LogicalOlapScan>, LogicalOlapScan> join = new LogicalJoin<>(
                JoinType.LEFT_OUTER_JOIN, leftProject, rightScan, new JoinReorderContext());

        // Filter: fn MATCH_ANY 'hello' OR rightId IS NOT NULL
        Slot fnSlot = fnAlias.toSlot();
        MatchAny matchExpr = new MatchAny(fnSlot, new StringLiteral("hello"));
        Or orPredicate = new Or(matchExpr, new Not(new IsNull(rightIdSlot)));

        LogicalFilter<?> filter = new LogicalFilter<>(ImmutableSet.of(orPredicate), join);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new PushDownMatchPredicateAsVirtualColumn())
                .getPlan();

        // Verify plan structure: Filter -> Join -> Project -> OlapScan
        Assertions.assertInstanceOf(LogicalFilter.class, root);
        LogicalFilter<?> resFilter = (LogicalFilter<?>) root;
        Assertions.assertInstanceOf(LogicalJoin.class, resFilter.child());
        LogicalJoin<?, ?> resJoin = (LogicalJoin<?, ?>) resFilter.child();
        Assertions.assertInstanceOf(LogicalProject.class, resJoin.left());
        LogicalProject<?> resProject = (LogicalProject<?>) resJoin.left();
        Assertions.assertInstanceOf(LogicalOlapScan.class, resProject.child());
        LogicalOlapScan resScan = (LogicalOlapScan) resProject.child();

        // Verify virtual column was created on scan
        Assertions.assertEquals(1, resScan.getVirtualColumns().size());
        Alias vcAlias = (Alias) resScan.getVirtualColumns().get(0);
        Assertions.assertInstanceOf(MatchAny.class, vcAlias.child());

        // Verify the MATCH in the virtual column uses the original CAST(name) expression, not the alias slot
        MatchAny vcMatch = (MatchAny) vcAlias.child();
        Assertions.assertInstanceOf(Cast.class, vcMatch.left());

        // Verify project has the virtual column slot appended
        Assertions.assertEquals(3, resProject.getProjects().size());

        // Verify filter predicate replaced MATCH with slot reference
        Expression resPredicate = resFilter.getConjuncts().iterator().next();
        Assertions.assertInstanceOf(Or.class, resPredicate);
        Or resOr = (Or) resPredicate;
        Assertions.assertInstanceOf(SlotReference.class, resOr.child(0));
    }

    /**
     * Pattern 2: Filter -> Join -> Project -> Filter -> OlapScan
     */
    @Test
    void testPattern2FilterJoinProjectFilterScan() {
        LogicalOlapScan leftScan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> leftSlots = leftScan.getOutput();
        Slot idSlot = leftSlots.get(0);
        Slot nameSlot = leftSlots.get(1);

        // Inner filter on scan
        GreaterThan innerPred = new GreaterThan(idSlot, new IntegerLiteral(0));
        LogicalFilter<LogicalOlapScan> innerFilter = new LogicalFilter<>(
                ImmutableSet.of(innerPred), leftScan);

        Cast castExpr = new Cast(nameSlot, StringType.INSTANCE);
        Alias fnAlias = new Alias(castExpr, "fn");
        LogicalProject<LogicalFilter<LogicalOlapScan>> leftProject = new LogicalProject<>(
                ImmutableList.of(idSlot, fnAlias), innerFilter);

        LogicalOlapScan rightScan = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        Slot rightIdSlot = rightScan.getOutput().get(0);

        LogicalJoin<?, ?> join = new LogicalJoin<>(
                JoinType.LEFT_OUTER_JOIN, leftProject, rightScan, new JoinReorderContext());

        Slot fnSlot = fnAlias.toSlot();
        MatchAny matchExpr = new MatchAny(fnSlot, new StringLiteral("hello"));
        Or orPredicate = new Or(matchExpr, new Not(new IsNull(rightIdSlot)));
        LogicalFilter<?> outerFilter = new LogicalFilter<>(ImmutableSet.of(orPredicate), join);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), outerFilter)
                .applyTopDown(new PushDownMatchPredicateAsVirtualColumn())
                .getPlan();

        // Verify structure: Filter -> Join -> Project -> Filter -> OlapScan
        Assertions.assertInstanceOf(LogicalFilter.class, root);
        LogicalFilter<?> resFilter = (LogicalFilter<?>) root;
        LogicalJoin<?, ?> resJoin = (LogicalJoin<?, ?>) resFilter.child();
        LogicalProject<?> resProject = (LogicalProject<?>) resJoin.left();
        Assertions.assertInstanceOf(LogicalFilter.class, resProject.child());
        LogicalFilter<?> resInnerFilter = (LogicalFilter<?>) resProject.child();
        LogicalOlapScan resScan = (LogicalOlapScan) resInnerFilter.child();

        // Virtual column on scan, inner filter preserved
        Assertions.assertEquals(1, resScan.getVirtualColumns().size());
        Assertions.assertEquals(ImmutableSet.of(innerPred), resInnerFilter.getConjuncts());
    }

    /**
     * When slot has originalColumn and originalTable (metadata intact),
     * the rule should NOT trigger — no pushdown needed.
     */
    @Test
    void testMetadataIntactSkipsPushDown() {
        LogicalOlapScan leftScan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> leftSlots = leftScan.getOutput();
        Slot idSlot = leftSlots.get(0);
        Slot nameSlot = leftSlots.get(1);

        // Project directly passes through nameSlot (no CAST wrapper) — metadata preserved
        LogicalProject<LogicalOlapScan> leftProject = new LogicalProject<>(
                ImmutableList.of(idSlot, nameSlot), leftScan);

        LogicalOlapScan rightScan = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        Slot rightIdSlot = rightScan.getOutput().get(0);

        LogicalJoin<?, ?> join = new LogicalJoin<>(
                JoinType.LEFT_OUTER_JOIN, leftProject, rightScan, new JoinReorderContext());

        // MATCH on nameSlot which has full metadata
        MatchAny matchExpr = new MatchAny(nameSlot, new StringLiteral("hello"));
        Or orPredicate = new Or(matchExpr, new Not(new IsNull(rightIdSlot)));
        LogicalFilter<?> filter = new LogicalFilter<>(ImmutableSet.of(orPredicate), join);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new PushDownMatchPredicateAsVirtualColumn())
                .getPlan();

        // Rule should not trigger — no virtual columns added
        LogicalFilter<?> resFilter = (LogicalFilter<?>) root;
        LogicalJoin<?, ?> resJoin = (LogicalJoin<?, ?>) resFilter.child();
        LogicalProject<?> resProject = (LogicalProject<?>) resJoin.left();
        LogicalOlapScan resScan = (LogicalOlapScan) resProject.child();
        Assertions.assertTrue(resScan.getVirtualColumns().isEmpty());
    }

    /**
     * Non-DUP_KEYS/non-MOW table should not trigger the rule.
     */
    @Test
    void testNonDupKeysTableSkipsPushDown() {
        // AGG_KEYS table
        LogicalOlapScan leftScan = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
                PlanConstructor.newOlapTable(0, "t_agg", 0, KeysType.AGG_KEYS),
                ImmutableList.of("db"));
        List<Slot> leftSlots = leftScan.getOutput();
        Slot idSlot = leftSlots.get(0);
        Slot nameSlot = leftSlots.get(1);

        Cast castExpr = new Cast(nameSlot, StringType.INSTANCE);
        Alias fnAlias = new Alias(castExpr, "fn");
        LogicalProject<LogicalOlapScan> leftProject = new LogicalProject<>(
                ImmutableList.of(idSlot, fnAlias), leftScan);

        LogicalOlapScan rightScan = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        Slot rightIdSlot = rightScan.getOutput().get(0);

        LogicalJoin<?, ?> join = new LogicalJoin<>(
                JoinType.LEFT_OUTER_JOIN, leftProject, rightScan, new JoinReorderContext());

        Slot fnSlot = fnAlias.toSlot();
        MatchAny matchExpr = new MatchAny(fnSlot, new StringLiteral("hello"));
        Or orPredicate = new Or(matchExpr, new Not(new IsNull(rightIdSlot)));
        LogicalFilter<?> filter = new LogicalFilter<>(ImmutableSet.of(orPredicate), join);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new PushDownMatchPredicateAsVirtualColumn())
                .getPlan();

        // Rule should not trigger for AGG_KEYS table
        LogicalFilter<?> resFilter = (LogicalFilter<?>) root;
        LogicalJoin<?, ?> resJoin = (LogicalJoin<?, ?>) resFilter.child();
        LogicalProject<?> resProject = (LogicalProject<?>) resJoin.left();
        LogicalOlapScan resScan = (LogicalOlapScan) resProject.child();
        Assertions.assertTrue(resScan.getVirtualColumns().isEmpty());
    }

    /**
     * Scan already has existing virtual columns — new ones should be appended, not replace them.
     */
    @Test
    void testAppendToExistingVirtualColumns() {
        LogicalOlapScan leftScan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> leftSlots = leftScan.getOutput();
        Slot idSlot = leftSlots.get(0);
        Slot nameSlot = leftSlots.get(1);

        // Pre-existing virtual column
        Alias existingVc = new Alias(new GreaterThan(idSlot, new IntegerLiteral(5)), "cse_vc");
        LogicalOlapScan scanWithVc = leftScan.withVirtualColumns(ImmutableList.of(existingVc));

        Cast castExpr = new Cast(nameSlot, StringType.INSTANCE);
        Alias fnAlias = new Alias(castExpr, "fn");
        LogicalProject<LogicalOlapScan> leftProject = new LogicalProject<>(
                ImmutableList.of(idSlot, fnAlias), scanWithVc);

        LogicalOlapScan rightScan = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        Slot rightIdSlot = rightScan.getOutput().get(0);

        LogicalJoin<?, ?> join = new LogicalJoin<>(
                JoinType.LEFT_OUTER_JOIN, leftProject, rightScan, new JoinReorderContext());

        Slot fnSlot = fnAlias.toSlot();
        MatchAny matchExpr = new MatchAny(fnSlot, new StringLiteral("hello"));
        Or orPredicate = new Or(matchExpr, new Not(new IsNull(rightIdSlot)));
        LogicalFilter<?> filter = new LogicalFilter<>(ImmutableSet.of(orPredicate), join);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new PushDownMatchPredicateAsVirtualColumn())
                .getPlan();

        // Verify both virtual columns exist
        LogicalFilter<?> resFilter = (LogicalFilter<?>) root;
        LogicalJoin<?, ?> resJoin = (LogicalJoin<?, ?>) resFilter.child();
        LogicalProject<?> resProject = (LogicalProject<?>) resJoin.left();
        LogicalOlapScan resScan = (LogicalOlapScan) resProject.child();
        Assertions.assertEquals(2, resScan.getVirtualColumns().size());

        // Existing one preserved
        Alias firstVc = (Alias) resScan.getVirtualColumns().get(0);
        Assertions.assertInstanceOf(GreaterThan.class, firstVc.child());

        // New MATCH one appended
        Alias secondVc = (Alias) resScan.getVirtualColumns().get(1);
        Assertions.assertInstanceOf(MatchAny.class, secondVc.child());
    }

    /**
     * Pattern 3: Join(otherPredicates has MATCH) -> Project -> OlapScan
     */
    @Test
    void testPattern3JoinOtherPredicatesProjectScan() {
        LogicalOlapScan leftScan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> leftSlots = leftScan.getOutput();
        Slot idSlot = leftSlots.get(0);
        Slot nameSlot = leftSlots.get(1);

        Cast castExpr = new Cast(nameSlot, StringType.INSTANCE);
        Alias fnAlias = new Alias(castExpr, "fn");
        LogicalProject<LogicalOlapScan> leftProject = new LogicalProject<>(
                ImmutableList.of(idSlot, fnAlias), leftScan);

        LogicalOlapScan rightScan = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        Slot rightIdSlot = rightScan.getOutput().get(0);

        // MATCH in join's otherJoinConjuncts
        Slot fnSlot = fnAlias.toSlot();
        MatchAny matchExpr = new MatchAny(fnSlot, new StringLiteral("hello"));
        Or orOther = new Or(matchExpr, new Not(new IsNull(rightIdSlot)));

        LogicalJoin<LogicalProject<LogicalOlapScan>, LogicalOlapScan> join = new LogicalJoin<>(
                JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(),
                ImmutableList.of(orOther),
                leftProject, rightScan, new JoinReorderContext());

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new PushDownMatchPredicateAsVirtualColumn())
                .getPlan();

        // Verify: Join -> Project -> OlapScan with virtual column
        Assertions.assertInstanceOf(LogicalJoin.class, root);
        LogicalJoin<?, ?> resJoin = (LogicalJoin<?, ?>) root;
        LogicalProject<?> resProject = (LogicalProject<?>) resJoin.left();
        LogicalOlapScan resScan = (LogicalOlapScan) resProject.child();
        Assertions.assertEquals(1, resScan.getVirtualColumns().size());

        // Verify MATCH in otherJoinConjuncts was replaced with slot
        List<Expression> resOther = resJoin.getOtherJoinConjuncts();
        Assertions.assertEquals(1, resOther.size());
        Or resOr = (Or) resOther.get(0);
        Assertions.assertInstanceOf(SlotReference.class, resOr.child(0));
    }
}

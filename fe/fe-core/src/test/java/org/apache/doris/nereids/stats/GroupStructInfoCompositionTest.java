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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unit test of the contract that a memo group is described once per query and that its strings are
 * materialized on demand: the description of a group is memoized on it (see
 * {@link GroupStructInfo#describeGroup(Group)}), a parent composes the descriptions of its children
 * through the rope, and materializing a parent copies the string of a child which already has one
 * instead of walking that child's sub tree again.
 */
public class GroupStructInfoCompositionTest {

    private static final GroupStructInfo.LiteralMode NO_LITERAL = GroupStructInfo.LiteralMode.NO_LITERAL;
    private static final GroupStructInfo.LiteralMode WITH_LITERAL = GroupStructInfo.LiteralMode.WITH_LITERAL;

    /** A filter over a scan, joined with two more scans: leaves and chain members in one memo. */
    private Memo joinChainMemo() {
        LogicalOlapScan t1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan t2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        LogicalOlapScan t3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(ImmutableSet.of(), t1);
        LogicalJoin<LogicalFilter<LogicalOlapScan>, LogicalOlapScan> inner =
                new LogicalJoin<>(JoinType.INNER_JOIN, filter, t2, null);
        LogicalJoin<LogicalJoin<LogicalFilter<LogicalOlapScan>, LogicalOlapScan>, LogicalOlapScan> root =
                new LogicalJoin<>(JoinType.INNER_JOIN, inner, t3, null);
        return new Memo(null, root);
    }

    /** A join whose right side is an aggregation: the aggregation is a leaf of the join chain. */
    private Memo joinOverAggregateMemo() {
        LogicalOlapScan t1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan t2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        LogicalOlapScan t3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> inner =
                new LogicalJoin<>(JoinType.INNER_JOIN, t2, t3, null);
        LogicalAggregate<LogicalJoin<LogicalOlapScan, LogicalOlapScan>> aggregate =
                new LogicalAggregate<>(ImmutableList.of(), ImmutableList.of(), true, Optional.empty(), inner);
        return new Memo(null, new LogicalJoin<>(JoinType.INNER_JOIN, t1, aggregate, null));
    }

    @Test
    void testAGroupIsNeverDescribedTwice() {
        Memo memo = joinChainMemo();
        GroupStructInfo.resetDescribedGroupCount();
        Map<Group, GroupStructInfo> first = new LinkedHashMap<>();
        for (Group group : memo.getGroups()) {
            first.put(group, GroupStructInfo.describeGroup(group));
        }
        int describedOnce = GroupStructInfo.getDescribedGroupCount();
        Assertions.assertTrue(describedOnce > 0, "no group was described");
        Assertions.assertTrue(describedOnce <= memo.getGroups().size(),
                "a group was described more than once in the first pass");
        // the same object comes back: every later request is answered from the memo of that group
        for (Group group : memo.getGroups()) {
            GroupStructInfo.of(group);
            Assertions.assertSame(first.get(group), GroupStructInfo.describeGroup(group));
            first.get(group).getFingerprint(WITH_LITERAL);
            first.get(group).getFingerprint(NO_LITERAL);
        }
        Assertions.assertEquals(describedOnce, GroupStructInfo.getDescribedGroupCount(),
                "a group was described again after it had been built");
    }

    private static String tokenOf(String canonical) {
        int start = canonical.indexOf("S{");
        Assertions.assertTrue(start >= 0, "no scan token in " + canonical);
        return canonical.substring(start + 2, canonical.indexOf('}', start));
    }

    @Test
    void testTheFingerprintIsKeyedOnTheStructureNotOnTheText() {
        Memo memo = joinChainMemo();
        GroupStructInfo root = GroupStructInfo.describeGroup(memo.getRoot());
        Assertions.assertTrue(root.isValid(), root.getSkipReason());
        String shape = root.getShape(NO_LITERAL);
        Assertions.assertFalse(shape.isEmpty(), "the shape of the root is empty");
        // the key is the structural identity of the description: the shape text is its readable
        // projection, and a table name which carries a character of the printed grammar moves the text
        // but not the key (see HboScanDescriptorIdentityTest)
        Assertions.assertEquals(64, root.getFingerprint(NO_LITERAL).length());
        Assertions.assertNotEquals(Hashing.sha256().hashString(shape, StandardCharsets.UTF_8).toString(),
                root.getFingerprint(NO_LITERAL),
                "the fingerprint is still the sha256 of the shape text");
        // the canonical string carries the data state of the scans, the shape does not: the token of the
        // shape is the table alone (the fingerprint must not move when rows are loaded)
        Assertions.assertTrue(root.getCanonical(NO_LITERAL).contains("S{"),
                "the canonical string has no scan token");
        Assertions.assertFalse(tokenOf(shape).contains(","),
                "the shape of a scan carries a data state: " + tokenOf(shape));
        Assertions.assertTrue(tokenOf(root.getCanonical(NO_LITERAL)).contains(","),
                "the canonical string of a scan carries no data state: "
                        + tokenOf(root.getCanonical(NO_LITERAL)));
    }

    @Test
    void testAParentComposesTheCanonicalOfItsChildren() {
        Memo memo = joinOverAggregateMemo();
        GroupStructInfo root = GroupStructInfo.describeGroup(memo.getRoot());
        Assertions.assertTrue(root.isValid(), root.getSkipReason());
        String rootCanonical = root.getCanonical(NO_LITERAL);
        String rootShape = root.getShape(NO_LITERAL);
        int contained = 0;
        for (Group group : memo.getGroups()) {
            GroupStructInfo child = GroupStructInfo.describeGroup(group);
            String canonical = child.getCanonical(NO_LITERAL);
            if (canonical.equals(rootCanonical)) {
                continue;
            }
            if (rootCanonical.contains(canonical)) {
                contained++;
                Assertions.assertTrue(rootShape.contains(child.getShape(NO_LITERAL)),
                        "the shape of the parent does not contain the shape of its child");
            }
        }
        // the whole sub tree of the aggregation (its own canonical and the two scans) appears
        // literally inside the canonical of the join: it is composed, not described again
        Assertions.assertTrue(contained >= 2,
                "the canonical of the parent does not contain the canonical of its children");
    }

    @Test
    void testAMaterializedChildIsNotWalkedAgain() {
        Memo memo = joinOverAggregateMemo();
        GroupStructInfo root = GroupStructInfo.describeGroup(memo.getRoot());
        // materialize every other description first, the same way the read side asks for the
        // fingerprint of a node before the explain annotation prints the plan
        for (Group group : memo.getGroups()) {
            if (group != memo.getRoot()) {
                GroupStructInfo.describeGroup(group).getCanonical(NO_LITERAL);
            }
        }
        GroupStructInfo.resetMaterializedGroupCount();
        root.getCanonical(NO_LITERAL);
        Assertions.assertEquals(1, GroupStructInfo.getMaterializedGroupCount(),
                "materializing the root walked the sub tree of an already materialized child");
        // repeating the request answers from the cache of the root itself
        GroupStructInfo.resetMaterializedGroupCount();
        root.getCanonical(NO_LITERAL);
        Assertions.assertEquals(0, GroupStructInfo.getMaterializedGroupCount(),
                "a materialized string was built again");
        // and the two forms are cached independently: asking for the shape still walks the rope
        Assertions.assertTrue(root.getShape(NO_LITERAL).startsWith("J{inner"),
                root.getShape(NO_LITERAL));
    }

    @Test
    void testASetOperationSortsItsBranchesButKeepsTheMinuend() {
        LogicalOlapScan t1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan t2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        Memo union = new Memo(null, new LogicalUnion(SetOperation.Qualifier.ALL,
                ImmutableList.of(t2, t1)));
        GroupStructInfo unionInfo = GroupStructInfo.describeGroup(union.getRoot());
        Assertions.assertTrue(unionInfo.isValid(), unionInfo.getSkipReason());
        String unionShape = unionInfo.getShape(NO_LITERAL);
        Assertions.assertTrue(unionShape.startsWith("U{all,cols:["), unionShape);
        // the memo reorders the branches of a set operation, so the shape has to be insensitive to it
        String unionBranches = unionShape.substring(unionShape.indexOf("]}("));
        Assertions.assertTrue(unionBranches.indexOf("t1") < unionBranches.indexOf("t2"), unionShape);

        Memo except = new Memo(null, new LogicalExcept(SetOperation.Qualifier.ALL,
                ImmutableList.of(t2, t1)));
        GroupStructInfo exceptInfo = GroupStructInfo.describeGroup(except.getRoot());
        Assertions.assertTrue(exceptInfo.isValid(), exceptInfo.getSkipReason());
        String exceptShape = exceptInfo.getShape(NO_LITERAL);
        Assertions.assertTrue(exceptShape.startsWith("E{all,cols:["), exceptShape);
        // the first branch is the minuend: it keeps its position, only the subtrahends are sorted
        String exceptBranches = exceptShape.substring(exceptShape.indexOf("]}("));
        Assertions.assertTrue(exceptBranches.indexOf("t2") < exceptBranches.indexOf("t1"), exceptShape);
    }

    @Test
    void testALimitBoundIsNotFoldedAway() {
        LogicalOlapScan t1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan t2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        LogicalJoin<LogicalLimit<LogicalOlapScan>, LogicalOlapScan> five = new LogicalJoin<>(
                JoinType.INNER_JOIN, new LogicalLimit<>(5, 0, LimitPhase.ORIGIN, t1), t2, null);
        LogicalJoin<LogicalLimit<LogicalOlapScan>, LogicalOlapScan> seven = new LogicalJoin<>(
                JoinType.INNER_JOIN, new LogicalLimit<>(7, 0, LimitPhase.ORIGIN, t1), t2, null);
        GroupStructInfo fiveInfo = GroupStructInfo.describeGroup(new Memo(null, five).getRoot());
        GroupStructInfo sevenInfo = GroupStructInfo.describeGroup(new Memo(null, seven).getRoot());
        Assertions.assertTrue(fiveInfo.isValid(), fiveInfo.getSkipReason());
        Assertions.assertTrue(fiveInfo.getShape(NO_LITERAL).contains("L{5,0}"),
                fiveInfo.getShape(NO_LITERAL));
        // the bound changes the row count, so the constant agnostic mode must not fold it away
        Assertions.assertNotEquals(fiveInfo.getFingerprint(NO_LITERAL), sevenInfo.getFingerprint(NO_LITERAL),
                "two different limits share one key");
        Assertions.assertEquals(fiveInfo.getFingerprint(WITH_LITERAL),
                fiveInfo.getFingerprint(NO_LITERAL),
                "a bound is written with the same text in both modes");
    }

    @Test
    void testTheChainInfoOfAChainMemberIsMemoized() {
        Memo memo = joinChainMemo();
        GroupStructInfo root = GroupStructInfo.describeGroup(memo.getRoot());
        Assertions.assertTrue(root.isValid(), root.getSkipReason());
        // the chain of the members below the root is collected once and remembered on those groups: the
        // inner join is the root of a chain of its own, a leaf group never is
        Group innerJoin = memo.getGroups().stream()
                .filter(group -> group != memo.getRoot())
                .filter(group -> !group.getLogicalExpressions().isEmpty())
                .filter(group -> group.getLogicalExpressions().get(0).getPlan() instanceof LogicalJoin)
                .findFirst()
                .orElseThrow(() -> new AssertionError("the memo has no inner join group"));
        Assertions.assertNotNull(innerJoin.getHboChainInfoOrNull(),
                "the inner join memoized no chain although it is the root of one");
    }

    @Test
    void testTheStructInfoDoesNotDependOnTheChosenExpression() {
        GroupStructInfo withoutChoice = GroupStructInfo.describeGroup(joinChainMemo().getRoot());
        Memo chosenMemo = joinChainMemo();
        // the plan chooser records the id of the expression it picked; the struct info is built from the
        // first logical expression, so the key of a group does not move when a plan is chosen (the read
        // side looks the key up while the plan is still being optimized, before anything is chosen)
        chosenMemo.getRoot().setChosenGroupExpressionId(9999);
        // the gate: an implementation which prefers the recorded choice would return the chosen
        // expression, or null when its id resolves to nothing (9999 is no expression of the group)
        Assertions.assertSame(chosenMemo.getRoot().getLogicalExpressions().get(0),
                GroupStructInfo.getRepresentativeExpression(chosenMemo.getRoot()),
                "the described expression of a group follows the recorded choice");
        GroupStructInfo withChoice = GroupStructInfo.describeGroup(chosenMemo.getRoot());
        Assertions.assertTrue(withoutChoice.isValid(), withoutChoice.getSkipReason());
        Assertions.assertTrue(withChoice.isValid(), withChoice.getSkipReason());
        Assertions.assertEquals(withoutChoice.getCanonical(NO_LITERAL), withChoice.getCanonical(NO_LITERAL));
        Assertions.assertEquals(withoutChoice.getFingerprint(NO_LITERAL),
                withChoice.getFingerprint(NO_LITERAL));
    }

    @Test
    void testTheOrderingOfATopNIsPartOfTheKeyAboveIt() {
        // with t keys 1 and 2 and a partner table which only holds 1, `topn(1, a asc)` keeps the row which
        // matches and `topn(1, a desc)` keeps the one which does not: the two joins see a different number
        // of rows, so the key of the join has to differ even though both top-ns keep one row
        LogicalOlapScan t1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan t2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        Expression a = t1.getOutput().get(0);
        LogicalJoin<LogicalTopN<LogicalOlapScan>, LogicalOlapScan> ascending = new LogicalJoin<>(
                JoinType.INNER_JOIN,
                new LogicalTopN<>(ImmutableList.of(new OrderKey(a, true, true)), 1, 0, t1), t2, null);
        LogicalJoin<LogicalTopN<LogicalOlapScan>, LogicalOlapScan> descending = new LogicalJoin<>(
                JoinType.INNER_JOIN,
                new LogicalTopN<>(ImmutableList.of(new OrderKey(a, false, true)), 1, 0, t1), t2, null);
        GroupStructInfo ascendingInfo = GroupStructInfo.describeGroup(new Memo(null, ascending).getRoot());
        GroupStructInfo descendingInfo = GroupStructInfo.describeGroup(new Memo(null, descending).getRoot());
        Assertions.assertTrue(ascendingInfo.isValid(), ascendingInfo.getSkipReason());
        Assertions.assertTrue(ascendingInfo.getShape(NO_LITERAL).contains("T{1,0;ord:["),
                ascendingInfo.getShape(NO_LITERAL));
        Assertions.assertTrue(ascendingInfo.getShape(NO_LITERAL).contains(":asc:nulls_first]"),
                ascendingInfo.getShape(NO_LITERAL));
        Assertions.assertNotEquals(ascendingInfo.getFingerprint(NO_LITERAL),
                descendingInfo.getFingerprint(NO_LITERAL), "asc and desc share one key");
        // the null order is part of the ordering as well
        LogicalJoin<LogicalTopN<LogicalOlapScan>, LogicalOlapScan> nullsLast = new LogicalJoin<>(
                JoinType.INNER_JOIN,
                new LogicalTopN<>(ImmutableList.of(new OrderKey(a, true, false)), 1, 0, t1), t2, null);
        Assertions.assertNotEquals(ascendingInfo.getFingerprint(NO_LITERAL),
                GroupStructInfo.describeGroup(new Memo(null, nullsLast).getRoot()).getFingerprint(NO_LITERAL),
                "nulls first and nulls last share one key");
        // a top-n of the same bound and ordering keeps the key it had
        LogicalJoin<LogicalTopN<LogicalOlapScan>, LogicalOlapScan> same = new LogicalJoin<>(
                JoinType.INNER_JOIN,
                new LogicalTopN<>(ImmutableList.of(new OrderKey(a, true, true)), 1, 0, t1), t2, null);
        Assertions.assertEquals(ascendingInfo.getFingerprint(NO_LITERAL),
                GroupStructInfo.describeGroup(new Memo(null, same).getRoot()).getFingerprint(NO_LITERAL));
    }

    @Test
    void testTheSubPathOfAVariantSlotIsPartOfTheKey() {
        // after the variant sub path pruning, `v['age']` and `v['score']` are two slots which read
        // different values of the same column: a predicate over them keeps a different number of rows,
        // while the column name alone is the same for both
        LogicalOlapScan t = PlanConstructor.newLogicalOlapScan(0, "t", 0);
        SlotReference v = (SlotReference) t.getOutput().get(0);
        LogicalFilter<LogicalOlapScan> age = new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(v.withSubPath(ImmutableList.of("age")), new IntegerLiteral(30))), t);
        LogicalFilter<LogicalOlapScan> score = new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(v.withSubPath(ImmutableList.of("score")), new IntegerLiteral(30))), t);
        GroupStructInfo ageInfo = GroupStructInfo.describeGroup(new Memo(null, age).getRoot());
        GroupStructInfo scoreInfo = GroupStructInfo.describeGroup(new Memo(null, score).getRoot());
        Assertions.assertTrue(ageInfo.isValid(), ageInfo.getSkipReason());
        Assertions.assertTrue(ageInfo.getShape(WITH_LITERAL).contains("['age']"),
                ageInfo.getShape(WITH_LITERAL));
        // the sub path is structure, not a literal: the constant agnostic mode keeps it as well
        Assertions.assertTrue(ageInfo.getShape(NO_LITERAL).contains("['age']"),
                ageInfo.getShape(NO_LITERAL));
        Assertions.assertNotEquals(ageInfo.getFingerprint(NO_LITERAL), scoreInfo.getFingerprint(NO_LITERAL),
                "two sub paths of one variant column share one key");
        // a nested sub path is rendered part by part
        LogicalFilter<LogicalOlapScan> nested = new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(v.withSubPath(ImmutableList.of("age", "min")), new IntegerLiteral(30))), t);
        Assertions.assertTrue(GroupStructInfo.describeGroup(new Memo(null, nested).getRoot())
                .getShape(NO_LITERAL).contains("['age']['min']"));
    }

    @Test
    void testTheConstantBranchesOfAUnionArePartOfTheKey() {
        // MergeOneRowRelationIntoUnion takes the constant branches out of the children of the union, so
        // only the header can carry them: two distinct unions which differ only in their constants keep a
        // different number of rows (`select 1 union select 2` against `select 1 union select 1`)
        LogicalOlapScan t1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<List<NamedExpression>> twoBranches = new ArrayList<>();
        twoBranches.add(ImmutableList.<NamedExpression>of(new Alias(new IntegerLiteral(1), "c1")));
        twoBranches.add(ImmutableList.<NamedExpression>of(new Alias(new IntegerLiteral(2), "c1")));
        List<List<NamedExpression>> oneBranch = new ArrayList<>();
        oneBranch.add(ImmutableList.<NamedExpression>of(new Alias(new IntegerLiteral(1), "c1")));
        oneBranch.add(ImmutableList.<NamedExpression>of(new Alias(new IntegerLiteral(1), "c1")));
        Memo twoConstants = new Memo(null,
                new LogicalUnion(SetOperation.Qualifier.DISTINCT, twoBranches, ImmutableList.of(t1)));
        Memo oneConstant = new Memo(null,
                new LogicalUnion(SetOperation.Qualifier.DISTINCT, oneBranch, ImmutableList.of(t1)));
        GroupStructInfo twoInfo = GroupStructInfo.describeGroup(twoConstants.getRoot());
        GroupStructInfo oneInfo = GroupStructInfo.describeGroup(oneConstant.getRoot());
        Assertions.assertTrue(twoInfo.isValid(), twoInfo.getSkipReason());
        String twoShape = twoInfo.getShape(NO_LITERAL);
        Assertions.assertTrue(twoShape.contains("const:["), twoShape);
        // the values are rendered with their literal text in both modes: folding them would let two
        // different constant branches share a key
        Assertions.assertTrue(twoShape.contains("lit(1:INT)"), twoShape);
        Assertions.assertTrue(twoShape.contains("lit(2:INT)"), twoShape);
        Assertions.assertNotEquals(twoInfo.getFingerprint(NO_LITERAL), oneInfo.getFingerprint(NO_LITERAL),
                "two different constant branches share one key");
        // a constant is not folded away: the two modes name it as well
        Assertions.assertEquals(twoInfo.getShape(WITH_LITERAL), twoInfo.getShape(NO_LITERAL));
    }

    @Test
    void testALeafOfAChainIsRenderedOnce() {
        // a bounded ordering below a join is a leaf of the chain: its text is written once and its scans
        // are collected once
        LogicalOlapScan t1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan t2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        LogicalJoin<LogicalLimit<LogicalOlapScan>, LogicalOlapScan> join = new LogicalJoin<>(
                JoinType.INNER_JOIN, new LogicalLimit<>(5, 0, LimitPhase.ORIGIN, t1), t2, null);
        GroupStructInfo info = GroupStructInfo.describeGroup(new Memo(null, join).getRoot());
        Assertions.assertTrue(info.isValid(), info.getSkipReason());
        String shape = info.getShape(NO_LITERAL);
        Assertions.assertEquals(shape.indexOf("L{5,0}"), shape.lastIndexOf("L{5,0}"),
                "the leaf is rendered twice: " + shape);
        // the join reads two tables, so it collects two scans; the leaf is one of them
        Assertions.assertEquals(2, info.getScans().size(), shape);
        Assertions.assertTrue(shape.contains("L{5,0}S{null.null.t1#ti:0}"), shape);
    }

}

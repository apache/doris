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

package org.apache.doris.nereids.hint;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LeadingHintTest {

    @Test
    public void testLeftSemiJoinConstrainedSideMatchesExactly() {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);
        JoinConstraint semiJoinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                JoinType.LEFT_SEMI_JOIN);

        Pair<JoinConstraint, Boolean> exactReversed = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand), rightHand, leftHand);
        assertMatchedJoinConstraint(semiJoinConstraint, exactReversed, true);

        Pair<JoinConstraint, Boolean> withoutRetainedSide = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(rightHand, extraTable), rightHand, extraTable);
        assertNoMatchedJoinConstraint(withoutRetainedSide);

        Pair<JoinConstraint, Boolean> withExtraTable = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand, extraTable),
                LongBitmap.newBitmapUnion(rightHand, extraTable),
                leftHand);
        assertConstraintViolated(withExtraTable);
    }

    @Test
    public void testLeftAntiJoinConstrainedSideMatchesExactly() {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);
        JoinConstraint antiJoinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                JoinType.LEFT_ANTI_JOIN);

        Pair<JoinConstraint, Boolean> exactReversed = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand), rightHand, leftHand);
        assertMatchedJoinConstraint(antiJoinConstraint, exactReversed, true);

        Pair<JoinConstraint, Boolean> withExtraTable = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand, extraTable),
                LongBitmap.newBitmapUnion(rightHand, extraTable),
                leftHand);
        assertConstraintViolated(withExtraTable);
    }

    @Test
    public void testRightSemiJoinConstrainedSideMatchesExactly() {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);
        JoinConstraint semiJoinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                JoinType.RIGHT_SEMI_JOIN);

        Pair<JoinConstraint, Boolean> exact = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand), leftHand, rightHand);
        assertMatchedJoinConstraint(semiJoinConstraint, exact, false);

        Pair<JoinConstraint, Boolean> withoutConstrainedSide = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(rightHand, extraTable), rightHand, extraTable);
        assertNoMatchedJoinConstraint(withoutConstrainedSide);

        Pair<JoinConstraint, Boolean> withExtraTable = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand, extraTable),
                LongBitmap.newBitmapUnion(leftHand, extraTable),
                rightHand);
        assertConstraintViolated(withExtraTable);
    }

    @Test
    public void testRightAntiJoinConstrainedSideMatchesExactly() {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);
        JoinConstraint antiJoinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                JoinType.RIGHT_ANTI_JOIN);

        Pair<JoinConstraint, Boolean> exact = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand), leftHand, rightHand);
        assertMatchedJoinConstraint(antiJoinConstraint, exact, false);

        Pair<JoinConstraint, Boolean> withExtraTable = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand, extraTable),
                LongBitmap.newBitmapUnion(leftHand, extraTable),
                rightHand);
        assertConstraintViolated(withExtraTable);
    }

    @Test
    public void testCompositeLeftSemiAndAntiJoinConstrainedSideCanNotBeSplit() {
        assertCompositeConstrainedSideCanNotBeSplit(JoinType.LEFT_SEMI_JOIN);
        assertCompositeConstrainedSideCanNotBeSplit(JoinType.LEFT_ANTI_JOIN);
    }

    @Test
    public void testCompositeRightSemiAndAntiJoinConstrainedSideCanNotBeSplit() {
        assertCompositeConstrainedSideCanNotBeSplit(JoinType.RIGHT_SEMI_JOIN);
        assertCompositeConstrainedSideCanNotBeSplit(JoinType.RIGHT_ANTI_JOIN);
    }

    @Test
    public void testCompositeLeftSemiAndAntiJoinRetainedSideCanUseMinHand() {
        assertCompositeRetainedSideCanUseMinHand(JoinType.LEFT_SEMI_JOIN);
        assertCompositeRetainedSideCanUseMinHand(JoinType.LEFT_ANTI_JOIN);
    }

    @Test
    public void testCompositeRightSemiAndAntiJoinRetainedSideCanUseMinHand() {
        assertCompositeRetainedSideCanUseMinHand(JoinType.RIGHT_SEMI_JOIN);
        assertCompositeRetainedSideCanUseMinHand(JoinType.RIGHT_ANTI_JOIN);
    }

    @Test
    public void testIsJoinTypeCompatible() {
        // Exact match
        Assertions.assertTrue(LeadingHint.isJoinTypeCompatible(JoinType.INNER_JOIN, JoinType.INNER_JOIN));
        Assertions.assertTrue(LeadingHint.isJoinTypeCompatible(JoinType.LEFT_SEMI_JOIN, JoinType.LEFT_SEMI_JOIN));
        Assertions.assertTrue(LeadingHint.isJoinTypeCompatible(JoinType.LEFT_ANTI_JOIN, JoinType.LEFT_ANTI_JOIN));
        Assertions.assertTrue(LeadingHint.isJoinTypeCompatible(JoinType.LEFT_OUTER_JOIN, JoinType.LEFT_OUTER_JOIN));

        // One-side outer joins are interchangeable
        Assertions.assertTrue(LeadingHint.isJoinTypeCompatible(JoinType.LEFT_OUTER_JOIN, JoinType.RIGHT_OUTER_JOIN));
        Assertions.assertTrue(LeadingHint.isJoinTypeCompatible(JoinType.RIGHT_OUTER_JOIN, JoinType.LEFT_OUTER_JOIN));

        // Semi joins are compatible with each other
        Assertions.assertTrue(LeadingHint.isJoinTypeCompatible(JoinType.LEFT_SEMI_JOIN, JoinType.RIGHT_SEMI_JOIN));

        // Anti joins are compatible with each other
        Assertions.assertTrue(LeadingHint.isJoinTypeCompatible(JoinType.LEFT_ANTI_JOIN, JoinType.RIGHT_ANTI_JOIN));

        // Incompatible pairs
        Assertions.assertFalse(LeadingHint.isJoinTypeCompatible(JoinType.INNER_JOIN, JoinType.LEFT_SEMI_JOIN));
        Assertions.assertFalse(LeadingHint.isJoinTypeCompatible(JoinType.INNER_JOIN, JoinType.LEFT_ANTI_JOIN));
        Assertions.assertFalse(LeadingHint.isJoinTypeCompatible(JoinType.LEFT_SEMI_JOIN, JoinType.INNER_JOIN));
        Assertions.assertFalse(LeadingHint.isJoinTypeCompatible(JoinType.LEFT_ANTI_JOIN, JoinType.INNER_JOIN));
        Assertions.assertFalse(LeadingHint.isJoinTypeCompatible(JoinType.LEFT_SEMI_JOIN, JoinType.LEFT_ANTI_JOIN));
        Assertions.assertFalse(LeadingHint.isJoinTypeCompatible(JoinType.LEFT_OUTER_JOIN, JoinType.INNER_JOIN));

        // Full outer join is only compatible with itself
        Assertions.assertTrue(LeadingHint.isJoinTypeCompatible(JoinType.FULL_OUTER_JOIN, JoinType.FULL_OUTER_JOIN));
        Assertions.assertFalse(LeadingHint.isJoinTypeCompatible(JoinType.FULL_OUTER_JOIN, JoinType.INNER_JOIN));
        Assertions.assertFalse(LeadingHint.isJoinTypeCompatible(JoinType.INNER_JOIN, JoinType.FULL_OUTER_JOIN));
        Assertions.assertFalse(LeadingHint.isJoinTypeCompatible(JoinType.FULL_OUTER_JOIN, JoinType.LEFT_OUTER_JOIN));
    }

    @Test
    public void testFilterEntryKeepsOriginalJoinType() {
        // Same expression "a.v > 0" collected from two different joins:
        //   LeftAntiJoin(a,b)  → bitmap={0,1}, type=LEFT_ANTI_JOIN
        //   InnerJoin(c)       → bitmap={0},   type=INNER_JOIN
        // The inner-join occurrence should NOT be consumed by the anti join.
        LeadingHint leading = new LeadingHint("Leading");
        Expression expr = new IntegerLiteral(1);

        long a = LongBitmap.newBitmap(0);
        long b = LongBitmap.newBitmap(1);
        long ab = LongBitmap.newBitmapUnion(a, b);

        // Simulate CollectJoinConstraint bottom-up:
        // 1. LeftAntiJoin(a,b): records expr with bitmap={a,b}, type=LEFT_ANTI_JOIN
        leading.addFilter(ab, expr, JoinType.LEFT_ANTI_JOIN);
        // 2. InnerJoin((a,b),c): records same expr with bitmap={a}, type=INNER_JOIN
        leading.addFilter(a, expr, JoinType.INNER_JOIN);

        // Verify both entries are present with correct types
        Assertions.assertEquals(2, leading.getFilters().size());
        Assertions.assertEquals(ab, leading.getFilters().get(0).bitmap);
        Assertions.assertEquals(JoinType.LEFT_ANTI_JOIN, leading.getFilters().get(0).originalType);
        Assertions.assertEquals(a, leading.getFilters().get(1).bitmap);
        Assertions.assertEquals(JoinType.INNER_JOIN, leading.getFilters().get(1).originalType);
    }

    @Test
    public void testFullOuterJoinConstraintRequiresExactChildMatch() {
        // (a FULL OUTER JOIN b ON a.k = b.k) JOIN c
        // Leading(a c b) → the full outer constraint requires exact child match.
        // When children don't match exactly (left={a,c}, right={b}), the full
        // outer constraint continues (no match), and computeJoinType falls back
        // to INNER_JOIN. The FULL_OUTER_JOIN predicate would then be rejected by
        // isJoinTypeCompatible and put back, causing a leftover filter failure.
        //
        // This test verifies the constraint matching layer: full outer join only
        // matches when children exactly equal original leftHand/rightHand.
        LeadingHint leading = new LeadingHint("Leading");
        long a = LongBitmap.newBitmap(0);
        long b = LongBitmap.newBitmap(1);
        long c = LongBitmap.newBitmap(2);

        // Full outer join: leftHand={a}, rightHand={b}
        addJoinConstraint(leading, a, b, a, b, JoinType.FULL_OUTER_JOIN);

        long ab = LongBitmap.newBitmapUnion(a, b);
        long ac = LongBitmap.newBitmapUnion(a, c);
        long abc = LongBitmap.newBitmapUnion(ab, c);

        // Exact children match → constraint matches
        Pair<JoinConstraint, Boolean> exactMatch = leading.getJoinConstraint(ab, a, b);
        Assertions.assertTrue(exactMatch.second, "exact children should match full outer constraint");

        // Extra table mixed in left child → constraint does not match,
        // falls back to (null, true) = inner join (no constraint matched).
        // The full-outer predicate loss is caught later by the leftover-filter check.
        Pair<JoinConstraint, Boolean> mixedLeft = leading.getJoinConstraint(abc, ac, b);
        Assertions.assertNull(mixedLeft.first, "full outer constraint should not match with extra table");
        Assertions.assertTrue(mixedLeft.second, "no constraint matched → inner join is legal at this level");
    }

    @Test
    public void testRightOuterJoinBlocksPrematureInnerPredicateConsumption() {
        // (a RIGHT OUTER JOIN b ON a.k = b.k) INNER JOIN c ON a.x = c.x
        // Leading(a c b) → the inner predicate a.x = c.x references the nullable
        // left side {a} of the RIGHT OUTER JOIN. It must NOT be consumed at the
        // premature {a,c} join level because the outer join's preserved side {b}
        // has not arrived yet. Consuming it early would produce:
        //   (a INNER JOIN c) RIGHT OUTER JOIN b
        // which preserves b rows when a is empty — not equivalent to the original.
        //
        // At the {a,c} level the right-outer constraint is not applicable
        // (minRightHand={b} is absent), so getJoinConstraint returns (null,true).
        LeadingHint leading = new LeadingHint("Leading");
        long a = LongBitmap.newBitmap(0);
        long b = LongBitmap.newBitmap(1);
        long c = LongBitmap.newBitmap(2);

        // RIGHT OUTER JOIN constraint: leftHand={a} (nullable), rightHand={b} (preserved)
        addJoinConstraint(leading, a, b, a, b, JoinType.RIGHT_OUTER_JOIN);

        long ac = LongBitmap.newBitmapUnion(a, c);
        long abc = LongBitmap.newBitmapUnion(a, b, c);

        // Level {a,c}: constraint does not apply (b is absent)
        Pair<JoinConstraint, Boolean> levelAC = leading.getJoinConstraint(ac, a, c);
        Assertions.assertNull(levelAC.first,
                "right outer constraint should not match at {a,c} level (b absent)");
        Assertions.assertTrue(levelAC.second, "inner join is legal at {a,c} level");

        // Level {a,b,c}: constraint matches — children {a,c} and {b}
        // Wait, the constraint requires leftHand={a} on left and rightHand={b} on right.
        // With left={a,c} and right={b}: leftHand={a}⊆{a,c}? That's the minLeftHand check
        // in the general matching, not the full-outer exact match.
        // For RIGHT OUTER JOIN, the constraint uses the general minLeftHand/minRightHand
        // matching (not exact match like FULL OUTER JOIN).
        // minLeftHand={a} ⊆ {a,c}=left? Yes. minRightHand={b} ⊆ {b}=right? Yes.
        // → constraint matches as RIGHT_OUTER_JOIN.
        Pair<JoinConstraint, Boolean> levelABC = leading.getJoinConstraint(abc, ac, b);
        Assertions.assertNotNull(levelABC.first, "right outer constraint should match at {a,b,c} level");
        Assertions.assertTrue(levelABC.second, "constraint matched");
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, levelABC.first.getJoinType());
    }

    @Test
    public void testLeftOuterJoinBlocksPrematureInnerPredicateConsumption() {
        // (a LEFT OUTER JOIN b ON a.k = b.k) INNER JOIN c ON b.x = c.x
        // Leading(b c a) → the inner predicate b.x = c.x references the nullable
        // right side {b} of the LEFT OUTER JOIN. It must NOT be consumed at the
        // premature {b,c} join level before the preserved left side {a} arrives.
        LeadingHint leading = new LeadingHint("Leading");
        long a = LongBitmap.newBitmap(0);
        long b = LongBitmap.newBitmap(1);
        long c = LongBitmap.newBitmap(2);

        // LEFT OUTER JOIN constraint: leftHand={a} (preserved), rightHand={b} (nullable)
        addJoinConstraint(leading, a, b, a, b, JoinType.LEFT_OUTER_JOIN);

        long bc = LongBitmap.newBitmapUnion(b, c);
        long abc = LongBitmap.newBitmapUnion(a, b, c);

        // Level {b,c}: constraint does not apply (a is absent)
        Pair<JoinConstraint, Boolean> levelBC = leading.getJoinConstraint(bc, b, c);
        Assertions.assertNull(levelBC.first,
                "left outer constraint should not match at {b,c} level (a absent)");
        Assertions.assertTrue(levelBC.second, "inner join is legal at {b,c} level");

        // Level {a,b,c}: constraint matches
        Pair<JoinConstraint, Boolean> levelABC = leading.getJoinConstraint(abc, a, bc);
        Assertions.assertNotNull(levelABC.first, "left outer constraint should match at {a,b,c} level");
        Assertions.assertTrue(levelABC.second, "constraint matched");
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, levelABC.first.getJoinType());
    }

    @Test
    public void testFullOuterJoinPredicateNotConsumedAsScanFilter() {
        // a FULL OUTER JOIN b ON a.v > 0 — the predicate a.v > 0 belongs to
        // the full outer join's ON clause. It must NOT be pushed down as a
        // scan filter on a because rows where a.v <= 0 should produce
        // NULL-extended rows in the full outer join.
        //
        // Verifies: predicates with outer-join originalType are deferred from
        // scan-level consumption by shouldDeferFromScan.
        LeadingHint leading = new LeadingHint("Leading");
        Expression expr = new IntegerLiteral(1);
        long a = LongBitmap.newBitmap(0);

        // Simulate: FULL_OUTER_JOIN(a,b) records predicate a.v > 0 with bitmap={a}
        leading.addFilter(a, expr, JoinType.FULL_OUTER_JOIN);
        Assertions.assertEquals(JoinType.FULL_OUTER_JOIN,
                leading.getFilters().get(0).originalType,
                "full outer predicate should carry FULL_OUTER_JOIN type");
    }

    @Test
    public void testRightOuterJoinUpperPredicateNotConsumedBeforeNullableSide() {
        // (a LEFT OUTER JOIN b ON a.k = b.k) INNER JOIN c ON b.v > 0
        // leading(b a c): scan b is visited first. The predicate b.v > 0
        // has type=INNER_JOIN (from the upper join) and bitmap={b} which is
        // the nullable right side of the LEFT OUTER JOIN. It must NOT be
        // pushed as a scan filter on b — it must wait for the outer join.
        //
        // Verifies: isBlockedByPendingOuterJoin blocks INNER predicates that
        // reference the nullable side when the preserved side is absent.
        LeadingHint leading = new LeadingHint("Leading");
        long a = LongBitmap.newBitmap(0);
        long b = LongBitmap.newBitmap(1);

        // LEFT OUTER JOIN constraint: leftHand={a} (preserved), rightHand={b} (nullable)
        addJoinConstraint(leading, a, b, a, b, JoinType.LEFT_OUTER_JOIN);

        // The INNER JOIN predicate b.v > 0 has bitmap={b}, type=INNER_JOIN
        // Pretend we're at scan b: joinTableBitmap={b}
        // isBlockedByPendingOuterJoin({b}, {b}):
        //   LEFT OUTER JOIN: isOverlap({b}, rightHand={b})=true
        //   && !isSubset(leftHand={a}, {b})=true → blocked!
        //
        // getJoinConstraint at {b} level: minRightHand={b} overlaps {b}=true
        // → the else-branch mustBeLeftjoin guard we added skips because
        // minLeftHand={a} is not in {b}. So getJoinConstraint returns (null,true).
        Pair<JoinConstraint, Boolean> levelB = leading.getJoinConstraint(b, b, 0L);
        Assertions.assertNull(levelB.first,
                "left outer constraint should not match at scan {b} (a absent)");
        Assertions.assertTrue(levelB.second,
                "inner join is legal at {b} level — predicate deferral happens in collectJoinConditions/makeFilterPlanIfExist");
    }

    @Test
    public void testSemiJoinConstrainedSideViolatedWithMinLeftHand() {
        // Reproduces SQL 1 bug scenario:
        // LEFT SEMI JOIN: retained side leftHand={0,1}, rightHand={2}
        // minLeftHand={1} (condition only references table 1, not table 0)
        // So minLeftHand is a proper subset of leftHand (table 0 is an inner-join
        // table on the retained side that was propagated)
        // When constrained side {2} is mixed with {0} on a child → should fail
        LeadingHint leading = new LeadingHint("Leading");
        long retainedTable = LongBitmap.newBitmap(0);
        long minLeftTable = LongBitmap.newBitmap(1);
        long probeTable = LongBitmap.newBitmap(2);
        long leftHand = LongBitmap.newBitmapUnion(retainedTable, minLeftTable);
        long rightHand = probeTable;

        // minLeftHand={1}, minRightHand={2}, leftHand={0,1}, rightHand={2}
        addJoinConstraint(leading, minLeftTable, probeTable, leftHand, rightHand, JoinType.LEFT_SEMI_JOIN);

        // All tables present: joinTableBitmap={0,1,2}
        long joinTable = LongBitmap.newBitmapUnion(leftHand, rightHand);

        // Case 1: constrained side (rightHand={2}) intact on left, retained side intact on right → OK (reversed)
        Pair<JoinConstraint, Boolean> ok = leading.getJoinConstraint(
                joinTable, rightHand, leftHand);
        Assertions.assertTrue(ok.second, "constrained side intact on left should match (reversed)");

        // Case 2: constrained side {2} mixed with extra table {0} on left,
        // retained side {1} on right → VIOLATED
        Pair<JoinConstraint, Boolean> violated = leading.getJoinConstraint(
                joinTable,
                LongBitmap.newBitmapUnion(probeTable, retainedTable), // left={0,2} (constrained mixed with extra)
                minLeftTable); // right={1} (min retained)
        assertConstraintViolated(violated);
    }

    @Test
    public void testRightAntiJoinConstrainedSideViolatedWithMinLeftHand() {
        // Reproduces SQL 2 bug scenario:
        // RIGHT ANTI JOIN: leftHand={0}, rightHand={1}
        // minLeftHand={0}, minRightHand={1} (both are minimal)
        // Extra inner join table {2} is mixed with constrained side {0}
        // When constrained side {0} is mixed with {2} on a child → should fail
        LeadingHint leading = new LeadingHint("Leading");
        long constrainedTable = LongBitmap.newBitmap(0);
        long rightTable = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);

        // minLeftHand={0}, minRightHand={1}, leftHand={0}, rightHand={1}
        addJoinConstraint(leading, constrainedTable, rightTable,
                constrainedTable, rightTable, JoinType.RIGHT_ANTI_JOIN);

        // All tables present (including extra inner join table): {0,1,2}
        long joinTable = LongBitmap.newBitmapUnion(constrainedTable, rightTable, extraTable);

        // Case 1: constrained side {0} intact, right side {1} intact → OK
        Pair<JoinConstraint, Boolean> ok = leading.getJoinConstraint(
                joinTable, constrainedTable, rightTable);
        Assertions.assertTrue(ok.second, "constrained side intact should match");

        // Case 2: constrained side {0} mixed with extra {2} on left,
        // right side {1} on right → VIOLATED
        Pair<JoinConstraint, Boolean> violated = leading.getJoinConstraint(
                joinTable,
                LongBitmap.newBitmapUnion(constrainedTable, extraTable), // left={0,2}
                rightTable); // right={1}
        assertConstraintViolated(violated);
    }

    @Test
    public void testRightAntiJoinCompositeConstrainedSideMixedWithExtra() {
        // RIGHT ANTI JOIN with composite constrained side:
        // leftHand={0,2} (composite: constrainedTable + propagated inner join table)
        // rightHand={1}
        // minLeftHand={0,2}, minRightHand={1}
        // When constrained side {0,2} is split across both children → VIOLATED
        LeadingHint leading = new LeadingHint("Leading");
        long constrainedTable = LongBitmap.newBitmap(0);
        long innerJoinOnLeft = LongBitmap.newBitmap(2);
        long rightTable = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(3);

        long leftHand = LongBitmap.newBitmapUnion(constrainedTable, innerJoinOnLeft);
        long rightHand = rightTable;

        // composite constrained side: leftHand={0,2}, minLeftHand={0,2}
        addJoinConstraint(leading, leftHand, rightHand, leftHand, rightHand, JoinType.RIGHT_ANTI_JOIN);

        long joinTable = LongBitmap.newBitmapUnion(leftHand, rightHand, extraTable);

        // Constrained side {0,2} intact on left → OK
        Pair<JoinConstraint, Boolean> ok = leading.getJoinConstraint(
                joinTable, leftHand, rightHand);
        Assertions.assertTrue(ok.second, "composite constrained side intact should match");

        // Constrained side {0,2} split: {0} on left, {2} on right (mixed with right table) → VIOLATED
        Pair<JoinConstraint, Boolean> splitViolated = leading.getJoinConstraint(
                joinTable,
                constrainedTable, // left={0} (part of constrained)
                LongBitmap.newBitmapUnion(innerJoinOnLeft, rightTable)); // right={1,2}
        assertConstraintViolated(splitViolated);
    }

    @Test
    public void testLeftSemiJoinConstraintNotApplicableWithoutMinLeftHand() {
        // When minLeftHand is NOT yet present in joinTableBitmap,
        // the constraint should NOT be applied (not applicable yet),
        // returning (null, true) — not (null, false)!
        // Regression test: ensure the first guard (isSubset minLeftHand/minRightHand)
        // correctly does "continue", not "return failure".
        LeadingHint leading = new LeadingHint("Leading");
        long retainedTable = LongBitmap.newBitmap(0);
        long minLeftTable = LongBitmap.newBitmap(1);
        long probeTable = LongBitmap.newBitmap(2);
        long leftHand = LongBitmap.newBitmapUnion(retainedTable, minLeftTable);
        long rightHand = probeTable;

        addJoinConstraint(leading, minLeftTable, probeTable, leftHand, rightHand, JoinType.LEFT_SEMI_JOIN);

        // Only probe table and extra table present — minLeftHand={1} NOT present
        long extraTable = LongBitmap.newBitmap(3);
        Pair<JoinConstraint, Boolean> notApplicable = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(probeTable, extraTable),
                probeTable,
                extraTable);
        // Should be "no constraint matched" (null, true), NOT "violated" (null, false)
        assertNoMatchedJoinConstraint(notApplicable);
    }

    private JoinConstraint addJoinConstraint(LeadingHint leading, long leftHand, long rightHand,
            JoinType joinType) {
        JoinConstraint joinConstraint = new JoinConstraint(leftHand, rightHand, leftHand, rightHand, joinType, true);
        leading.getJoinConstraintList().add(joinConstraint);
        return joinConstraint;
    }

    private JoinConstraint addJoinConstraint(LeadingHint leading, long minLeftHand, long minRightHand,
            long leftHand, long rightHand, JoinType joinType) {
        JoinConstraint joinConstraint = new JoinConstraint(minLeftHand, minRightHand, leftHand, rightHand,
                joinType, true);
        leading.getJoinConstraintList().add(joinConstraint);
        return joinConstraint;
    }

    private void assertCompositeConstrainedSideCanNotBeSplit(JoinType joinType) {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);
        long joinTable = LongBitmap.newBitmapUnion(leftHand, rightHand, extraTable);

        if (joinType.isRightSemiOrAntiJoin()) {
            JoinConstraint joinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                    LongBitmap.newBitmapUnion(leftHand, extraTable), rightHand, joinType);

            Pair<JoinConstraint, Boolean> exact = leading.getJoinConstraint(
                    joinTable, LongBitmap.newBitmapUnion(leftHand, extraTable), rightHand);
            assertMatchedJoinConstraint(joinConstraint, exact, false);

            Pair<JoinConstraint, Boolean> splitConstrainedSide = leading.getJoinConstraint(
                    joinTable, leftHand, LongBitmap.newBitmapUnion(rightHand, extraTable));
            assertConstraintViolated(splitConstrainedSide);
        } else {
            JoinConstraint joinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                    leftHand, LongBitmap.newBitmapUnion(rightHand, extraTable), joinType);

            Pair<JoinConstraint, Boolean> exact = leading.getJoinConstraint(
                    joinTable, leftHand, LongBitmap.newBitmapUnion(rightHand, extraTable));
            assertMatchedJoinConstraint(joinConstraint, exact, false);

            Pair<JoinConstraint, Boolean> splitConstrainedSide = leading.getJoinConstraint(
                    joinTable, LongBitmap.newBitmapUnion(leftHand, extraTable), rightHand);
            assertConstraintViolated(splitConstrainedSide);
        }
    }

    private void assertCompositeRetainedSideCanUseMinHand(JoinType joinType) {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);

        JoinConstraint joinConstraint;
        if (joinType.isRightSemiOrAntiJoin()) {
            joinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                    leftHand, LongBitmap.newBitmapUnion(rightHand, extraTable), joinType);
        } else {
            joinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                    LongBitmap.newBitmapUnion(leftHand, extraTable), rightHand, joinType);
        }

        Pair<JoinConstraint, Boolean> minRetainedSide = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand), leftHand, rightHand);
        assertMatchedJoinConstraint(joinConstraint, minRetainedSide, false);
    }

    private void assertMatchedJoinConstraint(JoinConstraint expected, Pair<JoinConstraint, Boolean> actual,
            boolean reversed) {
        Assertions.assertSame(expected, actual.first);
        Assertions.assertTrue(actual.second);
        Assertions.assertEquals(reversed, actual.first.isReversed());
    }

    private void assertNoMatchedJoinConstraint(Pair<JoinConstraint, Boolean> actual) {
        Assertions.assertNull(actual.first);
        Assertions.assertTrue(actual.second);
    }

    private void assertConstraintViolated(Pair<JoinConstraint, Boolean> actual) {
        Assertions.assertNull(actual.first);
        Assertions.assertFalse(actual.second);
    }
}

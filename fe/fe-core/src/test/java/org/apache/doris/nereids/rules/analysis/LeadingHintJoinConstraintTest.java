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

import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The leading hint rebuilds the join tree from the join constraints which are collected by
 * {@link CollectJoinConstraint} in the analysis phase. The tests pin the semantic guards of the rebuild:
 * the one-sided ON conjunct of a full outer join must stay at the join instead of being pushed into
 * one of its inputs, and the matched side of a semi join must not be joined with unrelated tables
 * before the preserved side arrives.
 */
public class LeadingHintJoinConstraintTest extends TestWithFeService {
    private static final String DB_NAME = "leading_hint_join_constraint";
    private static final String TABLE_1 = "leading_hint_t1";
    private static final String TABLE_2 = "leading_hint_t2";
    private static final String TABLE_3 = "leading_hint_t3";

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(DB_NAME);
        useDatabase(DB_NAME);
        createTable("create table " + TABLE_1 + " (k int, v int) distributed by hash(k) buckets 1"
                + " properties(\"replication_num\" = \"1\");");
        createTable("create table " + TABLE_2 + " (k int, v int) distributed by hash(k) buckets 1"
                + " properties(\"replication_num\" = \"1\");");
        createTable("create table " + TABLE_3 + " (k int, v int) distributed by hash(k) buckets 1"
                + " properties(\"replication_num\" = \"1\");");
    }

    @Test
    public void testOneSidedFullOuterJoinConjunctStaysAtTheJoin() throws Exception {
        // A two-sided conjunct such as `k = k` is unaffected. The one-sided conjunct `k > 100` must not be
        // pushed into TABLE_1: FULL OUTER JOIN preserves both sides, so the rows which fail the conjunct
        // still have to be output with the other side null-extended.
        PlanChecker planChecker = PlanChecker.from(connectContext)
                .analyze("SELECT /*+ leading(" + TABLE_1 + " " + TABLE_2 + ") */ COUNT(*) FROM "
                        + TABLE_1 + " FULL OUTER JOIN " + TABLE_2
                        + " ON " + TABLE_1 + ".k > 100");
        Assertions.assertTrue(planChecker.getCascadesContext().getHintMap().get("Leading").isSuccess());

        LogicalJoin<?, ?> join = findJoin(planChecker.getPlan(), JoinType.FULL_OUTER_JOIN);
        Assertions.assertNotNull(join, () -> "full outer join is missing in plan:\n"
                + planChecker.getPlan().treeString());
        Assertions.assertTrue(join.getOtherJoinConjuncts().toString().contains("> 100"),
                () -> "the one-sided conjunct of the full outer join is not kept at the join: " + join);
    }

    @Test
    public void testSemiJoinDoesNotAbsorbTablesBeforeThePreservedSideArrives() throws Exception {
        // With `leading(TABLE_2 TABLE_3 TABLE_1)` the first join is {TABLE_2, TABLE_3}, but TABLE_2 is the
        // matched side of the semi join whose preserved side TABLE_1 is not present yet. Rebuilding the
        // semi join at this level would silently drop its condition, so the hint must be ignored.
        String query = "SELECT /*+ leading(" + TABLE_2 + " " + TABLE_3 + " " + TABLE_1 + ") */ COUNT(*) FROM ("
                + TABLE_1 + " LEFT SEMI JOIN " + TABLE_2 + " ON " + TABLE_1 + ".k = " + TABLE_2 + ".k)"
                + " CROSS JOIN " + TABLE_3;
        PlanChecker planChecker = PlanChecker.from(connectContext).analyze(query);
        Assertions.assertFalse(planChecker.getCascadesContext().getHintMap().get("Leading").isSuccess(),
                () -> "the illegal join order must not be applied:\n" + planChecker.getPlan().treeString());

        LogicalJoin<?, ?> semiJoin = findJoin(planChecker.getPlan(), JoinType.LEFT_SEMI_JOIN);
        Assertions.assertNotNull(semiJoin, () -> "the original semi join is lost in plan:\n"
                + planChecker.getPlan().treeString());
        Assertions.assertFalse(semiJoin.getHashJoinConjuncts().isEmpty()
                        && semiJoin.getOtherJoinConjuncts().isEmpty(),
                () -> "the condition of the semi join is lost: " + semiJoin);
    }

    @Test
    public void testSemiJoinIsKeptWhenThePreservedSideArrivesFirst() throws Exception {
        // The symmetric legal order still applies the hint: {TABLE_1, TABLE_2} is joined first, so the
        // semi join can be built with its preserved side and then joined with TABLE_3.
        String query = "SELECT /*+ leading(" + TABLE_1 + " " + TABLE_2 + " " + TABLE_3 + ") */ COUNT(*) FROM ("
                + TABLE_1 + " LEFT SEMI JOIN " + TABLE_2 + " ON " + TABLE_1 + ".k = " + TABLE_2 + ".k)"
                + " CROSS JOIN " + TABLE_3;
        PlanChecker planChecker = PlanChecker.from(connectContext).analyze(query);
        Assertions.assertTrue(planChecker.getCascadesContext().getHintMap().get("Leading").isSuccess(),
                () -> "the legal join order must be applied:\n" + planChecker.getPlan().treeString());

        LogicalJoin<?, ?> semiJoin = findJoin(planChecker.getPlan(), JoinType.LEFT_SEMI_JOIN);
        Assertions.assertNotNull(semiJoin, () -> "semi join is missing in plan:\n"
                + planChecker.getPlan().treeString());
        Assertions.assertFalse(semiJoin.getHashJoinConjuncts().isEmpty()
                        && semiJoin.getOtherJoinConjuncts().isEmpty(),
                () -> "the condition of the semi join is lost: " + semiJoin);
    }

    private LogicalJoin<?, ?> findJoin(Plan plan, JoinType joinType) {
        if (plan instanceof LogicalJoin && ((LogicalJoin<?, ?>) plan).getJoinType() == joinType) {
            return (LogicalJoin<?, ?>) plan;
        }
        for (Plan child : plan.children()) {
            LogicalJoin<?, ?> join = findJoin(child, joinType);
            if (join != null) {
                return join;
            }
        }
        return null;
    }
}

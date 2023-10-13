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

import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class PushFilterInsideJoinTest implements MemoPatternMatchSupported {

    private static LogicalOlapScan scan1;
    private static LogicalOlapScan scan2;
    private static LogicalOlapScan scoreScan;
    private static LogicalOlapScan studentScan;
    private static LogicalOlapScan courseScan;

    @BeforeAll
    public static void beforeAll() {
        scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        scoreScan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                PlanConstructor.score,
                ImmutableList.of(""));
        studentScan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                PlanConstructor.student,
                ImmutableList.of(""));
        courseScan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                PlanConstructor.course,
                ImmutableList.of(""));
    }

    @Test
    void testPushInsideCrossJoin() {
        Expression predicates = new GreaterThan(scan1.getOutput().get(1), scan2.getOutput().get(1));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .joinEmptyOn(scan2, JoinType.CROSS_JOIN)
                .filter(predicates)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushFilterInsideJoin())
                .printlnTree()
                .matchesFromRoot(
                        logicalJoin().when(join -> join.getOtherJoinConjuncts().get(0).equals(predicates))
                );
    }

    @Test
    public void testPushInsideInnerJoin() {
        Expression predicates = new Or(new And(
                // score.sid = student.id
                new EqualTo(scoreScan.getOutput().get(0), studentScan.getOutput().get(0)),
                // score.cid = course.cid
                new EqualTo(scoreScan.getOutput().get(1), courseScan.getOutput().get(0))),
                // grade > 2
                new GreaterThan(scoreScan.getOutput().get(2), new DoubleLiteral(2)));

        LogicalPlan plan = new LogicalPlanBuilder(scoreScan)
                .joinEmptyOn(studentScan, JoinType.CROSS_JOIN)
                .join(courseScan, JoinType.INNER_JOIN,
                        // score.cid = course.cid
                        ImmutableList.of(new EqualTo(scoreScan.getOutput().get(1), courseScan.getOutput().get(0))),
                        // grade < 10
                        ImmutableList.of(new LessThan(scoreScan.getOutput().get(2), new DoubleLiteral(10))))
                .filter(predicates)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushFilterInsideJoin())
                .printlnTree()
                .matchesFromRoot(
                        logicalJoin().when(join -> join.getOtherJoinConjuncts().get(0).equals(predicates)));
    }

    @Test
    public void testShouldNotPushInsideJoin() {
        for (JoinType joinType : JoinType.values()) {
            if (JoinType.INNER_JOIN == joinType || JoinType.CROSS_JOIN == joinType) {
                continue;
            }
            shouldNotPushInsideJoin(joinType);
        }
    }

    private void shouldNotPushInsideJoin(JoinType joinType) {
        // score.sid = student.id
        Expression eq = new EqualTo(scoreScan.getOutput().get(0), studentScan.getOutput().get(0));
        // default use left side column grade > 2
        Expression predicate = new GreaterThan(scoreScan.getOutput().get(2), new DoubleLiteral(2));
        if (JoinType.RIGHT_ANTI_JOIN == joinType || JoinType.RIGHT_SEMI_JOIN == joinType) {
            // use right side column age < 10
            predicate = new LessThan(studentScan.getOutput().get(3), new DoubleLiteral(10));
        }

        LogicalPlan plan = new LogicalPlanBuilder(scoreScan)
                .join(studentScan, joinType, ImmutableList.of(eq), ImmutableList.of())
                .filter(predicate)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushFilterInsideJoin())
                .printlnTree()
                .matches(
                        logicalJoin().when(join -> join.getHashJoinConjuncts().get(0).equals(eq))
                                .when(join -> join.getOtherJoinConjuncts().isEmpty()));
    }
}

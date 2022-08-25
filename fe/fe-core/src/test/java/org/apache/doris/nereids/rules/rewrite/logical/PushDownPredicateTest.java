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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionNormalization;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.nereids.util.PlanRewriter;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Optional;

/**
 * plan rewrite ut.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PushDownPredicateTest {

    private Plan rStudent;
    private Plan rScore;
    private Plan rCourse;

    /**
     * ut before.
     */
    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(PlanConstructor.student, ImmutableList.of("student"));

        rScore = new LogicalOlapScan(PlanConstructor.score, ImmutableList.of("score"));

        rCourse = new LogicalOlapScan(PlanConstructor.course, ImmutableList.of("course"));
    }

    @Test
    public void pushDownPredicateIntoScanTest1() {
        // select id,name,grade from student join score on student.id = score.sid and student.id > 1
        // and score.cid > 2 where student.age > 18 and score.grade > 60
        Expression onCondition1 = new EqualTo(rStudent.getOutput().get(0), rScore.getOutput().get(0));
        Expression onCondition2 = new GreaterThan(rStudent.getOutput().get(0), Literal.of(1));
        Expression onCondition3 = new GreaterThan(rScore.getOutput().get(0), Literal.of(2));
        Expression onCondition = ExpressionUtils.and(onCondition1, onCondition2, onCondition3);

        Expression whereCondition1 = new GreaterThan(rStudent.getOutput().get(1), Literal.of(18));
        Expression whereCondition2 = new GreaterThan(rScore.getOutput().get(2), Literal.of(60));
        Expression whereCondition = ExpressionUtils.and(whereCondition1, whereCondition2);


        Plan join = new LogicalJoin(JoinType.INNER_JOIN, new ArrayList<>(), Optional.of(onCondition), rStudent, rScore);
        Plan filter = new LogicalFilter(whereCondition, join);

        Plan root = new LogicalProject(
                Lists.newArrayList(rStudent.getOutput().get(1), rCourse.getOutput().get(1), rScore.getOutput().get(2)),
                filter
        );

        Memo memo = rewrite(root);

        Group rootGroup = memo.getRoot();

        Plan op1 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().getPlan();
        Plan op2 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(0).getLogicalExpression()
                .getPlan();
        Plan op3 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(1).getLogicalExpression()
                .getPlan();

        Assertions.assertTrue(op1 instanceof LogicalJoin);
        Assertions.assertTrue(op2 instanceof LogicalFilter);
        Assertions.assertTrue(op3 instanceof LogicalFilter);
        LogicalJoin join1 = (LogicalJoin) op1;
        LogicalFilter filter1 = (LogicalFilter) op2;
        LogicalFilter filter2 = (LogicalFilter) op3;

        Assertions.assertEquals(onCondition1, join1.getOtherJoinCondition().get());
        Assertions.assertEquals(ExpressionUtils.and(onCondition2, whereCondition1), filter1.getPredicates());
        Assertions.assertEquals(ExpressionUtils.and(onCondition3,
                        new GreaterThan(rScore.getOutput().get(2), new Cast(Literal.of(60), DoubleType.INSTANCE))),
                filter2.getPredicates());
    }

    @Test
    public void pushDownPredicateIntoScanTest3() {
        //select id,name,grade from student left join score on student.id + 1 = score.sid - 2
        //where student.age > 18 and score.grade > 60
        Expression whereCondition1 = new EqualTo(new Add(rStudent.getOutput().get(0), Literal.of(1)),
                new Subtract(rScore.getOutput().get(0), Literal.of(2)));
        Expression whereCondition2 = new GreaterThan(rStudent.getOutput().get(1), Literal.of(18));
        Expression whereCondition3 = new GreaterThan(rScore.getOutput().get(2), Literal.of(60));
        Expression whereCondition = ExpressionUtils.and(whereCondition1, whereCondition2, whereCondition3);

        Plan join = new LogicalJoin(JoinType.INNER_JOIN, new ArrayList<>(), Optional.empty(), rStudent, rScore);
        Plan filter = new LogicalFilter(whereCondition, join);

        Plan root = new LogicalProject(
                Lists.newArrayList(rStudent.getOutput().get(1), rCourse.getOutput().get(1), rScore.getOutput().get(2)),
                filter
        );

        Memo memo = rewrite(root);
        Group rootGroup = memo.getRoot();

        Plan op1 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().getPlan();
        Plan op2 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(0).getLogicalExpression()
                .getPlan();
        Plan op3 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(1).getLogicalExpression()
                .getPlan();

        Assertions.assertTrue(op1 instanceof LogicalJoin);
        Assertions.assertTrue(op2 instanceof LogicalFilter);
        Assertions.assertTrue(op3 instanceof LogicalFilter);
        LogicalJoin join1 = (LogicalJoin) op1;
        LogicalFilter filter1 = (LogicalFilter) op2;
        LogicalFilter filter2 = (LogicalFilter) op3;
        Assertions.assertEquals(whereCondition1, join1.getOtherJoinCondition().get());
        Assertions.assertEquals(whereCondition2, filter1.getPredicates());
        Assertions.assertEquals(
                new GreaterThan(rScore.getOutput().get(2), new Cast(Literal.of(60), DoubleType.INSTANCE)),
                filter2.getPredicates());
    }

    @Test
    public void pushDownPredicateIntoScanTest4() {
        /*
        select
         student.name,
         course.name,
         score.grade
        from student,score,course
        where on student.id = score.sid and student.age between 18 and 20 and score.grade > 60 and student.id = score.sid
         */

        // student.id = score.sid
        Expression whereCondition1 = new EqualTo(rStudent.getOutput().get(0), rScore.getOutput().get(0));
        // score.cid = course.cid
        Expression whereCondition2 = new EqualTo(rScore.getOutput().get(1), rCourse.getOutput().get(0));
        // student.age between 18 and 20
        Expression whereCondition3 = new Between(rStudent.getOutput().get(2), Literal.of(18), Literal.of(20));
        // student.age >= 18 and student.age <= 20
        Expression whereCondition3result = new And(
                new GreaterThanEqual(rStudent.getOutput().get(2), new Cast(Literal.of(18), StringType.INSTANCE)),
                new LessThanEqual(rStudent.getOutput().get(2), new Cast(Literal.of(20), StringType.INSTANCE)));

        // score.grade > 60
        Expression whereCondition4 = new GreaterThan(rScore.getOutput().get(2), Literal.of(60));

        Expression whereCondition = ExpressionUtils.and(whereCondition1, whereCondition2, whereCondition3,
                whereCondition4);

        Plan join = new LogicalJoin(JoinType.INNER_JOIN, ImmutableList.of(), Optional.empty(), rStudent, rScore);
        Plan join1 = new LogicalJoin(JoinType.INNER_JOIN, ImmutableList.of(), Optional.empty(), join, rCourse);
        Plan filter = new LogicalFilter(whereCondition, join1);

        Plan root = new LogicalProject(
                Lists.newArrayList(rStudent.getOutput().get(1), rCourse.getOutput().get(1), rScore.getOutput().get(2)),
                filter
        );

        Memo memo = rewrite(root);
        Group rootGroup = memo.getRoot();
        Plan join2 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().getPlan();
        Plan join3 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(0).getLogicalExpression()
                .getPlan();
        Plan op1 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(0).getLogicalExpression()
                .child(0).getLogicalExpression().getPlan();
        Plan op2 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(0).getLogicalExpression()
                .child(1).getLogicalExpression().getPlan();

        Assertions.assertTrue(join2 instanceof LogicalJoin);
        Assertions.assertTrue(join3 instanceof LogicalJoin);
        Assertions.assertTrue(op1 instanceof LogicalFilter);
        Assertions.assertTrue(op2 instanceof LogicalFilter);

        Assertions.assertEquals(whereCondition2, ((LogicalJoin) join2).getOtherJoinCondition().get());
        Assertions.assertEquals(whereCondition1, ((LogicalJoin) join3).getOtherJoinCondition().get());
        Assertions.assertEquals(whereCondition3result.toSql(), ((LogicalFilter) op1).getPredicates().toSql());
        Assertions.assertEquals(
                new GreaterThan(rScore.getOutput().get(2), new Cast(Literal.of(60), DoubleType.INSTANCE)),
                ((LogicalFilter) op2).getPredicates());
    }

    private Memo rewrite(Plan plan) {
        Plan normalizedPlan = PlanRewriter.topDownRewrite(plan, new ConnectContext(), new ExpressionNormalization());
        return PlanRewriter.topDownRewriteMemo(normalizedPlan, new ConnectContext(), new PushPredicateThroughJoin());
    }
}

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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.rewrite.RewriteTopDownJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.operators.plans.logical.LogicalFilter;
import org.apache.doris.nereids.operators.plans.logical.LogicalJoin;
import org.apache.doris.nereids.operators.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.Plans;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

/**
 * plan rewrite ut.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PushDownPredicateTest implements Plans {

    private Table student;
    private Table score;
    private Table course;

    private Plan rStudent;
    private Plan rScore;
    private Plan rCourse;

    /**
     * ut before.
     */
    @BeforeAll
    public final void beforeAll() {
        student = new Table(0L, "student", Table.TableType.OLAP,
                ImmutableList.<Column>of(new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "", ""),
                        new Column("age", Type.INT, true, AggregateType.NONE, "", "")));

        score = new Table(0L, "score", Table.TableType.OLAP,
                ImmutableList.<Column>of(new Column("sid", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("cid", Type.INT, true, AggregateType.NONE, "", ""),
                        new Column("grade", Type.DOUBLE, true, AggregateType.NONE, "", "")));

        course = new Table(0L, "course", Table.TableType.OLAP,
                ImmutableList.<Column>of(new Column("cid", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "", ""),
                        new Column("teacher", Type.STRING, true, AggregateType.NONE, "", "")));

        rStudent = plan(new LogicalOlapScan(student, ImmutableList.of("student")));

        rScore = plan(new LogicalOlapScan(score, ImmutableList.of("score")));

        rCourse = plan(new LogicalOlapScan(course, ImmutableList.of("course")));
    }

    @Test
    public void pushDownPredicateIntoScanTest1() {
        // select id,name,grade from student join score on student.id = score.sid and student.id > 1
        // and score.cid > 2 where student.age > 18 and score.grade > 60
        Expression onCondition1 = new EqualTo<>(rStudent.getOutput().get(0), rScore.getOutput().get(0));
        Expression onCondition2 = new GreaterThan<>(rStudent.getOutput().get(0), Literal.of(1));
        Expression onCondition3 = new GreaterThan<>(rScore.getOutput().get(0), Literal.of(2));
        Expression onCondition = ExpressionUtils.add(onCondition1, onCondition2, onCondition3);

        Expression whereCondition1 = new GreaterThan<>(rStudent.getOutput().get(1), Literal.of(18));
        Expression whereCondition2 = new GreaterThan<>(rScore.getOutput().get(2), Literal.of(60));
        Expression whereCondition = ExpressionUtils.add(whereCondition1, whereCondition2);


        Plan join = plan(new LogicalJoin(JoinType.INNER_JOIN, Optional.of(onCondition)), rStudent, rScore);
        Plan filter = plan(new LogicalFilter(whereCondition), join);

        Plan root = plan(new LogicalProject(
                Lists.newArrayList(rStudent.getOutput().get(1), rCourse.getOutput().get(1), rScore.getOutput().get(2))),
                filter);

        Memo memo = new Memo();
        memo.initialize(root);
        System.out.println(memo.copyOut().treeString());

        PlannerContext plannerContext = new PlannerContext(memo, null);
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), Double.MAX_VALUE);
        plannerContext.setCurrentJobContext(jobContext);

        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(),
                ImmutableList.of(new PushPredicateThroughJoin().build()), jobContext);
        plannerContext.pushJob(rewriteTopDownJob);
        plannerContext.getJobScheduler().executeJobPool(plannerContext);

        Group rootGroup = memo.getRoot();
        System.out.println(memo.copyOut().treeString());
        System.out.println(11);

        Operator op1 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().getOperator();
        Operator op2 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(0).getLogicalExpression().getOperator();
        Operator op3 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(1).getLogicalExpression().getOperator();

        Assertions.assertTrue(op1 instanceof LogicalJoin);
        Assertions.assertTrue(op2 instanceof LogicalFilter);
        Assertions.assertTrue(op3 instanceof LogicalFilter);
        LogicalJoin join1 = (LogicalJoin) op1;
        LogicalFilter filter1 = (LogicalFilter) op2;
        LogicalFilter filter2 = (LogicalFilter) op3;

        Assertions.assertEquals(join1.getCondition().get(), onCondition1);
        Assertions.assertEquals(filter1.getPredicates(), ExpressionUtils.add(onCondition2, whereCondition1));
        Assertions.assertEquals(filter2.getPredicates(), ExpressionUtils.add(onCondition3, whereCondition2));
    }

    @Test
    public void pushDownPredicateIntoScanTest3() {
        //select id,name,grade from student left join score on student.id + 1 = score.sid - 2
        //where student.age > 18 and score.grade > 60
        Expression whereCondition1 = new EqualTo<>(new Add<>(rStudent.getOutput().get(0), Literal.of(1)),
                new Subtract<>(rScore.getOutput().get(0), Literal.of(2)));
        Expression whereCondition2 = new GreaterThan<>(rStudent.getOutput().get(1), Literal.of(18));
        Expression whereCondition3 = new GreaterThan<>(rScore.getOutput().get(2), Literal.of(60));
        Expression whereCondition = ExpressionUtils.add(whereCondition1, whereCondition2, whereCondition3);

        Plan join = plan(new LogicalJoin(JoinType.INNER_JOIN, Optional.empty()), rStudent, rScore);
        Plan filter = plan(new LogicalFilter(whereCondition), join);

        Plan root = plan(new LogicalProject(
                Lists.newArrayList(rStudent.getOutput().get(1), rCourse.getOutput().get(1), rScore.getOutput().get(2))),
                filter);

        Memo memo = new Memo();
        memo.initialize(root);
        System.out.println(memo.copyOut().treeString());

        PlannerContext plannerContext = new PlannerContext(memo, null);
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), Double.MAX_VALUE);
        plannerContext.setCurrentJobContext(jobContext);

        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(),
                ImmutableList.of(new PushPredicateThroughJoin().build()), jobContext);
        plannerContext.pushJob(rewriteTopDownJob);
        plannerContext.getJobScheduler().executeJobPool(plannerContext);

        Group rootGroup = memo.getRoot();
        System.out.println(memo.copyOut().treeString());

        Operator op1 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().getOperator();
        Operator op2 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(0).getLogicalExpression().getOperator();
        Operator op3 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(1).getLogicalExpression().getOperator();

        Assertions.assertTrue(op1 instanceof LogicalJoin);
        Assertions.assertTrue(op2 instanceof LogicalFilter);
        Assertions.assertTrue(op3 instanceof LogicalFilter);
        LogicalJoin join1 = (LogicalJoin) op1;
        LogicalFilter filter1 = (LogicalFilter) op2;
        LogicalFilter filter2 = (LogicalFilter) op3;
        Assertions.assertEquals(join1.getCondition().get(), whereCondition1);
        Assertions.assertEquals(filter1.getPredicates(), whereCondition2);
        Assertions.assertEquals(filter2.getPredicates(), whereCondition3);
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
        Expression whereCondition1 = new EqualTo<>(rStudent.getOutput().get(0), rScore.getOutput().get(0));
        // score.cid = course.cid
        Expression whereCondition2 = new EqualTo<>(rScore.getOutput().get(1), rCourse.getOutput().get(0));
        // student.age between 18 and 20
        Expression whereCondition3 = new Between<>(rStudent.getOutput().get(2), Literal.of(18), Literal.of(20));
        // student.age >= 18 and student.age <= 20
        Expression whereCondition3result = new And<>(
                new GreaterThanEqual<>(rStudent.getOutput().get(2), Literal.of(18)),
                new LessThanEqual<>(rStudent.getOutput().get(2), Literal.of(20)));

        // score.grade > 60
        Expression whereCondition4 = new GreaterThan<>(rScore.getOutput().get(2), Literal.of(60));

        Expression whereCondition = ExpressionUtils.add(whereCondition1, whereCondition2, whereCondition3, whereCondition4);

        Plan join = plan(new LogicalJoin(JoinType.INNER_JOIN, Optional.empty()), rStudent, rScore);
        Plan join1 = plan(new LogicalJoin(JoinType.INNER_JOIN, Optional.empty()), join, rCourse);
        Plan filter = plan(new LogicalFilter(whereCondition), join1);

        Plan root = plan(new LogicalProject(
                Lists.newArrayList(rStudent.getOutput().get(1), rCourse.getOutput().get(1), rScore.getOutput().get(2))),
                filter);


        Memo memo = new Memo();
        memo.initialize(root);
        System.out.println(memo.copyOut().treeString());

        PlannerContext plannerContext = new PlannerContext(memo, null);
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), Double.MAX_VALUE);
        plannerContext.setCurrentJobContext(jobContext);

        List<Rule<Plan>> fakeRules = Lists.newArrayList(new PushPredicateThroughJoin().build());
        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(), fakeRules, jobContext);
        plannerContext.pushJob(rewriteTopDownJob);
        plannerContext.getJobScheduler().executeJobPool(plannerContext);

        Group rootGroup = memo.getRoot();
        System.out.println(memo.copyOut().treeString());
        Operator join2 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().getOperator();
        Operator join3 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(0).getLogicalExpression().getOperator();
        Operator op1 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(0).getLogicalExpression().child(0).getLogicalExpression().getOperator();
        Operator op2 = rootGroup.getLogicalExpression().child(0).getLogicalExpression().child(0).getLogicalExpression().child(1).getLogicalExpression().getOperator();

        Assertions.assertTrue(join2 instanceof LogicalJoin);
        Assertions.assertTrue(join3 instanceof LogicalJoin);
        Assertions.assertTrue(op1 instanceof LogicalFilter);
        Assertions.assertTrue(op2 instanceof LogicalFilter);

        Assertions.assertEquals(((LogicalJoin) join2).getCondition().get(), whereCondition2);
        Assertions.assertEquals(((LogicalJoin) join3).getCondition().get(), whereCondition1);
        Assertions.assertEquals(((LogicalFilter) op1).getPredicates().toSql(), whereCondition3result.toSql());
        Assertions.assertEquals(((LogicalFilter) op2).getPredicates(), whereCondition4);
    }
}

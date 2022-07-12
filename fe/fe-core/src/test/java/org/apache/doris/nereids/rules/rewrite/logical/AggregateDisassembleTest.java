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
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.operators.plans.AggPhase;
import org.apache.doris.nereids.operators.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.operators.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.AggregateDisassemble;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.Plans;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnaryPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AggregateDisassembleTest implements Plans {
    private Plan rStudent;

    @BeforeAll
    public final void beforeAll() {
        Table student = new Table(0L, "student", Table.TableType.OLAP,
                ImmutableList.of(new Column("id", Type.INT, true, AggregateType.NONE, true, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, true, "", ""),
                        new Column("age", Type.INT, true, AggregateType.NONE, true, "", "")));
        rStudent = plan(new LogicalOlapScan(student, ImmutableList.of("student")));
    }

    /**
     * the initial plan is:
     *   Aggregate(phase: [GLOBAL], outputExpr: [age, SUM(id) as sum], groupByExpr: [age])
     *   +--childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [GLOBAL], outputExpr: [a, SUM(b) as c], groupByExpr: [a])
     *   +--Aggregate(phase: [LOCAL], outputExpr: [age as a, SUM(id) as b], groupByExpr: [age])
     *       +--childPlan(id, name, age)
     */
    @Test
    public void slotReferenceGroupBy() {
        List<Expression> groupExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot());
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot(),
                new Alias(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = plan(new LogicalAggregate(groupExpressionList, outputExpressionList), rStudent);

        Memo memo = new Memo();
        memo.initialize(root);

        PlannerContext plannerContext = new PlannerContext(memo, new ConnectContext());
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), 0);
        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(),
                ImmutableList.of(new AggregateDisassemble().build()), jobContext);
        plannerContext.pushJob(rewriteTopDownJob);
        plannerContext.getJobScheduler().executeJobPool(plannerContext);

        Plan after = memo.copyOut();

        Assertions.assertTrue(after instanceof LogicalUnaryPlan);
        Assertions.assertTrue(after.getOperator() instanceof LogicalAggregate);
        Assertions.assertTrue(after.child(0) instanceof LogicalUnaryPlan);
        LogicalAggregate global = (LogicalAggregate) after.getOperator();
        LogicalAggregate local = (LogicalAggregate) after.child(0).getOperator();
        Assertions.assertEquals(AggPhase.GLOBAL, global.getAggPhase());
        Assertions.assertEquals(AggPhase.LOCAL, local.getAggPhase());

        Expression localOutput0 = rStudent.getOutput().get(2).toSlot();
        Expression localOutput1 = new Sum(rStudent.getOutput().get(0).toSlot());
        Expression localGroupBy = rStudent.getOutput().get(2).toSlot();

        Assertions.assertEquals(2, local.getOutputExpressionList().size());
        Assertions.assertTrue(local.getOutputExpressionList().get(0) instanceof SlotReference);
        Assertions.assertEquals(localOutput0, local.getOutputExpressionList().get(0));
        Assertions.assertTrue(local.getOutputExpressionList().get(1) instanceof Alias);
        Assertions.assertEquals(localOutput1, local.getOutputExpressionList().get(1).child(0));
        Assertions.assertEquals(1, local.getGroupByExpressionList().size());
        Assertions.assertEquals(localGroupBy, local.getGroupByExpressionList().get(0));

        Expression globalOutput0 = local.getOutputExpressionList().get(0).toSlot();
        Expression globalOutput1 = new Sum(local.getOutputExpressionList().get(1).toSlot());
        Expression globalGroupBy = local.getOutputExpressionList().get(0).toSlot();

        Assertions.assertEquals(2, global.getOutputExpressionList().size());
        Assertions.assertTrue(global.getOutputExpressionList().get(0) instanceof SlotReference);
        Assertions.assertEquals(globalOutput0, global.getOutputExpressionList().get(0));
        Assertions.assertTrue(global.getOutputExpressionList().get(1) instanceof Alias);
        Assertions.assertEquals(globalOutput1, global.getOutputExpressionList().get(1).child(0));
        Assertions.assertEquals(1, global.getGroupByExpressionList().size());
        Assertions.assertEquals(globalGroupBy, global.getGroupByExpressionList().get(0));

        // check id:
        Assertions.assertEquals(outputExpressionList.get(0).getExprId(),
                global.getOutputExpressionList().get(0).getExprId());
        Assertions.assertEquals(outputExpressionList.get(1).getExprId(),
                global.getOutputExpressionList().get(1).getExprId());
    }

    /**
     * the initial plan is:
     *   Aggregate(phase: [GLOBAL], outputExpr: [(age + 1) as key, SUM(id) as sum], groupByExpr: [age + 1])
     *   +--childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [GLOBAL], outputExpr: [a, SUM(b) as c], groupByExpr: [a])
     *   +--Aggregate(phase: [LOCAL], outputExpr: [(age + 1) as a, SUM(id) as b], groupByExpr: [age + 1])
     *       +--childPlan(id, name, age)
     */
    @Test
    public void aliasGroupBy() {
        List<Expression> groupExpressionList = Lists.newArrayList(
                new Add(rStudent.getOutput().get(2).toSlot(), new Literal(1)));
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new Add(rStudent.getOutput().get(2).toSlot(), new Literal(1)), "key"),
                new Alias(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = plan(new LogicalAggregate(groupExpressionList, outputExpressionList), rStudent);

        Memo memo = new Memo();
        memo.initialize(root);

        PlannerContext plannerContext = new PlannerContext(memo, new ConnectContext());
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), 0);
        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(),
                ImmutableList.of(new AggregateDisassemble().build()), jobContext);
        plannerContext.pushJob(rewriteTopDownJob);
        plannerContext.getJobScheduler().executeJobPool(plannerContext);

        Plan after = memo.copyOut();

        Assertions.assertTrue(after instanceof LogicalUnaryPlan);
        Assertions.assertTrue(after.getOperator() instanceof LogicalAggregate);
        Assertions.assertTrue(after.child(0) instanceof LogicalUnaryPlan);
        LogicalAggregate global = (LogicalAggregate) after.getOperator();
        LogicalAggregate local = (LogicalAggregate) after.child(0).getOperator();
        Assertions.assertEquals(AggPhase.GLOBAL, global.getAggPhase());
        Assertions.assertEquals(AggPhase.LOCAL, local.getAggPhase());

        Expression localOutput0 = new Add(rStudent.getOutput().get(2).toSlot(), new Literal(1));
        Expression localOutput1 = new Sum(rStudent.getOutput().get(0).toSlot());
        Expression localGroupBy = new Add(rStudent.getOutput().get(2).toSlot(), new Literal(1));

        Assertions.assertEquals(2, local.getOutputExpressionList().size());
        Assertions.assertTrue(local.getOutputExpressionList().get(0) instanceof Alias);
        Assertions.assertEquals(localOutput0, local.getOutputExpressionList().get(0).child(0));
        Assertions.assertTrue(local.getOutputExpressionList().get(1) instanceof Alias);
        Assertions.assertEquals(localOutput1, local.getOutputExpressionList().get(1).child(0));
        Assertions.assertEquals(1, local.getGroupByExpressionList().size());
        Assertions.assertEquals(localGroupBy, local.getGroupByExpressionList().get(0));

        Expression globalOutput0 = local.getOutputExpressionList().get(0).toSlot();
        Expression globalOutput1 = new Sum(local.getOutputExpressionList().get(1).toSlot());
        Expression globalGroupBy = local.getOutputExpressionList().get(0).toSlot();

        Assertions.assertEquals(2, global.getOutputExpressionList().size());
        Assertions.assertTrue(global.getOutputExpressionList().get(0) instanceof Alias);
        Assertions.assertEquals(globalOutput0, global.getOutputExpressionList().get(0).child(0));
        Assertions.assertTrue(global.getOutputExpressionList().get(1) instanceof Alias);
        Assertions.assertEquals(globalOutput1, global.getOutputExpressionList().get(1).child(0));
        Assertions.assertEquals(1, global.getGroupByExpressionList().size());
        Assertions.assertEquals(globalGroupBy, global.getGroupByExpressionList().get(0));

        // check id:
        Assertions.assertEquals(outputExpressionList.get(0).getExprId(),
                global.getOutputExpressionList().get(0).getExprId());
        Assertions.assertEquals(outputExpressionList.get(1).getExprId(),
                global.getOutputExpressionList().get(1).getExprId());
    }

    /**
     * the initial plan is:
     *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(id) as sum], groupByExpr: [])
     *   +--childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(b) as b], groupByExpr: [])
     *   +--Aggregate(phase: [LOCAL], outputExpr: [SUM(id) as a], groupByExpr: [])
     *       +--childPlan(id, name, age)
     */
    @Test
    public void globalAggregate() {
        List<Expression> groupExpressionList = Lists.newArrayList();
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = plan(new LogicalAggregate(groupExpressionList, outputExpressionList), rStudent);

        Memo memo = new Memo();
        memo.initialize(root);

        PlannerContext plannerContext = new PlannerContext(memo, new ConnectContext());
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), 0);
        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(),
                ImmutableList.of(new AggregateDisassemble().build()), jobContext);
        plannerContext.pushJob(rewriteTopDownJob);
        plannerContext.getJobScheduler().executeJobPool(plannerContext);

        Plan after = memo.copyOut();

        Assertions.assertTrue(after instanceof LogicalUnaryPlan);
        Assertions.assertTrue(after.getOperator() instanceof LogicalAggregate);
        Assertions.assertTrue(after.child(0) instanceof LogicalUnaryPlan);
        LogicalAggregate global = (LogicalAggregate) after.getOperator();
        LogicalAggregate local = (LogicalAggregate) after.child(0).getOperator();
        Assertions.assertEquals(AggPhase.GLOBAL, global.getAggPhase());
        Assertions.assertEquals(AggPhase.LOCAL, local.getAggPhase());

        Expression localOutput0 = new Sum(rStudent.getOutput().get(0).toSlot());

        Assertions.assertEquals(1, local.getOutputExpressionList().size());
        Assertions.assertTrue(local.getOutputExpressionList().get(0) instanceof Alias);
        Assertions.assertEquals(localOutput0, local.getOutputExpressionList().get(0).child(0));
        Assertions.assertEquals(0, local.getGroupByExpressionList().size());

        Expression globalOutput0 = new Sum(local.getOutputExpressionList().get(0).toSlot());

        Assertions.assertEquals(1, global.getOutputExpressionList().size());
        Assertions.assertTrue(global.getOutputExpressionList().get(0) instanceof Alias);
        Assertions.assertEquals(globalOutput0, global.getOutputExpressionList().get(0).child(0));
        Assertions.assertEquals(0, global.getGroupByExpressionList().size());

        // check id:
        Assertions.assertEquals(outputExpressionList.get(0).getExprId(),
                global.getOutputExpressionList().get(0).getExprId());
    }

    /**
     * the initial plan is:
     *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(id) as sum], groupByExpr: [age])
     *   +--childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(b) as c], groupByExpr: [a])
     *   +--Aggregate(phase: [LOCAL], outputExpr: [age as a, SUM(id) as b], groupByExpr: [age])
     *       +--childPlan(id, name, age)
     */
    @Test
    public void groupExpressionNotInOutput() {
        List<Expression> groupExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot());
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = plan(new LogicalAggregate(groupExpressionList, outputExpressionList), rStudent);

        Memo memo = new Memo();
        memo.initialize(root);

        PlannerContext plannerContext = new PlannerContext(memo, new ConnectContext());
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), 0);
        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(),
                ImmutableList.of(new AggregateDisassemble().build()), jobContext);
        plannerContext.pushJob(rewriteTopDownJob);
        plannerContext.getJobScheduler().executeJobPool(plannerContext);

        Plan after = memo.copyOut();

        Assertions.assertTrue(after instanceof LogicalUnaryPlan);
        Assertions.assertTrue(after.getOperator() instanceof LogicalAggregate);
        Assertions.assertTrue(after.child(0) instanceof LogicalUnaryPlan);
        LogicalAggregate global = (LogicalAggregate) after.getOperator();
        LogicalAggregate local = (LogicalAggregate) after.child(0).getOperator();
        Assertions.assertEquals(AggPhase.GLOBAL, global.getAggPhase());
        Assertions.assertEquals(AggPhase.LOCAL, local.getAggPhase());

        Expression localOutput0 = rStudent.getOutput().get(2).toSlot();
        Expression localOutput1 = new Sum(rStudent.getOutput().get(0).toSlot());
        Expression localGroupBy = rStudent.getOutput().get(2).toSlot();

        Assertions.assertEquals(2, local.getOutputExpressionList().size());
        Assertions.assertTrue(local.getOutputExpressionList().get(0) instanceof SlotReference);
        Assertions.assertEquals(localOutput0, local.getOutputExpressionList().get(0));
        Assertions.assertTrue(local.getOutputExpressionList().get(1) instanceof Alias);
        Assertions.assertEquals(localOutput1, local.getOutputExpressionList().get(1).child(0));
        Assertions.assertEquals(1, local.getGroupByExpressionList().size());
        Assertions.assertEquals(localGroupBy, local.getGroupByExpressionList().get(0));

        Expression globalOutput0 = new Sum(local.getOutputExpressionList().get(1).toSlot());
        Expression globalGroupBy = local.getOutputExpressionList().get(0).toSlot();

        Assertions.assertEquals(1, global.getOutputExpressionList().size());
        Assertions.assertTrue(global.getOutputExpressionList().get(0) instanceof Alias);
        Assertions.assertEquals(globalOutput0, global.getOutputExpressionList().get(0).child(0));
        Assertions.assertEquals(1, global.getGroupByExpressionList().size());
        Assertions.assertEquals(globalGroupBy, global.getGroupByExpressionList().get(0));

        // check id:
        Assertions.assertEquals(outputExpressionList.get(0).getExprId(),
                global.getOutputExpressionList().get(0).getExprId());
    }
}

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

import org.apache.doris.nereids.rules.rewrite.AggregateDisassemble;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Count;
import org.apache.doris.nereids.trees.expressions.functions.Sum;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnary;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.nereids.util.PlanRewriter;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AggregateDisassembleTest {
    private Plan rStudent;

    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student, ImmutableList.of(""));
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
        Plan root = new LogicalAggregate(groupExpressionList, outputExpressionList, rStudent);

        Plan after = rewrite(root);

        Assertions.assertTrue(after instanceof LogicalUnary);
        Assertions.assertTrue(after instanceof LogicalAggregate);
        Assertions.assertTrue(after.child(0) instanceof LogicalUnary);
        LogicalAggregate<Plan> global = (LogicalAggregate) after;
        LogicalAggregate<Plan> local = (LogicalAggregate) after.child(0);
        Assertions.assertEquals(AggPhase.GLOBAL, global.getAggPhase());
        Assertions.assertEquals(AggPhase.LOCAL, local.getAggPhase());

        Expression localOutput0 = rStudent.getOutput().get(2).toSlot();
        Expression localOutput1 = new Sum(rStudent.getOutput().get(0).toSlot());
        Expression localGroupBy = rStudent.getOutput().get(2).toSlot();

        Assertions.assertEquals(2, local.getOutputExpressions().size());
        Assertions.assertTrue(local.getOutputExpressions().get(0) instanceof SlotReference);
        Assertions.assertEquals(localOutput0, local.getOutputExpressions().get(0));
        Assertions.assertTrue(local.getOutputExpressions().get(1) instanceof Alias);
        Assertions.assertEquals(localOutput1, local.getOutputExpressions().get(1).child(0));
        Assertions.assertEquals(1, local.getGroupByExpressions().size());
        Assertions.assertEquals(localGroupBy, local.getGroupByExpressions().get(0));

        Expression globalOutput0 = local.getOutputExpressions().get(0).toSlot();
        Expression globalOutput1 = new Sum(local.getOutputExpressions().get(1).toSlot());
        Expression globalGroupBy = local.getOutputExpressions().get(0).toSlot();

        Assertions.assertEquals(2, global.getOutputExpressions().size());
        Assertions.assertTrue(global.getOutputExpressions().get(0) instanceof SlotReference);
        Assertions.assertEquals(globalOutput0, global.getOutputExpressions().get(0));
        Assertions.assertTrue(global.getOutputExpressions().get(1) instanceof Alias);
        Assertions.assertEquals(globalOutput1, global.getOutputExpressions().get(1).child(0));
        Assertions.assertEquals(1, global.getGroupByExpressions().size());
        Assertions.assertEquals(globalGroupBy, global.getGroupByExpressions().get(0));

        // check id:
        Assertions.assertEquals(outputExpressionList.get(0).getExprId(),
                global.getOutputExpressions().get(0).getExprId());
        Assertions.assertEquals(outputExpressionList.get(1).getExprId(),
                global.getOutputExpressions().get(1).getExprId());
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
                new Add(rStudent.getOutput().get(2).toSlot(), new IntegerLiteral(1)));
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new Add(rStudent.getOutput().get(2).toSlot(), new IntegerLiteral(1)), "key"),
                new Alias(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList, rStudent);

        Plan after = rewrite(root);

        Assertions.assertTrue(after instanceof LogicalUnary);
        Assertions.assertTrue(after instanceof LogicalAggregate);
        Assertions.assertTrue(after.child(0) instanceof LogicalUnary);
        LogicalAggregate<Plan> global = (LogicalAggregate) after;
        LogicalAggregate<Plan> local = (LogicalAggregate) after.child(0);
        Assertions.assertEquals(AggPhase.GLOBAL, global.getAggPhase());
        Assertions.assertEquals(AggPhase.LOCAL, local.getAggPhase());

        Expression localOutput0 = new Add(rStudent.getOutput().get(2).toSlot(), new IntegerLiteral(1));
        Expression localOutput1 = new Sum(rStudent.getOutput().get(0).toSlot());
        Expression localGroupBy = new Add(rStudent.getOutput().get(2).toSlot(), new IntegerLiteral(1));

        Assertions.assertEquals(2, local.getOutputExpressions().size());
        Assertions.assertTrue(local.getOutputExpressions().get(0) instanceof Alias);
        Assertions.assertEquals(localOutput0, local.getOutputExpressions().get(0).child(0));
        Assertions.assertTrue(local.getOutputExpressions().get(1) instanceof Alias);
        Assertions.assertEquals(localOutput1, local.getOutputExpressions().get(1).child(0));
        Assertions.assertEquals(1, local.getGroupByExpressions().size());
        Assertions.assertEquals(localGroupBy, local.getGroupByExpressions().get(0));

        Expression globalOutput0 = local.getOutputExpressions().get(0).toSlot();
        Expression globalOutput1 = new Sum(local.getOutputExpressions().get(1).toSlot());
        Expression globalGroupBy = local.getOutputExpressions().get(0).toSlot();

        Assertions.assertEquals(2, global.getOutputExpressions().size());
        Assertions.assertTrue(global.getOutputExpressions().get(0) instanceof Alias);
        Assertions.assertEquals(globalOutput0, global.getOutputExpressions().get(0).child(0));
        Assertions.assertTrue(global.getOutputExpressions().get(1) instanceof Alias);
        Assertions.assertEquals(globalOutput1, global.getOutputExpressions().get(1).child(0));
        Assertions.assertEquals(1, global.getGroupByExpressions().size());
        Assertions.assertEquals(globalGroupBy, global.getGroupByExpressions().get(0));

        // check id:
        Assertions.assertEquals(outputExpressionList.get(0).getExprId(),
                global.getOutputExpressions().get(0).getExprId());
        Assertions.assertEquals(outputExpressionList.get(1).getExprId(),
                global.getOutputExpressions().get(1).getExprId());
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
        Plan root = new LogicalAggregate(groupExpressionList, outputExpressionList, rStudent);

        Plan after = rewrite(root);

        Assertions.assertTrue(after instanceof LogicalUnary);
        Assertions.assertTrue(after instanceof LogicalAggregate);
        Assertions.assertTrue(after.child(0) instanceof LogicalUnary);
        LogicalAggregate<Plan> global = (LogicalAggregate) after;
        LogicalAggregate<Plan> local = (LogicalAggregate) after.child(0);
        Assertions.assertEquals(AggPhase.GLOBAL, global.getAggPhase());
        Assertions.assertEquals(AggPhase.LOCAL, local.getAggPhase());

        Expression localOutput0 = new Sum(rStudent.getOutput().get(0).toSlot());

        Assertions.assertEquals(1, local.getOutputExpressions().size());
        Assertions.assertTrue(local.getOutputExpressions().get(0) instanceof Alias);
        Assertions.assertEquals(localOutput0, local.getOutputExpressions().get(0).child(0));
        Assertions.assertEquals(0, local.getGroupByExpressions().size());

        Expression globalOutput0 = new Sum(local.getOutputExpressions().get(0).toSlot());

        Assertions.assertEquals(1, global.getOutputExpressions().size());
        Assertions.assertTrue(global.getOutputExpressions().get(0) instanceof Alias);
        Assertions.assertEquals(globalOutput0, global.getOutputExpressions().get(0).child(0));
        Assertions.assertEquals(0, global.getGroupByExpressions().size());

        // check id:
        Assertions.assertEquals(outputExpressionList.get(0).getExprId(),
                global.getOutputExpressions().get(0).getExprId());
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
        Plan root = new LogicalAggregate(groupExpressionList, outputExpressionList, rStudent);

        Plan after = rewrite(root);

        Assertions.assertTrue(after instanceof LogicalUnary);
        Assertions.assertTrue(after instanceof LogicalAggregate);
        Assertions.assertTrue(after.child(0) instanceof LogicalUnary);
        LogicalAggregate<Plan> global = (LogicalAggregate) after;
        LogicalAggregate<Plan> local = (LogicalAggregate) after.child(0);
        Assertions.assertEquals(AggPhase.GLOBAL, global.getAggPhase());
        Assertions.assertEquals(AggPhase.LOCAL, local.getAggPhase());

        Expression localOutput0 = rStudent.getOutput().get(2).toSlot();
        Expression localOutput1 = new Sum(rStudent.getOutput().get(0).toSlot());
        Expression localGroupBy = rStudent.getOutput().get(2).toSlot();

        Assertions.assertEquals(2, local.getOutputExpressions().size());
        Assertions.assertTrue(local.getOutputExpressions().get(0) instanceof SlotReference);
        Assertions.assertEquals(localOutput0, local.getOutputExpressions().get(0));
        Assertions.assertTrue(local.getOutputExpressions().get(1) instanceof Alias);
        Assertions.assertEquals(localOutput1, local.getOutputExpressions().get(1).child(0));
        Assertions.assertEquals(1, local.getGroupByExpressions().size());
        Assertions.assertEquals(localGroupBy, local.getGroupByExpressions().get(0));

        Expression globalOutput0 = new Sum(local.getOutputExpressions().get(1).toSlot());
        Expression globalGroupBy = local.getOutputExpressions().get(0).toSlot();

        Assertions.assertEquals(1, global.getOutputExpressions().size());
        Assertions.assertTrue(global.getOutputExpressions().get(0) instanceof Alias);
        Assertions.assertEquals(globalOutput0, global.getOutputExpressions().get(0).child(0));
        Assertions.assertEquals(1, global.getGroupByExpressions().size());
        Assertions.assertEquals(globalGroupBy, global.getGroupByExpressions().get(0));

        // check id:
        Assertions.assertEquals(outputExpressionList.get(0).getExprId(),
                global.getOutputExpressions().get(0).getExprId());
    }

    /**
     * the initial plan is:
     *   Aggregate(phase: [GLOBAL], outputExpr: [(COUNT(distinct age + 1) + 2) as c], groupByExpr: [id + 3])
     *   +-- childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [DISTINCT_LOCAL], outputExpr: [(COUNT(distinct b) + 2) as c], groupByExpr: [a])
     *   +-- Aggregate(phase: [GLOBAL], outputExpr: [a, b], groupByExpr: [a, b])
     *       +-- Aggregate(phase: [LOCAL], outputExpr: [(id + 3) as a, (age + 1) as b], groupByExpr: [id + 3, age + 1])
     *           +-- childPlan(id, name, age)
     */
    @Test
    public void distinctAggregateWithGroupBy() {
        List<Expression> groupExpressionList = Lists.newArrayList(
                new Add(rStudent.getOutput().get(0).toSlot(), new IntegerLiteral(3)));
        List<NamedExpression> outputExpressionList = Lists.newArrayList(new Alias(
                new Add(new Count(new Add(rStudent.getOutput().get(2).toSlot(), new IntegerLiteral(1)), true),
                        new IntegerLiteral(2)), "c"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList, rStudent);

        Plan after = rewrite(root);

        Assertions.assertTrue(after instanceof LogicalUnary);
        Assertions.assertTrue(after instanceof LogicalAggregate);
        Assertions.assertTrue(after.child(0) instanceof LogicalUnary);
        LogicalAggregate<Plan> distinctLocal = (LogicalAggregate) after;
        LogicalAggregate<Plan> global = (LogicalAggregate) after.child(0);
        LogicalAggregate<Plan> local = (LogicalAggregate) after.child(0).child(0);
        Assertions.assertEquals(AggPhase.DISTINCT_LOCAL, distinctLocal.getAggPhase());
        Assertions.assertEquals(AggPhase.GLOBAL, global.getAggPhase());
        Assertions.assertEquals(AggPhase.LOCAL, local.getAggPhase());
        // check local:
        // id + 3
        Expression localOutput0 = new Add(rStudent.getOutput().get(0).toSlot(), new IntegerLiteral(3));
        // age + 1
        Expression localOutput1 = new Add(rStudent.getOutput().get(2).toSlot(), new IntegerLiteral(1));
        // id + 3
        Expression localGroupBy0 = new Add(rStudent.getOutput().get(0).toSlot(), new IntegerLiteral(3));
        // age + 1
        Expression localGroupBy1 = new Add(rStudent.getOutput().get(2).toSlot(), new IntegerLiteral(1));

        Assertions.assertEquals(2, local.getOutputExpressions().size());
        Assertions.assertTrue(local.getOutputExpressions().get(0) instanceof Alias);
        Assertions.assertEquals(localOutput0, local.getOutputExpressions().get(0).child(0));
        Assertions.assertTrue(local.getOutputExpressions().get(1) instanceof Alias);
        Assertions.assertEquals(localOutput1, local.getOutputExpressions().get(1).child(0));
        Assertions.assertEquals(2, local.getGroupByExpressions().size());
        Assertions.assertEquals(localGroupBy0, local.getGroupByExpressions().get(0));
        Assertions.assertEquals(localGroupBy1, local.getGroupByExpressions().get(1));

        // check global:
        Expression globalOutput0 = local.getOutputExpressions().get(0).toSlot();
        Expression globalOutput1 = local.getOutputExpressions().get(1).toSlot();
        Expression globalGroupBy0 = local.getOutputExpressions().get(0).toSlot();
        Expression globalGroupBy1 = local.getOutputExpressions().get(1).toSlot();

        Assertions.assertEquals(2, global.getOutputExpressions().size());
        Assertions.assertTrue(global.getOutputExpressions().get(0) instanceof SlotReference);
        Assertions.assertEquals(globalOutput0, global.getOutputExpressions().get(0));
        Assertions.assertTrue(global.getOutputExpressions().get(1) instanceof SlotReference);
        Assertions.assertEquals(globalOutput1, global.getOutputExpressions().get(1));
        Assertions.assertEquals(2, global.getGroupByExpressions().size());
        Assertions.assertEquals(globalGroupBy0, global.getGroupByExpressions().get(0));
        Assertions.assertEquals(globalGroupBy1, global.getGroupByExpressions().get(1));

        // check distinct local:
        Expression distinctLocalOutput = new Add(new Count(local.getOutputExpressions().get(1).toSlot(), true),
                new IntegerLiteral(2));
        Expression distinctLocalGroupBy = local.getOutputExpressions().get(0).toSlot();

        Assertions.assertEquals(1, distinctLocal.getOutputExpressions().size());
        Assertions.assertTrue(distinctLocal.getOutputExpressions().get(0) instanceof Alias);
        Assertions.assertEquals(distinctLocalOutput, distinctLocal.getOutputExpressions().get(0).child(0));
        Assertions.assertEquals(1, distinctLocal.getGroupByExpressions().size());
        Assertions.assertEquals(distinctLocalGroupBy, distinctLocal.getGroupByExpressions().get(0));

        // check id:
        Assertions.assertEquals(outputExpressionList.get(0).getExprId(),
                distinctLocal.getOutputExpressions().get(0).getExprId());
    }

    private Plan rewrite(Plan input) {
        return PlanRewriter.topDownRewrite(input, new ConnectContext(), new AggregateDisassemble());
    }
}

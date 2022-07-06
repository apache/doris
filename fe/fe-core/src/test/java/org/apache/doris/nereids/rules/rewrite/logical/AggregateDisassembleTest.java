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
import org.apache.doris.nereids.OptimizerContext;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.rewrite.RewriteTopDownJob;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.operators.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.operators.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.AggregateDisassemble;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.Plans;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
                ImmutableList.<Column>of(new Column("id", Type.INT, true, AggregateType.NONE, true, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, true, "", ""),
                        new Column("age", Type.INT, true, AggregateType.NONE, true, "", "")));
        rStudent = plan(new LogicalOlapScan(student, ImmutableList.of("student")));
    }

    @Test
    public void slotReferenceGroupBy() {
        List<Expression> groupExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot());
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot(),
                new Alias<>(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = plan(new LogicalAggregate(groupExpressionList, outputExpressionList), rStudent);

        Memo memo = new Memo();
        memo.initialize(root);
        System.out.println(memo.copyOut().treeString());

        OptimizerContext optimizerContext = new OptimizerContext(memo);
        PlannerContext plannerContext = new PlannerContext(optimizerContext, null, new PhysicalProperties());
        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(),
                ImmutableList.of(new AggregateDisassemble().build()), plannerContext);
        plannerContext.getOptimizerContext().pushJob(rewriteTopDownJob);
        plannerContext.getOptimizerContext().getJobScheduler().executeJobPool(plannerContext);

        System.out.println(memo.copyOut().treeString());
    }

    @Test
    public void aliasGroupBy() {
        List<Expression> groupExpressionList = Lists.newArrayList(
                new Add<>(rStudent.getOutput().get(2).toSlot(), new Literal(1)));
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias<>(new Add<>(rStudent.getOutput().get(2).toSlot(), new Literal(1)), "key"),
                new Alias<>(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = plan(new LogicalAggregate(groupExpressionList, outputExpressionList), rStudent);

        Memo memo = new Memo();
        memo.initialize(root);
        System.out.println(memo.copyOut().treeString());

        OptimizerContext optimizerContext = new OptimizerContext(memo);
        PlannerContext plannerContext = new PlannerContext(optimizerContext, null, new PhysicalProperties());
        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(),
                ImmutableList.of(new AggregateDisassemble().build()), plannerContext);
        plannerContext.getOptimizerContext().pushJob(rewriteTopDownJob);
        plannerContext.getOptimizerContext().getJobScheduler().executeJobPool(plannerContext);

        System.out.println(memo.copyOut().treeString());
    }

    @Test
    public void globalAgg() {
        List<Expression> groupExpressionList = Lists.newArrayList();
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias<>(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = plan(new LogicalAggregate(groupExpressionList, outputExpressionList), rStudent);

        Memo memo = new Memo();
        memo.initialize(root);
        System.out.println(memo.copyOut().treeString());

        OptimizerContext optimizerContext = new OptimizerContext(memo);
        PlannerContext plannerContext = new PlannerContext(optimizerContext, null, new PhysicalProperties());
        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(),
                ImmutableList.of(new AggregateDisassemble().build()), plannerContext);
        plannerContext.getOptimizerContext().pushJob(rewriteTopDownJob);
        plannerContext.getOptimizerContext().getJobScheduler().executeJobPool(plannerContext);

        System.out.println(memo.copyOut().treeString());
    }

    @Test
    public void groupExpressionNotInOutput() {
        List<Expression> groupExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot());
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias<>(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = plan(new LogicalAggregate(groupExpressionList, outputExpressionList), rStudent);

        Memo memo = new Memo();
        memo.initialize(root);
        System.out.println(memo.copyOut().treeString());

        OptimizerContext optimizerContext = new OptimizerContext(memo);
        PlannerContext plannerContext = new PlannerContext(optimizerContext, null, new PhysicalProperties());
        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(),
                ImmutableList.of(new AggregateDisassemble().build()), plannerContext);
        plannerContext.getOptimizerContext().pushJob(rewriteTopDownJob);
        plannerContext.getOptimizerContext().getJobScheduler().executeJobPool(plannerContext);

        System.out.println(memo.copyOut().treeString());
    }
}

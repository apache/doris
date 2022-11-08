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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.FieldChecker;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NormalizeAggregateTest implements PatternMatchSupported {
    private LogicalPlan rStudent;

    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(RelationId.createGenerator().getNextId(), PlanConstructor.student,
                ImmutableList.of(""));
    }

    /*-
     * original plan:
     * LogicalAggregate (phase: [GLOBAL], output: [name#2, sum(id#0) AS `sum`#4], groupBy: [name#2])
     * +--ScanOlapTable (student.student, output: [id#0, gender#1, name#2, age#3])
     *
     * after rewrite:
     * LogicalProject (name#2, sum(id)#5 AS `sum`#4)
     * +--LogicalAggregate (phase: [GLOBAL], output: [name#2, sum(id#0) AS `sum(id)`#5], groupBy: [name#2])
     *    +--ScanOlapTable (student.student, output: [id#0, gender#1, name#2, age#3])
     */
    @Test
    public void testSimpleKeyWithSimpleAggregateFunction() {
        NamedExpression key = rStudent.getOutput().get(2).toSlot();
        NamedExpression aggregateFunction = new Alias(new Sum(rStudent.getOutput().get(0).toSlot()), "sum");
        List<Expression> groupExpressionList = Lists.newArrayList(key);
        List<NamedExpression> outputExpressionList = Lists.newArrayList(key, aggregateFunction);
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList, rStudent);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new NormalizeAggregate())
                .matchesFromRoot(
                        logicalProject(
                                logicalAggregate(
                                        logicalOlapScan()
                                ).when(FieldChecker.check("groupByExpressions", ImmutableList.of(key)))
                                        .when(aggregate -> aggregate.getOutputExpressions().get(0).equals(key))
                                        .when(aggregate -> aggregate.getOutputExpressions().get(1).child(0)
                                                .equals(aggregateFunction.child(0)))
                                        .when(FieldChecker.check("normalized", true))
                        ).when(project -> project.getProjects().get(0).equals(key))
                                .when(project -> project.getProjects().get(1) instanceof Alias)
                                .when(project -> (project.getProjects().get(1)).getExprId()
                                        .equals(aggregateFunction.getExprId()))
                                .when(project -> project.getProjects().get(1).child(0) instanceof SlotReference)
                );
    }

    /*-
     * original plan:
     * LogicalAggregate (phase: [GLOBAL], output: [(sum((id#0 * 1)) + 2) AS `(sum((id * 1)) + 2)`#4], groupBy: [name#2])
     * +--ScanOlapTable (student.student, output: [id#0, gender#1, name#2, age#3])
     *
     * after rewrite:
     * LogicalProject ( projects=[(sum((id * 1))#6 + 2) AS `(sum((id * 1)) + 2)`#4] )
     * +--LogicalAggregate ( phase=LOCAL, outputExpr=[sum((id * 1)#5) AS `sum((id * 1))`#6], groupByExpr=[name#2] )
     *    +--LogicalProject ( projects=[name#2, (id#0 * 1) AS `(id * 1)`#5] )
     *       +--GroupPlan( GroupId#0 )
     */
    @Test
    public void testComplexFuncWithComplexOutputOfFunc() {
        Expression multiply = new Multiply(rStudent.getOutput().get(0).toSlot(), new IntegerLiteral(1));
        Expression aggregateFunction = new Sum(multiply);
        Expression complexOutput = new Add(aggregateFunction, new IntegerLiteral(2));
        Alias output = new Alias(complexOutput, complexOutput.toSql());
        List<NamedExpression> outputExpressionList = Lists.newArrayList(output);

        LogicalPlan root = new LogicalPlanBuilder(rStudent)
                .aggGroupUsingIndex(ImmutableList.of(2), outputExpressionList)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new NormalizeAggregate())
                .matchesFromRoot(
                        logicalProject(
                                logicalAggregate(
                                        logicalProject(
                                                logicalOlapScan()
                                        ).when(project -> project.getProjects().size() == 2)
                                                .when(project -> project.getProjects().get(0) instanceof SlotReference)
                                                .when(project -> project.getProjects().get(1).child(0).equals(multiply))
                                ).when(FieldChecker.check("groupByExpressions",
                                                ImmutableList.of(rStudent.getOutput().get(2))))
                                        .when(aggregate -> aggregate.getOutputExpressions().size() == 1)
                                        .when(aggregate -> aggregate.getOutputExpressions().get(0)
                                                .child(0) instanceof AggregateFunction)
                        ).when(project -> project.getProjects().size() == 1)
                                .when(project -> project.getProjects().get(0) instanceof Alias)
                                .when(project -> project.getProjects().get(0).getExprId().equals(output.getExprId()))
                                .when(project -> project.getProjects().get(0).child(0) instanceof Add)
                                .when(project -> project.getProjects().get(0).child(0)
                                        .child(0) instanceof SlotReference)
                                .when(project -> project.getProjects().get(0).child(0).child(1)
                                        .equals(new IntegerLiteral(2)))
                );
    }

    /*-
     * original plan:
     * LogicalAggregate (phase: [GLOBAL], output: [((gender#1 + 1) + 2) AS `((gender + 1) + 2)`#4], groupBy: [(gender#1 + 1)])
     * +--ScanOlapTable (student.student, output: [id#0, gender#1, name#2, age#3])
     *
     * after rewrite:
     * LogicalProject (((gender + 1)#5 + 2) AS `((gender + 1) + 2)`#4)
     * +--LogicalAggregate (phase: [GLOBAL], output: [(gender + 1)#5], groupBy: [(gender + 1)#5])
     *    +--LogicalProject ((gender#1 + 1) AS `(gender + 1)`#5)
     *       +--ScanOlapTable (student.student, output: [id#0, gender#1, name#2, age#3])
     */
    @Test
    public void testComplexKeyWithComplexOutputOfKey() {
        Expression key = new Add(rStudent.getOutput().get(1), new IntegerLiteral(1));
        Expression complexKeyOutput = new Add(key, new IntegerLiteral(2));
        NamedExpression keyOutput = new Alias(complexKeyOutput, complexKeyOutput.toSql());

        List<Expression> groupExpressionList = Lists.newArrayList(key);
        List<NamedExpression> outputExpressionList = Lists.newArrayList(keyOutput);

        LogicalPlan root = new LogicalPlanBuilder(rStudent)
                .agg(groupExpressionList, outputExpressionList)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new NormalizeAggregate())
                .matchesFromRoot(
                        logicalProject(
                                logicalAggregate(
                                        logicalProject(
                                                logicalOlapScan()
                                        ).when(project -> project.getProjects().size() == 1)
                                                .when(project -> project.getProjects().get(0) instanceof Alias)
                                                .when(project -> project.getProjects().get(0).child(0).equals(key))
                                ).when(aggregate -> aggregate.getGroupByExpressions().get(0) instanceof SlotReference)
                                        .when(aggregate -> aggregate.getOutputExpressions()
                                                .get(0) instanceof SlotReference)
                                        .when(aggregate -> aggregate.getGroupByExpressions()
                                                .equals(aggregate.getOutputExpressions()))
                        ).when(project -> project.getProjects().get(0).getExprId().equals(keyOutput.getExprId()))
                                .when(project -> project.getProjects().get(0).child(0) instanceof Add)
                                .when(project -> project.getProjects().get(0).child(0)
                                        .child(0) instanceof SlotReference)
                                .when(project -> project.getProjects().get(0).child(0).child(1)
                                        .equals(new IntegerLiteral(2)))

                );
    }
}

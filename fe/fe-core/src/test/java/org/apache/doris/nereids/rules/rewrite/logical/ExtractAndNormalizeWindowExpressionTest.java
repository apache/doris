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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.RelationUtil;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExtractAndNormalizeWindowExpressionTest implements MemoPatternMatchSupported {

    private LogicalPlan rStudent;

    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(RelationUtil.newRelationId(), PlanConstructor.student, ImmutableList.of());
    }

    @Test
    public void testSimpleWindowFunction() {
        NamedExpression id = rStudent.getOutput().get(0).toSlot();
        NamedExpression gender = rStudent.getOutput().get(1).toSlot();
        NamedExpression age = rStudent.getOutput().get(3).toSlot();

        List<Expression> partitionKeyList = ImmutableList.of(gender);
        List<OrderExpression> orderKeyList = ImmutableList.of(new OrderExpression(
                new OrderKey(age, true, true)));
        WindowExpression window = new WindowExpression(new Rank(), partitionKeyList, orderKeyList);
        Alias windowAlias = new Alias(window, window.toSql());

        List<NamedExpression> outputExpressions = Lists.newArrayList(id, windowAlias);
        Plan root = new LogicalProject<>(outputExpressions, rStudent);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new ExtractAndNormalizeWindowExpression())
                .matchesFromRoot(
                        logicalProject(
                                logicalWindow(
                                        logicalProject(
                                                logicalOlapScan()
                                        ).when(project -> {
                                            List<NamedExpression> projects = project.getProjects();
                                            return projects.get(0).equals(id)
                                                && projects.get(1).equals(gender)
                                                && projects.get(2).equals(age);
                                        })
                                ).when(logicalWindow -> logicalWindow.getWindowExpressions().get(0).equals(windowAlias))
                        ).when(project -> {
                            List<NamedExpression> projects = project.getProjects();
                            return projects.get(0).equals(id)
                                && projects.get(1) instanceof Alias;
                        })
                );
    }

    /*-
     * original plan:
     * LogicalProject (output: [id#0, rank() over(partition by id#0+2 order by age#3+2) AS `Window.toSql()`#4)
     * +--ScanOlapTable (student.student, output: [id#0, gender#1, name#2, age#3])
     *
     * after rewrite:
     * LogicalProject ( projects=[id#0, `Window.toSql()`#4] )
     * +--LogicalWindow (windowExpressions: [rank() over(partition by (id + 2)#5 order by (age3 + 2)#6 AS `Window.toSql()`#4)
     *    +--LogicalProject( projects=[id#0, (id#0 + 2) AS `(id + 2)`#5, (age#3 + 2) AS `(age + 2)`#6] )
     *       +--ScanOlapTable (student.student, output: [id#0, gender#1, name#2, age#3])
     */
    @Test
    public void testComplexWindowFunction() {
        NamedExpression id = rStudent.getOutput().get(0).toSlot();
        NamedExpression age = rStudent.getOutput().get(3).toSlot();

        List<Expression> partitionKeyList = ImmutableList.of(new Add(id, new IntegerLiteral(2)));
        List<OrderExpression> orderKeyList = ImmutableList.of(new OrderExpression(
                new OrderKey(new Add(age, new IntegerLiteral(2)), true, true)));
        WindowExpression window = new WindowExpression(new Rank(), partitionKeyList, orderKeyList);
        Alias windowAlias = new Alias(window, window.toSql());

        List<NamedExpression> outputExpressions = Lists.newArrayList(id, windowAlias);
        Plan root = new LogicalProject<>(outputExpressions, rStudent);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new ExtractAndNormalizeWindowExpression())
                .matchesFromRoot(
                        logicalProject(
                                logicalWindow(
                                        logicalProject(
                                                logicalOlapScan()
                                        ).when(project -> {
                                            List<NamedExpression> projects = project.getProjects();
                                            return projects.get(1) instanceof Alias && projects.get(1).child(0).child(0).equals(id)
                                                && projects.get(1).child(0).child(1).equals(new IntegerLiteral(2))
                                                && projects.get(2) instanceof Alias && projects.get(2).child(0).child(0).equals(age)
                                                && projects.get(2).child(0).child(1).equals(new IntegerLiteral(2))
                                                && projects.get(0).equals(id);
                                        })
                                ).when(logicalWindow -> {
                                    List<NamedExpression> outputs = logicalWindow.getWindowExpressions();
                                    WindowExpression newWindow = (WindowExpression) outputs.get(0).child(0);
                                    Expression pk = newWindow.getPartitionKeys().get(0);
                                    OrderExpression ok = newWindow.getOrderKeys().get(0);
                                    return !newWindow.equals(windowAlias.child(0))
                                        && pk instanceof SlotReference
                                        && ok.child() instanceof SlotReference;
                                })
                        ).when(project -> {
                            List<NamedExpression> projects = project.getProjects();
                            return projects.get(0) instanceof SlotReference
                                && projects.get(1) instanceof Alias;
                        })
                );
    }

    /*
     * select gender, sum(id+1), sum(id+1) over(partition by gender order by id+1)
     * from student
     * group by gender, id
     *
     * original plan:
     * LogicalAggregate (output: [gender#1, sum(id#0 + 1) As `sum`#4, sum(id#0 + 1) over(partition by gender#1 order by id#0 + 1) AS `Window.toSql()`#5],
     *                  groupBy: [gender#1, id#0])
     * +--ScanOlapTable (student.student, output: [id#0, gender#1, name#2, age#3])
     *
     * after rewrite:
     * LogicalProject ( projects=[gender#1, sum#4, Window.toSql()#5 AS `Window.toSql()`#5] )
     * +--LogicalWindow (windowExpressions: [sum((id + 1)#7) over(partition by gender#1 order by (id + 1)#7 AS `Window.toSql()`#5)
     *    +--LogicalProject( projects=[gender#1, sum#4 AS `sum`#4, (id#0 + 1) AS `(id + 1)`#7] )
     *       +--LogicalAggregate (output: [gender#1, id#0, sum((id + 1)#6) As `sum`#4],
     *                  groupBy: [gender#1, id#0])
     *          +--LogicalProject( projects=[gender#1, id#0, (id#0 + 1) AS `(id + 1)`#6] )
     *             +--ScanOlapTable (student.student, output: [id#0, gender#1, name#2, age#3])
     */
    @Test
    public void testComplexWindowFunctionTogetherWithAggregateFunction() {
        NamedExpression id = rStudent.getOutput().get(0).toSlot();
        NamedExpression gender = rStudent.getOutput().get(1).toSlot();
        Add add = new Add(id, new IntegerLiteral(1));
        Alias sumAlias = new Alias(new Sum(add), "sum");

        List<Expression> partitionKeyList = ImmutableList.of(gender);
        List<OrderExpression> orderKeyList = ImmutableList.of(new OrderExpression(new OrderKey(add, true, true)));
        WindowExpression window = new WindowExpression(new Sum(add), partitionKeyList, orderKeyList);
        Alias windowAlias = new Alias(window, window.toSql());

        List<Expression> groupKeys = Lists.newArrayList(gender, id);
        List<NamedExpression> outputExpressions = Lists.newArrayList(gender, sumAlias, windowAlias);
        Plan root = new LogicalAggregate<>(groupKeys, outputExpressions, rStudent);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new NormalizeAggregate())
                .matches(
                        logicalProject(
                                logicalAggregate()
                        ).when(project -> {
                            // ensure that WindowExpression in LogicalAgg will not be changed
                            // while AggregateFunction can be normalized as usual
                            // when Window's function is same as AggregateFunction.
                            // In this example, agg function [sum(id+1)] is same as Window's function [sum(id+1) over...]
                            List<NamedExpression> projects = project.getProjects();
                            return projects.get(1).child(0) instanceof SlotReference
                                && projects.get(2).equals(windowAlias);
                        })
                )
                .applyTopDown(new ExtractAndNormalizeWindowExpression())
                .matchesFromRoot(
                        logicalProject(
                                logicalWindow(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(
                                                                logicalOlapScan()
                                                        )
                                                )
                                        ).when(project -> project.getProjects().get(2).child(0).equals(add))
                                ).when(logicalWindow -> {
                                    WindowExpression newWindow = (WindowExpression) logicalWindow.getWindowExpressions().get(0).child(0);
                                    Expression func = newWindow.getFunction();
                                    Expression ok = newWindow.getOrderKeys().get(0).child(0);
                                    return !newWindow.equals(windowAlias.child(0))
                                        && func.child(0) instanceof SlotReference
                                        && ok instanceof SlotReference
                                        && func.child(0).equals(ok);
                                })
                        ).when(project -> {
                            List<NamedExpression> projects = project.getProjects();
                            return projects.get(0).equals(gender)
                                && projects.get(1) instanceof SlotReference
                                && projects.get(2) instanceof Alias
                                && projects.get(2).child(0) instanceof SlotReference;
                        })
                );
    }
}

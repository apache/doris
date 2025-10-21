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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.WindowFuncType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

public class GeneratePartitionTopnFromWindowTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
            ImmutableList.of(""));

    /*-
     * origin plan:
     *                project
     *                  |
     *                filter row_number <= 100
     *                  |
     *               window(ROW_NUMBER() as row_number PARTITION BY gender ORDER BY age)
     *                  |
     *               scan(student)
     *
     *  transformed plan:
     *                project
     *                  |
     *                filter row_number <= 100
     *                  |
     *               window(ROW_NUMBER() as row_number PARTITION BY gender ORDER BY age)
     *                  |
     *      partitionTopN(row_number(), partition by gender, order by age, hasGlobalLimit: false, partitionLimit: 100)
     *                  |
     *               scan(student)
     */
    @Test
    public void testGeneratePartitionTopnFromWindow() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        context.getSessionVariable().setEnablePartitionTopN(true);
        NamedExpression gender = scan.getOutput().get(1).toSlot();
        NamedExpression age = scan.getOutput().get(3).toSlot();

        List<Expression> partitionKeyList = ImmutableList.of(gender);
        List<OrderExpression> orderKeyList = ImmutableList.of(new OrderExpression(
                new OrderKey(age, true, true)));
        WindowFrame windowFrame = new WindowFrame(WindowFrame.FrameUnitsType.ROWS,
                WindowFrame.FrameBoundary.newPrecedingBoundary(),
                WindowFrame.FrameBoundary.newCurrentRowBoundary());
        WindowExpression window1 = new WindowExpression(new RowNumber(), partitionKeyList, orderKeyList, windowFrame);
        Alias windowAlias1 = new Alias(window1, window1.toSql());
        List<NamedExpression> expressions = Lists.newArrayList(windowAlias1);
        LogicalWindow<LogicalOlapScan> window = new LogicalWindow<>(expressions, scan);
        Expression filterPredicate = new LessThanEqual(window.getOutput().get(4).toSlot(), Literal.of(100));

        LogicalPlan plan = new LogicalPlanBuilder(window)
                .filter(filterPredicate)
                .project(ImmutableList.of(0))
                .build();

        PlanChecker.from(context, plan)
                .applyTopDown(new CreatePartitionTopNFromWindow())
                .matches(
                    logicalProject(
                        logicalFilter(
                            logicalWindow(
                                logicalPartitionTopN(
                                    logicalOlapScan()
                                ).when(logicalPartitionTopN -> {
                                    WindowFuncType funName = logicalPartitionTopN.getFunction();
                                    List<Expression> partitionKeys = logicalPartitionTopN.getPartitionKeys();
                                    List<OrderExpression> orderKeys = logicalPartitionTopN.getOrderKeys();
                                    boolean hasGlobalLimit = logicalPartitionTopN.hasGlobalLimit();
                                    long partitionLimit = logicalPartitionTopN.getPartitionLimit();
                                    return funName == WindowFuncType.ROW_NUMBER && partitionKeys.equals(partitionKeyList)
                                        && orderKeys.equals(orderKeyList) && !hasGlobalLimit && partitionLimit == 100;
                                })
                            )
                        ).when(filter -> filter.getConjuncts().equals(ImmutableSet.of(filterPredicate)))
                    )
                );
    }
}

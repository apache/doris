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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
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
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

class PushdownFilterThroughWindowTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
            PlanConstructor.student,
            ImmutableList.of(""));

    @Test
    void pushDownFilterThroughWindowTest() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        NamedExpression age = scan.getOutput().get(3).toSlot();
        List<Expression> partitionKeyList = ImmutableList.of(age);
        WindowFrame windowFrame = new WindowFrame(WindowFrame.FrameUnitsType.ROWS,
                WindowFrame.FrameBoundary.newPrecedingBoundary(),
                WindowFrame.FrameBoundary.newCurrentRowBoundary());
        WindowExpression window1 = new WindowExpression(new RowNumber(), partitionKeyList,
                Lists.newArrayList(), windowFrame);
        Alias windowAlias1 = new Alias(window1, window1.toSql());
        List<NamedExpression> expressions = Lists.newArrayList(windowAlias1);
        LogicalWindow<LogicalOlapScan> window = new LogicalWindow<>(expressions, scan);
        Expression filterPredicate = new EqualTo(age, Literal.of(100));

        LogicalPlan plan = new LogicalPlanBuilder(window)
                .filter(filterPredicate)
                .project(ImmutableList.of(0))
                .build();
        PlanChecker.from(context, plan)
                .applyTopDown(new PushdownFilterThroughWindow())
                .matches(
                        logicalProject(
                                logicalWindow(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> {
                                            if (filter.getConjuncts().size() != 1) {
                                                return false;
                                            }
                                            Expression conj = filter.getConjuncts().iterator().next();
                                            if (!(conj instanceof EqualTo)) {
                                                return false;
                                            }
                                            EqualTo eq = (EqualTo) conj;
                                            return eq.left().equals(age);

                                        })
                                )
                        )
                );
    }
}


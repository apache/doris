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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.FirstValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lag;
import org.apache.doris.nereids.trees.expressions.functions.window.LastValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lead;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.functions.window.WindowFunction;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CheckAndStandardizeWindowFunctionTest implements MemoPatternMatchSupported {

    private LogicalPlan rStudent;
    private NamedExpression gender;
    private NamedExpression age;
    private List<Expression> partitionKeyList;
    private List<OrderExpression> orderKeyList;
    private WindowFrame defaultWindowFrame;

    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student, ImmutableList.of());

        gender = rStudent.getOutput().get(1).toSlot();
        age = rStudent.getOutput().get(3).toSlot();
        partitionKeyList = ImmutableList.of(gender);
        orderKeyList = ImmutableList.of(new OrderExpression(new OrderKey(age, true, true)));
        defaultWindowFrame = new WindowFrame(FrameUnitsType.RANGE,
            FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());
    }

    @Test
    public void testRankAndDenseRank() {
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.RANGE,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());
        List<WindowFunction> funcList = Lists.newArrayList(new Rank(), new DenseRank());

        for (WindowFunction func : funcList) {
            WindowExpression window = new WindowExpression(func, partitionKeyList, orderKeyList);
            Alias windowAlias = new Alias(window, window.toSql());
            List<NamedExpression> outputExpressions = Lists.newArrayList(windowAlias);
            Plan root = new LogicalProject<>(outputExpressions, rStudent);

            PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                    .applyTopDown(new ExtractAndNormalizeWindowExpression())
                    .applyTopDown(new CheckAndStandardizeWindowFunctionAndFrame())
                    .matches(
                            logicalWindow()
                                    .when(logicalWindow -> {
                                        WindowExpression newWindow = (WindowExpression) logicalWindow.getWindowExpressions().get(0).child(0);
                                        return newWindow.getWindowFrame().get().equals(requiredFrame);
                                    })
                    );
        }
    }

    @Test
    public void testRowNumber() {
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());
        WindowExpression window = new WindowExpression(new RowNumber(), partitionKeyList, orderKeyList);
        Alias windowAlias = new Alias(window, window.toSql());
        List<NamedExpression> outputExpressions = Lists.newArrayList(windowAlias);
        Plan root = new LogicalWindow<>(outputExpressions, rStudent);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new ExtractAndNormalizeWindowExpression())
                .applyTopDown(new CheckAndStandardizeWindowFunctionAndFrame())
                .matches(
                        logicalWindow()
                                .when(logicalWindow -> {
                                    WindowExpression newWindow = (WindowExpression) logicalWindow.getWindowExpressions().get(0).child(0);
                                    return newWindow.getWindowFrame().get().equals(requiredFrame);
                                })
                );
    }

    @Test
    public void testLead() {
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newFollowingBoundary(new IntegerLiteral(5)));
        WindowExpression window = new WindowExpression(new Lead(age, new IntegerLiteral(5), new IntegerLiteral(0)),
                partitionKeyList, orderKeyList);
        Alias windowAlias = new Alias(window, window.toSql());
        List<NamedExpression> outputExpressions = Lists.newArrayList(windowAlias);
        Plan root = new LogicalWindow<>(outputExpressions, rStudent);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new ExtractAndNormalizeWindowExpression())
                .applyTopDown(new CheckAndStandardizeWindowFunctionAndFrame())
                .matches(
                        logicalWindow()
                        .when(logicalWindow -> {
                            WindowExpression newWindow = (WindowExpression) logicalWindow.getWindowExpressions().get(0).child(0);
                            return newWindow.getWindowFrame().get().equals(requiredFrame);
                        })
                );
    }

    @Test
    public void testLag() {
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newPrecedingBoundary(new IntegerLiteral(5)));
        WindowExpression window = new WindowExpression(new Lag(age, new IntegerLiteral(5), new IntegerLiteral(0)),
                partitionKeyList, orderKeyList);
        Alias windowAlias = new Alias(window, window.toSql());
        List<NamedExpression> outputExpressions = Lists.newArrayList(windowAlias);
        Plan root = new LogicalWindow<>(outputExpressions, rStudent);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new ExtractAndNormalizeWindowExpression())
                .applyTopDown(new CheckAndStandardizeWindowFunctionAndFrame())
                .matches(
                        logicalWindow()
                        .when(logicalWindow -> {
                            WindowExpression newWindow = (WindowExpression) logicalWindow.getWindowExpressions().get(0).child(0);
                            return newWindow.getWindowFrame().get().equals(requiredFrame);
                        })
                );
    }

    @Test
    public void testCheckWindowFrameBeforeFunc0() {
        WindowExpression window = new WindowExpression(new Rank(), partitionKeyList, Lists.newArrayList(), defaultWindowFrame);
        String errorMsg = "WindowFrame clause requires OrderBy clause";

        forCheckWindowFrameBeforeFunc(window, errorMsg);
    }

    @Test
    public void testCheckWindowFrameBeforeFunc1() {
        WindowFrame windowFrame1 = new WindowFrame(FrameUnitsType.ROWS, FrameBoundary.newFollowingBoundary());
        String errorMsg1 = "WindowFrame in any window function cannot use UNBOUNDED FOLLOWING as left boundary";
        forCheckWindowFrameBeforeFunc(windowFrame1, errorMsg1);

        WindowFrame windowFrame2 = new WindowFrame(FrameUnitsType.ROWS,
                WindowFrame.FrameBoundary.newFollowingBoundary(new IntegerLiteral(3)), FrameBoundary.newCurrentRowBoundary());
        String errorMsg2 = "WindowFrame with FOLLOWING left boundary requires UNBOUNDED FOLLOWING or FOLLOWING right boundary";
        forCheckWindowFrameBeforeFunc(windowFrame2, errorMsg2);
    }

    @Test
    public void testCheckWindowFrameBeforeFunc2() {
        WindowFrame windowFrame1 = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newCurrentRowBoundary(), FrameBoundary.newPrecedingBoundary());
        String errorMsg1 = "WindowFrame in any window function cannot use UNBOUNDED PRECEDING as right boundary";
        forCheckWindowFrameBeforeFunc(windowFrame1, errorMsg1);

        WindowFrame windowFrame2 = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newCurrentRowBoundary(), FrameBoundary.newPrecedingBoundary(new IntegerLiteral(3)));
        String errorMsg2 = "WindowFrame with PRECEDING right boundary requires UNBOUNDED PRECEDING or PRECEDING left boundary";
        forCheckWindowFrameBeforeFunc(windowFrame2, errorMsg2);
    }

    @Test
    public void testCheckWindowFrameBeforeFunc3() {
        WindowFrame windowFrame1 = new WindowFrame(FrameUnitsType.RANGE,
                FrameBoundary.newPrecedingBoundary(new IntegerLiteral(3)), FrameBoundary.newCurrentRowBoundary());
        WindowFrame windowFrame2 = new WindowFrame(FrameUnitsType.RANGE,
                FrameBoundary.newCurrentRowBoundary(), FrameBoundary.newFollowingBoundary(new IntegerLiteral(3)));
        WindowFrame windowFrame3 = new WindowFrame(FrameUnitsType.RANGE,
                FrameBoundary.newCurrentRowBoundary(), FrameBoundary.newCurrentRowBoundary());
        String errorMsg = "WindowFrame with RANGE must use both UNBOUNDED boundary or one UNBOUNDED boundary and one CURRENT ROW";

        forCheckWindowFrameBeforeFunc(windowFrame1, errorMsg);
        forCheckWindowFrameBeforeFunc(windowFrame2, errorMsg);
        forCheckWindowFrameBeforeFunc(windowFrame3, errorMsg);
    }

    @Test
    public void testCheckWindowFrameBeforeFunc4() {
        WindowFrame windowFrame1 = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(new IntegerLiteral(-3)), FrameBoundary.newCurrentRowBoundary());
        String errorMsg1 = "BoundOffset of WindowFrame must be positive";
        forCheckWindowFrameBeforeFunc(windowFrame1, errorMsg1);

        WindowFrame windowFrame2 = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(new DoubleLiteral(3.5)), FrameBoundary.newCurrentRowBoundary());
        String errorMsg2 = "BoundOffset of ROWS WindowFrame must be an Integer";
        forCheckWindowFrameBeforeFunc(windowFrame2, errorMsg2);
    }

    @Test
    public void testCheckWindowFrameBeforeFunc5() {
        WindowFrame windowFrame1 = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(new IntegerLiteral(3)), FrameBoundary.newPrecedingBoundary(new IntegerLiteral(4)));
        String errorMsg1 = "WindowFrame with PRECEDING boundary requires that leftBoundOffset >= rightBoundOffset";
        forCheckWindowFrameBeforeFunc(windowFrame1, errorMsg1);

        WindowFrame windowFrame2 = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newFollowingBoundary(new IntegerLiteral(5)), FrameBoundary.newFollowingBoundary(new IntegerLiteral(4)));
        String errorMsg2 = "WindowFrame with FOLLOWING boundary requires that leftBoundOffset >= rightBoundOffset";
        forCheckWindowFrameBeforeFunc(windowFrame2, errorMsg2);
    }

    @Test
    public void testFirstValueRewrite() {
        age = rStudent.getOutput().get(3).toSlot();
        WindowExpression window = new WindowExpression(new FirstValue(age, BooleanLiteral.FALSE), partitionKeyList, orderKeyList);
        Alias windowAlias = new Alias(window, window.toSql());
        WindowExpression windowLastValue = new WindowExpression(new LastValue(age, BooleanLiteral.FALSE), partitionKeyList, orderKeyList);
        Alias windowLastValueAlias = new Alias(windowLastValue, windowLastValue.toSql());
        List<NamedExpression> outputExpressions = Lists.newArrayList(windowAlias, windowLastValueAlias);
        Plan root = new LogicalWindow<>(outputExpressions, rStudent);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new ExtractAndNormalizeWindowExpression())
                .applyTopDown(new CheckAndStandardizeWindowFunctionAndFrame())
                .matches(
                        logicalWindow()
                                .when(logicalWindow -> {
                                    WindowExpression newWindowFirstValue = (WindowExpression) logicalWindow.getWindowExpressions().get(0).child(0);
                                    WindowExpression newWindowLastValue = (WindowExpression) logicalWindow.getWindowExpressions().get(0).child(0);
                                    return newWindowFirstValue.getFunction().arity() == 1 && newWindowLastValue.getFunction().arity() == 1;
                                })
                );
    }

    private void forCheckWindowFrameBeforeFunc(WindowFrame windowFrame, String errorMsg) {
        WindowExpression window = new WindowExpression(new Rank(), partitionKeyList, orderKeyList, windowFrame);
        forCheckWindowFrameBeforeFunc(window, errorMsg);
    }

    private void forCheckWindowFrameBeforeFunc(WindowExpression window, String errorMsg) {
        Alias windowAlias = new Alias(window, window.toSql());
        List<NamedExpression> outputExpressions = Lists.newArrayList(windowAlias);
        Plan root = new LogicalWindow<>(outputExpressions, rStudent);

        Exception exception = Assertions.assertThrows(Exception.class, () -> {
            PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                    .applyTopDown(new ExtractAndNormalizeWindowExpression())
                    .applyTopDown(new CheckAndStandardizeWindowFunctionAndFrame());
        }, "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains(errorMsg));
    }
}

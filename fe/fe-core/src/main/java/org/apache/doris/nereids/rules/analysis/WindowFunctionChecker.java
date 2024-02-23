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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundType;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.functions.window.CumeDist;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.FirstOrLastValue;
import org.apache.doris.nereids.trees.expressions.functions.window.FirstValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lag;
import org.apache.doris.nereids.trees.expressions.functions.window.LastValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lead;
import org.apache.doris.nereids.trees.expressions.functions.window.Ntile;
import org.apache.doris.nereids.trees.expressions.functions.window.PercentRank;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Check and standardize Window expression:
 *
 * step 1: checkWindowBeforeFunc():
 *  general checking for WindowFrame, including check OrderKeyList, set default right boundary, check offset if exists,
 *  check correctness of boundaryType
 * step 2: checkWindowFunction():
 *  check window function, and different function has different checking rules .
 *  If window frame not exits, set a unique default window frame according to their function type.
 * step 3: checkWindowAfterFunc():
 *  reverse window if necessary (just for first_value() and last_value()), and add a general default
 *  window frame (RANGE between UNBOUNDED PRECEDING and CURRENT ROW)
 */
public class WindowFunctionChecker extends DefaultExpressionVisitor<Expression, Void> {

    private WindowExpression windowExpression;

    public WindowFunctionChecker(WindowExpression window) {
        this.windowExpression = window;
    }

    public WindowExpression getWindow() {
        return windowExpression;
    }

    /**
     * step 1: check windowFrame in window;
     */
    public void checkWindowBeforeFunc() {
        windowExpression.getWindowFrame().ifPresent(this::checkWindowFrameBeforeFunc);
    }

    /**
     * step 2: check windowFunction in window
     */
    public Expression checkWindowFunction() {
        // todo: visitNtile()

        // in checkWindowFrameBeforeFunc() we have confirmed that both left and right boundary are set as long as
        // windowFrame exists, therefore in all following visitXXX functions we don't need to check whether the right
        // boundary is null.
        return windowExpression.accept(this, null);
    }

    /**
     * step 3: check window
     */
    public void checkWindowAfterFunc() {
        Optional<WindowFrame> windowFrame = windowExpression.getWindowFrame();
        if (windowFrame.isPresent()) {
            // reverse windowFrame
            checkWindowFrameAfterFunc(windowFrame.get());
        } else {
            setDefaultWindowFrameAfterFunc();
        }
    }

    /* ********************************************************************************************
     * methods for step 1
     * ******************************************************************************************** */

    /**
     *
     * if WindowFrame doesn't have right boundary, we will set it a default one(current row);
     * but if WindowFrame itself doesn't exist, we will keep it null still.
     *
     * Basic exception cases:
     * 0. WindowFrame != null, but OrderKeyList == null
     *
     * WindowFrame EXCEPTION cases:
     * 1. (unbounded following, xxx) || (offset following, !following)
     * 2. (xxx, unbounded preceding) || (!preceding, offset preceding)
     * 3. RANGE && ( (offset preceding, xxx) || (xxx, offset following) || (current row, current row) )
     *
     * WindowFrame boundOffset check:
     * 4. check value of boundOffset: Literal; Positive; Integer (for ROWS) or Numeric (for RANGE)
     * 5. check that boundOffset of left <= boundOffset of right
     */
    private void checkWindowFrameBeforeFunc(WindowFrame windowFrame) {
        // case 0
        if (windowExpression.getOrderKeys().isEmpty()) {
            throw new AnalysisException("WindowFrame clause requires OrderBy clause");
        }

        // set default rightBoundary
        if (windowFrame.getRightBoundary().isNull()) {
            windowFrame = windowFrame.withRightBoundary(FrameBoundary.newCurrentRowBoundary());
        }
        FrameBoundary left = windowFrame.getLeftBoundary();
        FrameBoundary right = windowFrame.getRightBoundary();

        // case 1
        if (left.getFrameBoundType() == FrameBoundType.UNBOUNDED_FOLLOWING) {
            throw new AnalysisException("WindowFrame in any window function cannot use "
                + "UNBOUNDED FOLLOWING as left boundary");
        }
        if (left.getFrameBoundType() == FrameBoundType.FOLLOWING && !right.asFollowing()) {
            throw new AnalysisException("WindowFrame with FOLLOWING left boundary requires "
                + "UNBOUNDED FOLLOWING or FOLLOWING right boundary");
        }

        // case 2
        if (right.getFrameBoundType() == FrameBoundType.UNBOUNDED_PRECEDING) {
            throw new AnalysisException("WindowFrame in any window function cannot use "
                + "UNBOUNDED PRECEDING as right boundary");
        }
        if (right.getFrameBoundType() == FrameBoundType.PRECEDING && !left.asPreceding()) {
            throw new AnalysisException("WindowFrame with PRECEDING right boundary requires "
                + "UNBOUNDED PRECEDING or PRECEDING left boundary");
        }

        // case 3
        // this case will be removed when RANGE with offset boundaries is supported
        if (windowFrame.getFrameUnits() == FrameUnitsType.RANGE) {
            if (left.hasOffset() || right.hasOffset()
                    || (left.getFrameBoundType() == FrameBoundType.CURRENT_ROW
                    && right.getFrameBoundType() == FrameBoundType.CURRENT_ROW)) {
                throw new AnalysisException("WindowFrame with RANGE must use both UNBOUNDED boundary or "
                    + "one UNBOUNDED boundary and one CURRENT ROW");
            }
        }

        // case 4
        if (left.hasOffset()) {
            checkFrameBoundOffset(left);
        }
        if (right.hasOffset()) {
            checkFrameBoundOffset(right);
        }

        // case 5
        // check correctness of left boundary and right boundary
        if (left.hasOffset() && right.hasOffset()) {
            double leftOffsetValue = ((Literal) left.getBoundOffset().get()).getDouble();
            double rightOffsetValue = ((Literal) right.getBoundOffset().get()).getDouble();
            if (left.asPreceding() && right.asPreceding()) {
                Preconditions.checkArgument(leftOffsetValue >= rightOffsetValue, "WindowFrame with "
                        + "PRECEDING boundary requires that leftBoundOffset >= rightBoundOffset");
            } else if (left.asFollowing() && right.asFollowing()) {
                Preconditions.checkArgument(leftOffsetValue <= rightOffsetValue, "WindowFrame with "
                        + "FOLLOWING boundary requires that leftBoundOffset >= rightBoundOffset");
            }
        }

        windowExpression = windowExpression.withWindowFrame(windowFrame);
    }

    /**
     * check boundOffset of FrameBoundary if it exists:
     * 1 boundOffset should be Literal, but this restriction can be removed after completing FoldConstant
     * 2 boundOffset should be positive
     * 2 boundOffset should be a positive INTEGER if FrameUnitsType == ROWS
     * 3 boundOffset should be a positive INTEGER or DECIMAL if FrameUnitsType == RANGE
     */
    private void checkFrameBoundOffset(FrameBoundary frameBoundary) {
        Expression offset = frameBoundary.getBoundOffset().get();

        // case 1
        Preconditions.checkArgument(offset.isLiteral(), "BoundOffset of WindowFrame must be Literal");

        // case 2
        boolean isPositive = ((Literal) offset).getDouble() > 0;
        Preconditions.checkArgument(isPositive, "BoundOffset of WindowFrame must be positive");

        // case 3
        FrameUnitsType frameUnits = windowExpression.getWindowFrame().get().getFrameUnits();
        if (frameUnits == FrameUnitsType.ROWS) {
            Preconditions.checkArgument(offset.getDataType().isIntegralType(),
                    "BoundOffset of ROWS WindowFrame must be an Integer");
        }

        // case 4
        if (frameUnits == FrameUnitsType.RANGE) {
            Preconditions.checkArgument(offset.getDataType().isNumericType(),
                    "BoundOffset of RANGE WindowFrame must be an Integer or Decimal");
        }
    }

    /* ********************************************************************************************
     * methods for step 2
     * ******************************************************************************************** */

    /**
     * required WindowFrame: (UNBOUNDED PRECEDING, offset PRECEDING)
     * but in Spark, it is (offset PRECEDING, offset PRECEDING)
     */
    @Override
    public Lag visitLag(Lag lag, Void ctx) {
        // check and complete window frame
        windowExpression.getWindowFrame().ifPresent(wf -> {
            throw new AnalysisException("WindowFrame for LAG() must be null");
        });
        if (lag.children().size() != 3) {
            throw new AnalysisException("Lag must have three parameters");
        }

        Expression column = lag.child(0);
        Expression offset = lag.getOffset();
        Expression defaultValue = lag.getDefaultValue();
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newPrecedingBoundary(offset));
        windowExpression = windowExpression.withWindowFrame(requiredFrame);

        // check if the class of lag's column matches defaultValue, and cast it
        if (!TypeCoercionUtils.implicitCast(column.getDataType(), defaultValue.getDataType()).isPresent()) {
            throw new AnalysisException("DefaultValue's Datatype of LAG() cannot match its relevant column. The column "
                + "type is " + column.getDataType() + ", but the defaultValue type is " + defaultValue.getDataType());
        }
        return lag.withChildren(ImmutableList.of(column, offset,
                TypeCoercionUtils.castIfNotMatchType(defaultValue, column.getDataType())));
    }

    /**
     * required WindowFrame: (UNBOUNDED PRECEDING, offset FOLLOWING)
     * but in Spark, it is (offset FOLLOWING, offset FOLLOWING)
     */
    @Override
    public Lead visitLead(Lead lead, Void ctx) {
        windowExpression.getWindowFrame().ifPresent(wf -> {
            throw new AnalysisException("WindowFrame for LEAD() must be null");
        });
        if (lead.children().size() != 3) {
            throw new AnalysisException("Lead must have three parameters");
        }

        Expression column = lead.child(0);
        Expression offset = lead.getOffset();
        Expression defaultValue = lead.getDefaultValue();
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newFollowingBoundary(offset));
        windowExpression = windowExpression.withWindowFrame(requiredFrame);

        // check if the class of lag's column matches defaultValue, and cast it
        if (!TypeCoercionUtils.implicitCast(column.getDataType(), defaultValue.getDataType()).isPresent()) {
            throw new AnalysisException("DefaultValue's Datatype of LEAD() can't match its relevant column. The column "
                + "type is " + column.getDataType() + ", but the defaultValue type is " + defaultValue.getDataType());
        }
        return lead.withChildren(ImmutableList.of(column, offset,
            TypeCoercionUtils.castIfNotMatchType(defaultValue, column.getDataType())));
    }

    /**
     * [Copied from class AnalyticExpr.standardize()]:
     *
     *    FIRST_VALUE without UNBOUNDED PRECEDING gets rewritten to use a different window
     *    and change the function to return the last value. We either set the fn to be
     *    'last_value' or 'first_value_rewrite', which simply wraps the 'last_value'
     *    implementation but allows us to handle the first rows in a partition in a special
     *    way in the backend. There are a few cases:
     *     a) Start bound is X FOLLOWING or CURRENT ROW (X=0):
     *        Use 'last_value' with a window where both bounds are X FOLLOWING (or
     *        CURRENT ROW). Setting the start bound to X following is necessary because the
     *        X rows at the end of a partition have no rows in their window. Note that X
     *        FOLLOWING could be rewritten as lead(X) but that would not work for CURRENT
     *        ROW.
     *     b) Start bound is X PRECEDING and end bound is CURRENT ROW or FOLLOWING:
     *        Use 'first_value_rewrite' and a window with an end bound X PRECEDING. An
     *        extra parameter '-1' is added to indicate to the backend that NULLs should
     *        not be added for the first X rows.
     *     c) Start bound is X PRECEDING and end bound is Y PRECEDING:
     *        Use 'first_value_rewrite' and a window with an end bound X PRECEDING. The
     *        first Y rows in a partition have empty windows and should be NULL. An extra
     *        parameter with the integer constant Y is added to indicate to the backend
     *        that NULLs should be added for the first Y rows.
     */
    @Override
    public FirstOrLastValue visitFirstValue(FirstValue firstValue, Void ctx) {
        Optional<WindowFrame> windowFrame = windowExpression.getWindowFrame();
        if (windowFrame.isPresent()) {
            WindowFrame wf = windowFrame.get();
            if (wf.getLeftBoundary().isNot(FrameBoundType.UNBOUNDED_PRECEDING)
                    && wf.getLeftBoundary().isNot(FrameBoundType.PRECEDING)) {
                windowExpression = windowExpression.withWindowFrame(
                        wf.withFrameUnits(FrameUnitsType.ROWS).withRightBoundary(wf.getLeftBoundary()));
                LastValue lastValue = new LastValue(firstValue.children());
                windowExpression = windowExpression.withFunction(lastValue);
                return lastValue;
            }

            if (wf.getLeftBoundary().is(FrameBoundType.UNBOUNDED_PRECEDING)
                    && wf.getRightBoundary().isNot(FrameBoundType.PRECEDING)) {
                windowExpression = windowExpression.withWindowFrame(
                        wf.withRightBoundary(FrameBoundary.newCurrentRowBoundary()));
            }
        } else {
            windowExpression = windowExpression.withWindowFrame(new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary()));
        }
        return firstValue;
    }

    /**
     * required WindowFrame: (RANGE, UNBOUNDED PRECEDING, CURRENT ROW)
     */
    @Override
    public Rank visitRank(Rank rank, Void ctx) {
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.RANGE,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());

        checkAndCompleteWindowFrame(requiredFrame, rank.getName());
        return rank;
    }

    /**
     * required WindowFrame: (RANGE, UNBOUNDED PRECEDING, CURRENT ROW)
     */
    @Override
    public DenseRank visitDenseRank(DenseRank denseRank, Void ctx) {
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.RANGE,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());

        checkAndCompleteWindowFrame(requiredFrame, denseRank.getName());
        return denseRank;
    }

    /**
     * required WindowFrame: (RANGE, UNBOUNDED PRECEDING, CURRENT ROW)
     */
    @Override
    public PercentRank visitPercentRank(PercentRank percentRank, Void ctx) {
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.RANGE,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());

        checkAndCompleteWindowFrame(requiredFrame, percentRank.getName());
        return percentRank;
    }

    /**
     * required WindowFrame: (ROWS, UNBOUNDED PRECEDING, CURRENT ROW)
     */
    @Override
    public RowNumber visitRowNumber(RowNumber rowNumber, Void ctx) {
        // check and complete window frame
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());

        checkAndCompleteWindowFrame(requiredFrame, rowNumber.getName());
        return rowNumber;
    }

    /**
     * required WindowFrame: (RANGE, UNBOUNDED PRECEDING, CURRENT ROW)
     */
    @Override
    public CumeDist visitCumeDist(CumeDist cumeDist, Void ctx) {
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.RANGE,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());

        checkAndCompleteWindowFrame(requiredFrame, cumeDist.getName());
        return cumeDist;
    }

    /**
     * required WindowFrame: (ROWS, UNBOUNDED PRECEDING, CURRENT ROW)
     */
    @Override
    public Ntile visitNtile(Ntile ntile, Void ctx) {
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());

        checkAndCompleteWindowFrame(requiredFrame, ntile.getName());
        return ntile;
    }

    /**
     * check if the current WindowFrame equals with the required WindowFrame; if current WindowFrame is null,
     * the requiredFrame should be used as default frame.
     */
    private void checkAndCompleteWindowFrame(WindowFrame requiredFrame, String functionName) {
        windowExpression.getWindowFrame().ifPresent(wf -> {
            if (!wf.equals(requiredFrame)) {
                throw new AnalysisException("WindowFrame for " + functionName + "() must be null "
                    + "or match with " + requiredFrame);
            }
        });
        windowExpression = windowExpression.withWindowFrame(requiredFrame);
    }

    /* ********************************************************************************************
     * methods for step 3
     * ******************************************************************************************** */

    private void checkWindowFrameAfterFunc(WindowFrame wf) {
        if (wf.getRightBoundary().is(FrameBoundType.UNBOUNDED_FOLLOWING)
                && wf.getLeftBoundary().isNot(FrameBoundType.UNBOUNDED_PRECEDING)) {
            // reverse OrderKey's asc and isNullFirst;
            // in checkWindowFrameBeforeFunc(), we have confirmed that orderKeyLists must exist
            List<OrderExpression> newOKList = windowExpression.getOrderKeys().stream()
                    .map(orderExpression -> {
                        OrderKey orderKey = orderExpression.getOrderKey();
                        return new OrderExpression(
                                new OrderKey(orderKey.getExpr(), !orderKey.isAsc(), !orderKey.isNullFirst()));
                    })
                    .collect(Collectors.toList());
            windowExpression = windowExpression.withOrderKeys(newOKList);

            // reverse WindowFrame
            // e.g. (3 preceding, unbounded following) -> (unbounded preceding, 3 following)
            windowExpression = windowExpression.withWindowFrame(wf.reverseWindow());

            // reverse WindowFunction, which is used only for first_value() and last_value()
            Expression windowFunction = windowExpression.getFunction();
            if (windowFunction instanceof FirstOrLastValue) {
                // windowExpression = windowExpression.withChildren(
                //         ImmutableList.of(((FirstOrLastValue) windowFunction).reverse()));
                windowExpression = windowExpression.withFunction(((FirstOrLastValue) windowFunction).reverse());
            }
        }
    }

    private void setDefaultWindowFrameAfterFunc() {
        // this is equal to DEFAULT_WINDOW in class AnalyticWindow
        windowExpression = windowExpression.withWindowFrame(new WindowFrame(FrameUnitsType.RANGE,
            FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary()));
    }
}

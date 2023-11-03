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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AnalyticExpr.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.analysis.AnalyticWindow.Boundary;
import org.apache.doris.analysis.AnalyticWindow.BoundaryType;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.TreeNode;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.TExprNode;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Representation of an analytic function call with OVER clause.
 * All "subexpressions" (such as the actual function call parameters as well as the
 * partition/ordering exprs, etc.) are embedded as children in order to allow expr
 * substitution:
 *   function call params: child 0 .. #params
 *   partition exprs: children #params + 1 .. #params + #partition-exprs
 *   ordering exprs:
 *     children #params + #partition-exprs + 1 ..
 *       #params + #partition-exprs + #order-by-elements
 *   exprs in windowing clause: remaining children
 *
 * Note that it's wrong to embed the FunctionCallExpr itself as a child,
 * because in 'COUNT(..) OVER (..)' the 'COUNT(..)' is not part of a standard aggregate
 * computation and must not be substituted as such. However, the parameters of the
 * analytic function call might reference the output of an aggregate computation
 * and need to be substituted as such; example: COUNT(COUNT(..)) OVER (..)
 */
public class AnalyticExpr extends Expr {
    private static final Logger LOG = LoggerFactory.getLogger(AnalyticExpr.class);

    private FunctionCallExpr fnCall;
    private final List<Expr> partitionExprs;
    // These elements are modified to point to the corresponding child exprs to keep them
    // in sync through expr substitutions.
    private List<OrderByElement> orderByElements = Lists.newArrayList();
    private AnalyticWindow window;

    // If set, requires the window to be set to null in resetAnalysisState(). Required for
    // proper substitution/cloning because standardization may set a window that is illegal
    // in SQL, and hence, will fail analysis().
    private boolean resetWindow = false;

    // SQL string of this AnalyticExpr before standardization. Returned in toSqlImpl().
    private String sqlString;

    private static String NTILE = "NTILE";
    private static String LEAD = "LEAD";
    private static String LAG = "LAG";
    private static String FIRSTVALUE = "FIRST_VALUE";
    private static String LASTVALUE = "LAST_VALUE";
    private static String RANK = "RANK";
    private static String DENSERANK = "DENSE_RANK";
    private static String ROWNUMBER = "ROW_NUMBER";
    private static String MIN = "MIN";
    private static String MAX = "MAX";
    private static String SUM = "SUM";
    private static String COUNT = "COUNT";

    // Internal function used to implement FIRST_VALUE with a window equal and
    // additional null handling in the backend.
    public static String FIRST_VALUE_REWRITE = "FIRST_VALUE_REWRITE";

    public AnalyticExpr(FunctionCallExpr fnCall, List<Expr> partitionExprs,
                        List<OrderByElement> orderByElements, AnalyticWindow window) {
        Preconditions.checkNotNull(fnCall);
        this.fnCall = fnCall;
        this.partitionExprs = partitionExprs != null ? partitionExprs : new ArrayList<Expr>();

        if (orderByElements != null) {
            this.orderByElements.addAll(orderByElements);
        }

        this.window = window;
        setChildren();
    }

    /**
     * clone() c'tor
     */
    protected AnalyticExpr(AnalyticExpr other) {
        super(other);
        fnCall = (FunctionCallExpr) other.fnCall.clone();

        for (OrderByElement e : other.orderByElements) {
            orderByElements.add(e.clone());
        }

        partitionExprs = Expr.cloneList(other.partitionExprs);
        window = (other.window != null ? other.window.clone() : null);
        resetWindow = other.resetWindow;
        sqlString = other.sqlString;
        setChildren();
    }

    public FunctionCallExpr getFnCall() {
        return fnCall;
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public AnalyticWindow getWindow() {
        return window;
    }

    @Override
    protected String getExprName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of(Utils.normalizeName(getFnCall().getExprName(), DEFAULT_EXPR_NAME));
        }
        return this.exprName.get();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fnCall, orderByElements, window);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }

        AnalyticExpr o = (AnalyticExpr) obj;

        if (!fnCall.equals(o.getFnCall())) {
            return false;
        }

        if ((window == null) != (o.window == null)) {
            return false;
        }

        if (window != null) {
            if (!window.equals(o.window)) {
                return false;
            }
        }

        return orderByElements.equals(o.orderByElements);
    }

    /**
     * Analytic exprs cannot be constant.
     */
    @Override
    protected boolean isConstantImpl() {
        return false;
    }

    @Override
    public Expr clone() {
        return new AnalyticExpr(this);
    }

    @Override
    public String debugString() {
        return MoreObjects.toStringHelper(this)
               .add("fn", getFnCall())
               .add("window", window)
               .addValue(super.debugString())
               .toString();
    }

    @Override
    protected void toThrift(TExprNode msg) {
    }

    public static boolean isAnalyticFn(Function fn) {
        return fn instanceof AggregateFunction
                && ((AggregateFunction) fn).isAnalyticFn();
    }

    public static boolean isAggregateFn(Function fn) {
        if (fn.functionName().equalsIgnoreCase(SUM) || fn.functionName().equalsIgnoreCase(MIN)
                || fn.functionName().equalsIgnoreCase(MAX) || fn.functionName().equalsIgnoreCase(COUNT)) {
            return true;
        }
        return false;
    }

    private static boolean isOffsetFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(LEAD) || fn.functionName().equalsIgnoreCase(LAG);
    }

    private static boolean isMinMax(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(MIN) || fn.functionName().equalsIgnoreCase(MAX);
    }

    private static boolean isRankingFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(RANK)
               || fn.functionName().equalsIgnoreCase(DENSERANK)
               || fn.functionName().equalsIgnoreCase(ROWNUMBER)
               || fn.functionName().equalsIgnoreCase(NTILE);
    }

    private static boolean isHllAggFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(FunctionSet.HLL_UNION_AGG);
    }

    private static boolean isNTileFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(NTILE);
    }

    /**
     * Rewrite the following analytic functions: ntile().
     * Returns a new Expr if the analytic expr is rewritten, returns null if it's not one
     * that we want to equal.
     */
    public static Expr rewrite(AnalyticExpr analyticExpr) {
        return null;
    }

    /**
     * Rewrite ntile().
     * The logic is translated from be class WindowFunctionNTile.
     * count = bigBucketNum * (smallBucketSize + 1) + smallBucketNum * smallBucketSize
     * bigBucketNum + smallBucketNum = bucketNum
     */
    private static Expr createNTile(AnalyticExpr analyticExpr) {
        Preconditions.checkState(AnalyticExpr.isNTileFn(analyticExpr.getFnCall().getFn()));
        Expr bucketNum = analyticExpr.getChild(0);
        AnalyticExpr rowNum = create("row_number", analyticExpr, true, false);
        AnalyticExpr count = create("count", analyticExpr, false, false);

        IntLiteral one = new IntLiteral(1);
        ArithmeticExpr smallBucketSize = new ArithmeticExpr(ArithmeticExpr.Operator.INT_DIVIDE,
                count, bucketNum);
        ArithmeticExpr bigBucketNum = new ArithmeticExpr(ArithmeticExpr.Operator.MOD,
                count, bucketNum);
        ArithmeticExpr firstSmallBucketRowIndex = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY,
                bigBucketNum,
                new ArithmeticExpr(ArithmeticExpr.Operator.ADD,
                        smallBucketSize, one));
        ArithmeticExpr rowIndex = new ArithmeticExpr(ArithmeticExpr.Operator.SUBTRACT,
                rowNum, one);


        List<Expr> ifParams = new ArrayList<>();
        ifParams.add(
            new BinaryPredicate(BinaryPredicate.Operator.GE, rowIndex, firstSmallBucketRowIndex));

        ArithmeticExpr rowInSmallBucket = new ArithmeticExpr(ArithmeticExpr.Operator.ADD,
                new ArithmeticExpr(ArithmeticExpr.Operator.ADD,
                        bigBucketNum, one),
                new ArithmeticExpr(ArithmeticExpr.Operator.INT_DIVIDE,
                        new ArithmeticExpr(ArithmeticExpr.Operator.SUBTRACT,
                                rowIndex, firstSmallBucketRowIndex),
                        smallBucketSize));
        ArithmeticExpr rowInBigBucket = new ArithmeticExpr(ArithmeticExpr.Operator.ADD,
                new ArithmeticExpr(ArithmeticExpr.Operator.INT_DIVIDE,
                        rowIndex,
                        new ArithmeticExpr(ArithmeticExpr.Operator.ADD,
                                smallBucketSize, one)),
                one);
        ifParams.add(rowInSmallBucket);
        ifParams.add(rowInBigBucket);

        return new FunctionCallExpr("if", ifParams);
    }

    /**
     * Create a new Analytic Expr and associate it with a new function.
     * Takes a reference analytic expression and clones the partition expressions and the
     * order by expressions if 'copyOrderBy' is set and optionally reverses it if
     * 'reverseOrderBy' is set. The new function that it will be associated with is
     * specified by fnName.
     */
    private static AnalyticExpr create(String fnName,
                                       AnalyticExpr referenceExpr, boolean copyOrderBy, boolean reverseOrderBy) {
        FunctionCallExpr fnExpr = new FunctionCallExpr(fnName, new ArrayList<>());
        fnExpr.setIsAnalyticFnCall(true);
        List<OrderByElement> orderByElements = null;
        if (copyOrderBy) {
            if (reverseOrderBy) {
                orderByElements = OrderByElement.reverse(referenceExpr.getOrderByElements());
            } else {
                orderByElements = new ArrayList<>();
                for (OrderByElement elem : referenceExpr.getOrderByElements()) {
                    orderByElements.add(elem.clone());
                }
            }
        }
        AnalyticExpr analyticExpr = new AnalyticExpr(fnExpr,
                Expr.cloneList(referenceExpr.getPartitionExprs()), orderByElements, null);
        return analyticExpr;
    }

    /**
     * Checks that the value expr of an offset boundary of a RANGE window is compatible
     * with orderingExprs (and that there's only a single ordering expr).
     */
    private void checkRangeOffsetBoundaryExpr(AnalyticWindow.Boundary boundary) throws AnalysisException {
        Preconditions.checkState(boundary.getType().isOffset());

        if (orderByElements.size() > 1) {
            throw new AnalysisException("Only one ORDER BY expression allowed if used with "
                                        + "a RANGE window with PRECEDING/FOLLOWING: " + toSql());
        }

        Expr rangeExpr = boundary.getExpr();

        if (!Type.isImplicitlyCastable(
                    rangeExpr.getType(), orderByElements.get(0).getExpr().getType(), false)) {
            throw new AnalysisException(
                "The value expression of a PRECEDING/FOLLOWING clause of a RANGE window must "
                + "be implicitly convertible to the ORDER BY expression's type: "
                + rangeExpr.toSql() + " cannot be implicitly converted to "
                + orderByElements.get(0).getExpr().toSql());
        }
    }

    /**
     * check the value out of range in lag/lead() function
     */
    void checkDefaultValue(Analyzer analyzer) throws AnalysisException {
        Expr val = getFnCall().getChild(2);

        if (!(val instanceof LiteralExpr)) {
            return;
        }

        if (!getFnCall().getChild(0).getType().getPrimitiveType().isNumericType()) {
            return;
        }

        double value = val.getConstFromExpr(val);
        PrimitiveType type = getFnCall().getChild(0).getType().getPrimitiveType();
        boolean out = false;

        if (type == PrimitiveType.TINYINT) {
            if (value > Byte.MAX_VALUE) {
                out = true;
            }
        } else if (type == PrimitiveType.SMALLINT) {
            if (value > Short.MAX_VALUE) {
                out = true;
            }
        } else if (type == PrimitiveType.INT) {
            if (value > Integer.MAX_VALUE) {
                out = true;
            }
        } else if (type == PrimitiveType.BIGINT) {
            if (value > Long.MAX_VALUE) {
                out = true;
            }
        } else if (type == PrimitiveType.FLOAT) {
            if (value > Float.MAX_VALUE) {
                out = true;
            }
        } else if (type == PrimitiveType.DOUBLE) {
            if (value > Double.MAX_VALUE) {
                out = true;
            }
        } else {
            return;
        }

        if (out) {
            throw new AnalysisException("Column type="
                    + getFnCall().getChildren().get(0).getType() + ", value is out of range ");
        }
    }

    /**
     * Checks offset of lag()/lead().
     */
    void checkOffset(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(isOffsetFn(getFnCall().getFn()));
        Preconditions.checkState(getFnCall().getChildren().size() > 1);
        Expr offset = getFnCall().getChild(1);

        try {
            Preconditions.checkState(offset.getType().isFixedPointType());
        } catch (Exception e) {
            throw new AnalysisException(
                "The offset parameter of LEAD/LAG must be a constant positive integer: "
                + getFnCall().toSql());
        }

        boolean isPosConstant = true;

        if (!offset.isConstant()) {
            isPosConstant = false;
        } else {
            double value = 0;

            if (offset instanceof IntLiteral) {
                IntLiteral intl = (IntLiteral) offset;
                value = intl.getDoubleValue();
            } else if (offset instanceof LargeIntLiteral) {
                LargeIntLiteral intl = (LargeIntLiteral) offset;
                value = intl.getDoubleValue();
            }

            if (value <= 0) {
                isPosConstant = false;
            }
        }

        if (!isPosConstant) {
            throw new AnalysisException(
                "The offset parameter of LEAD/LAG must be a constant positive integer: "
                + getFnCall().toSql());
        }
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        fnCall.analyze(analyzer);
        type = getFnCall().getType();

        for (Expr e : partitionExprs) {
            if (e.isLiteral()) {
                throw new AnalysisException(
                    "Expressions in the PARTITION BY clause must not be constant: "
                    + e.toSql() + " (in " + toSql() + ")");
            }
        }

        for (OrderByElement e : orderByElements) {
            if (e.getExpr().isLiteral()) {
                throw new AnalysisException(
                    "Expressions in the ORDER BY clause must not be constant: "
                            + e.getExpr().toSql() + " (in " + toSql() + ")");
            }
        }

        if (getFnCall().getParams().isDistinct()) {
            throw new AnalysisException(
                "DISTINCT not allowed in analytic function: " + getFnCall().toSql());
        }

        // check for correct composition of analytic expr
        Function fn = getFnCall().getFn();

        if (!(fn instanceof AggregateFunction)) {
            throw new AnalysisException(
                "OVER clause requires aggregate or analytic function: "
                + getFnCall().toSql());
        }

        // check for non-analytic aggregate functions
        if (!isAnalyticFn(fn)) {
            throw new AnalysisException(
                String.format("Aggregate function '%s' not supported with OVER clause.",
                              getFnCall().toSql()));
        }

        if (isAnalyticFn(fn) && !isAggregateFn(fn)) {
            // if (orderByElements.isEmpty()) {
            //    throw new AnalysisException(
            //        "'" + getFnCall().toSql() + "' requires an ORDER BY clause");
            // }

            if ((isRankingFn(fn) || isOffsetFn(fn) || isHllAggFn(fn)) && window != null) {
                throw new AnalysisException(
                    "Windowing clause not allowed with '" + getFnCall().toSql() + "'");
            }

            if (isOffsetFn(fn) && getFnCall().getChildren().size() > 1) {
                checkOffset(analyzer);

                // check the default, which needs to be a constant at the moment
                // TODO: remove this check when the backend can handle non-constants
                if (getFnCall().getChildren().size() > 2) {
                    if (!getFnCall().getChild(2).isConstant()) {
                        throw new AnalysisException(
                            "The default parameter (parameter 3) of LEAD/LAG must be a constant: "
                            + getFnCall().toSql());
                    }
                }
            }
        }

        if (window != null) {

            if (orderByElements.isEmpty()) {
                throw new AnalysisException("Windowing clause requires ORDER BY clause: "
                                            + toSql());
            }

            window.analyze(analyzer);

            if (!orderByElements.isEmpty()
                    && window.getType() == AnalyticWindow.Type.RANGE) {
                // check that preceding/following ranges match ordering
                if (window.getLeftBoundary().getType().isOffset()) {
                    checkRangeOffsetBoundaryExpr(window.getLeftBoundary());
                }

                if (window.getRightBoundary() != null
                        && window.getRightBoundary().getType().isOffset()) {
                    checkRangeOffsetBoundaryExpr(window.getRightBoundary());
                }
            }
        }

        // check nesting
        if (TreeNode.contains(getChildren(), AnalyticExpr.class)) {
            throw new AnalysisException(
                "Nesting of analytic expressions is not allowed: " + toSql());
        }

        sqlString = toSql();

        standardize(analyzer);

        setChildren();
    }

    /**
     * If necessary, rewrites the analytic function, window, and/or order-by elements into
     * a standard format for the purpose of simpler backend execution, as follows:
     * 1. row_number():
     *    Set a window from UNBOUNDED PRECEDING to CURRENT_ROW.
     * 2. lead()/lag():
     *    Explicitly set the default arguments to for BE simplicity.
     *    Set a window for lead(): UNBOUNDED PRECEDING to OFFSET FOLLOWING.
     *    Set a window for lag(): UNBOUNDED PRECEDING to OFFSET PRECEDING.
     * 3. UNBOUNDED FOLLOWING windows:
     *    Reverse the ordering and window if the start bound is not UNBOUNDED PRECEDING.
     *    Flip first_value() and last_value().
     * 4. first_value():
     *    Set the upper boundary to CURRENT_ROW if the lower boundary is
     *    UNBOUNDED_PRECEDING.
     * 5. Explicitly set the default window if no window was given but there
     *    are order-by elements.
     * 6. FIRST_VALUE without UNBOUNDED PRECEDING gets rewritten to use a different window
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
    private void standardize(Analyzer analyzer) throws AnalysisException {
        FunctionName analyticFnName = getFnCall().getFnName();

        // Set a window from UNBOUNDED PRECEDING to CURRENT_ROW for row_number().
        if (analyticFnName.getFunction().equalsIgnoreCase(ROWNUMBER)) {
            Preconditions.checkState(window == null, "Unexpected window set for row_number()");
            window = new AnalyticWindow(AnalyticWindow.Type.ROWS,
                                        new Boundary(BoundaryType.UNBOUNDED_PRECEDING, null),
                                        new Boundary(BoundaryType.CURRENT_ROW, null));
            resetWindow = true;
            return;
        }

        if (analyticFnName.getFunction().equalsIgnoreCase(NTILE)) {
            Preconditions.checkState(window == null, "Unexpected window set for ntile()");

            Expr bucketExpr = getFnCall().getFnParams().exprs().get(0);
            if (bucketExpr instanceof LiteralExpr && bucketExpr.getType().getPrimitiveType().isIntegerType()) {
                Preconditions.checkState(((LiteralExpr) bucketExpr).getLongValue() > 0,
                        "Parameter n in ntile(n) should be positive.");
            } else {
                throw new AnalysisException("Parameter n in ntile(n) should be constant positive integer.");
            }

            window = new AnalyticWindow(AnalyticWindow.Type.ROWS,
                    new Boundary(BoundaryType.UNBOUNDED_PRECEDING, null),
                    new Boundary(BoundaryType.CURRENT_ROW, null));
            resetWindow = true;
            return;
        }

        // Explicitly set the default arguments to lead()/lag() for BE simplicity.
        // Set a window for lead(): UNBOUNDED PRECEDING to OFFSET FOLLOWING,
        // Set a window for lag(): UNBOUNDED PRECEDING to OFFSET PRECEDING.
        if (isOffsetFn(getFnCall().getFn())) {
            Preconditions.checkState(window == null);

            // If necessary, create a new fn call with the default args explicitly set.
            List<Expr> newExprParams = null;

            if (getFnCall().getChildren().size() == 1) {
                newExprParams = Lists.newArrayListWithExpectedSize(3);
                newExprParams.addAll(getFnCall().getChildren());
                // Default offset is 1.
                // newExprParams.add(new DecimalLiteral(BigDecimal.valueOf(1)));
                newExprParams.add(new IntLiteral("1", Type.BIGINT));
                // Default default value is NULL.
                newExprParams.add(new NullLiteral());
                throw new AnalysisException("Lag/offset must have three parameters");
            } else if (getFnCall().getChildren().size() == 2) {
                newExprParams = Lists.newArrayListWithExpectedSize(3);
                newExprParams.addAll(getFnCall().getChildren());
                // Default default value is NULL.
                newExprParams.add(new NullLiteral());
                throw new AnalysisException("Lag/offset must have three parameters");
            } else  {
                Preconditions.checkState(getFnCall().getChildren().size() == 3);
            }

            try {
                if (!Type.matchExactType(getFnCall().getChildren().get(0).getType(),
                        getFnCall().getChildren().get(2).getType())) {
                    getFnCall().uncheckedCastChild(getFnCall().getChildren().get(0).getType(), 2);
                }
            }  catch (Exception e) {
                LOG.warn("", e);
                throw new AnalysisException("Convert type error in offset fn(default value); old_type="
                                            + getFnCall().getChildren().get(2).getType() + " new_type="
                                            + getFnCall().getChildren().get(0).getType());
            }

            // if (getFnCall().getChildren().get(2) instanceof CastExpr) {
            //     throw new AnalysisException("Type = " + type + " can't not convert to "
            //                                 + getFnCall().getChildren().get(0).getType());
            // }

            // check the value whether out of range
            checkDefaultValue(analyzer);

            try {
                getFnCall().uncheckedCastChild(Type.BIGINT, 1);
            }  catch (Exception e) {
                LOG.warn("", e);
                throw new AnalysisException("Convert type error in offset fn(default offset); type="
                                            + getFnCall().getChildren().get(1).getType());
            }

            // if (newExprParams != null) {
            // fnCall = new FunctionCallExpr(getFnCall(),
            //         new FunctionParams(newExprParams));
            // fnCall.setIsAnalyticFnCall(true);
            // fnCall.analyzeNoThrow(analyzer);
            // }

            // Set the window.
            BoundaryType rightBoundaryType = BoundaryType.FOLLOWING;

            if (analyticFnName.getFunction().equalsIgnoreCase(LAG)) {
                rightBoundaryType = BoundaryType.PRECEDING;
            }

            window = new AnalyticWindow(AnalyticWindow.Type.ROWS,
                                        new Boundary(BoundaryType.UNBOUNDED_PRECEDING, null),
                                        new Boundary(rightBoundaryType, getOffsetExpr(getFnCall())));

            try {
                window.analyze(analyzer);
            } catch (AnalysisException e) {
                throw new IllegalStateException(e);
            }

            resetWindow = true;
            return;
        }

        if (analyticFnName.getFunction().equalsIgnoreCase(FIRSTVALUE)
                && window != null
                && window.getLeftBoundary().getType() != BoundaryType.UNBOUNDED_PRECEDING) {
            if (window.getLeftBoundary().getType() != BoundaryType.PRECEDING) {
                // Here left bound can only be CURRENT_ROW and the function is last_value,
                // so the type is changed to rows
                window = new AnalyticWindow(AnalyticWindow.Type.ROWS, window.getLeftBoundary(),
                                            window.getLeftBoundary());
                fnCall = new FunctionCallExpr(new FunctionName(LASTVALUE),
                                              getFnCall().getParams());
            }

            fnCall.setIsAnalyticFnCall(true);
            fnCall.analyzeNoThrow(analyzer);
            analyticFnName = getFnCall().getFnName();
        }

        // Reverse the ordering and window for windows ending with UNBOUNDED FOLLOWING,
        // and not starting with UNBOUNDED PRECEDING.
        if (window != null
                && window.getRightBoundary().getType() == BoundaryType.UNBOUNDED_FOLLOWING
                && window.getLeftBoundary().getType() != BoundaryType.UNBOUNDED_PRECEDING) {
            orderByElements = OrderByElement.reverse(orderByElements);
            window = window.reverse();

            // Also flip first_value()/last_value(). For other analytic functions there is no
            // need to also change the function.
            FunctionName reversedFnName = null;

            if (analyticFnName.getFunction().equalsIgnoreCase(FIRSTVALUE)) {
                reversedFnName = new FunctionName(LASTVALUE);
            } else if (analyticFnName.getFunction().equalsIgnoreCase(LASTVALUE)) {
                reversedFnName = new FunctionName(FIRSTVALUE);
            }

            if (reversedFnName != null) {
                fnCall = new FunctionCallExpr(reversedFnName, getFnCall().getParams());
                fnCall.setIsAnalyticFnCall(true);
                fnCall.analyzeNoThrow(analyzer);
            }

            analyticFnName = getFnCall().getFnName();
        }

        // Set the upper boundary to CURRENT_ROW for first_value() if the lower boundary
        // is UNBOUNDED_PRECEDING.
        if (window != null
                && window.getLeftBoundary().getType() == BoundaryType.UNBOUNDED_PRECEDING
                && window.getRightBoundary().getType() != BoundaryType.PRECEDING
                && analyticFnName.getFunction().equalsIgnoreCase(FIRSTVALUE)) {
            window.setRightBoundary(new Boundary(BoundaryType.CURRENT_ROW, null));
        }

        // Set the default window.
        if (!orderByElements.isEmpty() && window == null) {
            window = AnalyticWindow.DEFAULT_WINDOW;
            resetWindow = true;
        }

        // Change first_value RANGE windows to ROWS
        if ((analyticFnName.getFunction().equalsIgnoreCase(FIRSTVALUE))
                && window != null
                && window.getType() == AnalyticWindow.Type.RANGE) {
            window = new AnalyticWindow(AnalyticWindow.Type.ROWS, window.getLeftBoundary(),
                        window.getRightBoundary());
        }
    }

    /**
     * Returns the explicit or implicit offset of an analytic function call.
     */
    private Expr getOffsetExpr(FunctionCallExpr offsetFnCall) {
        Preconditions.checkState(isOffsetFn(getFnCall().getFn()));

        if (offsetFnCall.getChild(1) != null) {
            return offsetFnCall.getChild(1);
        }

        // The default offset is 1.
        return new DecimalLiteral(BigDecimal.valueOf(1));
    }

    /**
     * Keep fnCall_, partitionExprs_ and orderByElements_ in sync with children_.
     */
    private void syncWithChildren() {
        int numArgs = fnCall.getChildren().size();

        for (int i = 0; i < numArgs; ++i) {
            fnCall.setChild(i, getChild(i));
        }

        int numPartitionExprs = partitionExprs.size();

        for (int i = 0; i < numPartitionExprs; ++i) {
            partitionExprs.set(i, getChild(numArgs + i));
        }

        for (int i = 0; i < orderByElements.size(); ++i) {
            orderByElements.get(i).setExpr(getChild(numArgs + numPartitionExprs + i));
        }
    }

    /**
     * Populate children_ from fnCall_, partitionExprs_, orderByElements_
     */
    private void setChildren() {
        getChildren().clear();
        addChildren(fnCall.getChildren());
        addChildren(partitionExprs);

        for (OrderByElement e : orderByElements) {
            addChild(e.getExpr());
        }

        if (window != null) {
            if (window.getLeftBoundary().getExpr() != null) {
                addChild(window.getLeftBoundary().getExpr());
            }

            if (window.getRightBoundary() != null
                    && window.getRightBoundary().getExpr() != null) {
                addChild(window.getRightBoundary().getExpr());
            }
        }
    }

    @Override
    protected void resetAnalysisState() {
        super.resetAnalysisState();
        fnCall.resetAnalysisState();

        if (resetWindow) {
            window = null;
        }

        resetWindow = false;
        // sync with children, now that they've been reset
        syncWithChildren();
    }

    @Override
    protected Expr substituteImpl(ExprSubstitutionMap sMap, ExprSubstitutionMap disjunctsMap, Analyzer analyzer) {
        Expr e = super.substituteImpl(sMap, disjunctsMap, analyzer);
        if (!(e instanceof AnalyticExpr)) {
            return e;
        }
        // Re-sync state after possible child substitution.
        ((AnalyticExpr) e).syncWithChildren();
        return e;
    }

    @Override
    public String toSqlImpl() {
        if (sqlString != null) {
            return sqlString;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(fnCall.toSql()).append(" OVER (");
        boolean needsSpace = false;
        if (!partitionExprs.isEmpty()) {
            sb.append("PARTITION BY ").append(exprListToSql(partitionExprs));
            needsSpace = true;
        }
        if (!orderByElements.isEmpty()) {
            List<String> orderByStrings = Lists.newArrayList();
            for (OrderByElement e : orderByElements) {
                orderByStrings.add(e.toSql());
            }
            if (needsSpace) {
                sb.append(" ");
            }
            sb.append("ORDER BY ").append(Joiner.on(", ").join(orderByStrings));
            needsSpace = true;
        }
        if (window != null) {
            if (needsSpace) {
                sb.append(" ");
            }
            sb.append(window.toSql());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String toDigestImpl() {
        StringBuilder sb = new StringBuilder();
        sb.append(fnCall.toDigest()).append(" OVER (");
        boolean needsSpace = false;
        if (!partitionExprs.isEmpty()) {
            sb.append("PARTITION BY ").append(exprListToDigest(partitionExprs));
            needsSpace = true;
        }
        if (!orderByElements.isEmpty()) {
            List<String> orderByStrings = Lists.newArrayList();
            for (OrderByElement e : orderByElements) {
                orderByStrings.add(e.toDigest());
            }
            if (needsSpace) {
                sb.append(" ");
            }
            sb.append("ORDER BY ").append(Joiner.on(", ").join(orderByStrings));
            needsSpace = true;
        }
        if (window != null) {
            if (needsSpace) {
                sb.append(" ");
            }
            sb.append(window.toDigest());
        }
        sb.append(")");
        return sb.toString();
    }

    private String exprListToSql(List<? extends Expr> exprs) {
        if (exprs == null || exprs.isEmpty()) {
            return "";
        }
        List<String> strings = Lists.newArrayList();
        for (Expr expr : exprs) {
            strings.add(expr.toSql());
        }
        return Joiner.on(", ").join(strings);
    }

    private String exprListToDigest(List<? extends Expr> exprs) {
        if (exprs == null || exprs.isEmpty()) {
            return "";
        }
        List<String> strings = Lists.newArrayList();
        for (Expr expr : exprs) {
            strings.add(expr.toDigest());
        }
        return Joiner.on(", ").join(strings);
    }
}

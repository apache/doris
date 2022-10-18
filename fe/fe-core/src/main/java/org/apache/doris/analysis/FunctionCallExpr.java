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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/FunctionCallExpr.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.AliasFunction;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// TODO: for aggregations, we need to unify the code paths for builtins and UDAs.
public class FunctionCallExpr extends Expr {
    private static final ImmutableSet<String> STDDEV_FUNCTION_SET =
            new ImmutableSortedSet.Builder(String.CASE_INSENSITIVE_ORDER)
                    .add("stddev").add("stddev_val").add("stddev_samp").add("stddev_pop")
                    .add("variance").add("variance_pop").add("variance_pop").add("var_samp").add("var_pop").build();
    private static final ImmutableSet<String> DECIMAL_SAME_TYPE_SET =
            new ImmutableSortedSet.Builder(String.CASE_INSENSITIVE_ORDER)
                    .add("min").add("max").add("lead").add("lag")
                    .add("first_value").add("last_value").add("abs")
                    .add("positive").add("negative").build();
    private static final ImmutableSet<String> DECIMAL_WIDER_TYPE_SET =
            new ImmutableSortedSet.Builder(String.CASE_INSENSITIVE_ORDER)
                    .add("sum").add("avg").add("multi_distinct_sum").build();
    private static final ImmutableSet<String> DECIMAL_FUNCTION_SET =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .addAll(DECIMAL_SAME_TYPE_SET)
                    .addAll(DECIMAL_WIDER_TYPE_SET)
                    .addAll(STDDEV_FUNCTION_SET).build();

    private static final ImmutableSet<String> TIME_FUNCTIONS_WITH_PRECISION =
            new ImmutableSortedSet.Builder(String.CASE_INSENSITIVE_ORDER)
                    .add("now").add("current_timestamp").add("localtime").add("localtimestamp").build();
    private static final int STDDEV_DECIMAL_SCALE = 9;
    private static final String ELEMENT_EXTRACT_FN_NAME = "%element_extract%";

    private static final Logger LOG = LogManager.getLogger(FunctionCallExpr.class);

    private FunctionName fnName;
    // private BuiltinAggregateFunction.Operator aggOp;
    private FunctionParams fnParams;

    private FunctionParams aggFnParams;

    private List<OrderByElement> orderByElements = Lists.newArrayList();

    // check analytic function
    private boolean isAnalyticFnCall = false;
    // check table function
    private boolean isTableFnCall = false;

    // Indicates whether this is a merge aggregation function that should use the merge
    // instead of the update symbol. This flag also affects the behavior of
    // resetAnalysisState() which is used during expr substitution.
    private boolean isMergeAggFn;

    // use to record the num of json_object parameters
    private int originChildSize;
    // Save the functionCallExpr in the original statement
    private Expr originStmtFnExpr;

    private boolean isRewrote = false;

    public void setAggFnParams(FunctionParams aggFnParams) {
        this.aggFnParams = aggFnParams;
    }

    public void setIsAnalyticFnCall(boolean v) {
        isAnalyticFnCall = v;
    }

    public void setTableFnCall(boolean tableFnCall) {
        isTableFnCall = tableFnCall;
    }

    public Function getFn() {
        return fn;
    }

    public FunctionName getFnName() {
        return fnName;
    }

    public FunctionParams getFnParams() {
        return fnParams;
    }

    // only used restore from readFields.
    private FunctionCallExpr() {
        super();
    }

    public FunctionCallExpr(String functionName, List<Expr> params) {
        this(new FunctionName(functionName), new FunctionParams(false, params));
    }

    public FunctionCallExpr(FunctionName fnName, List<Expr> params) {
        this(fnName, new FunctionParams(false, params));
    }

    public FunctionCallExpr(FunctionName fnName, List<Expr> params, List<OrderByElement> orderByElements)
            throws AnalysisException {
        this(fnName, new FunctionParams(false, params), orderByElements);
    }

    public FunctionCallExpr(String fnName, FunctionParams params) {
        this(new FunctionName(fnName), params, false);
    }

    public FunctionCallExpr(FunctionName fnName, FunctionParams params) {
        this(fnName, params, false);
    }

    public FunctionCallExpr(
            FunctionName fnName, FunctionParams params, List<OrderByElement> orderByElements) throws AnalysisException {
        this(fnName, params, false);
        this.orderByElements = orderByElements;
        if (!orderByElements.isEmpty()) {
            if (!VectorizedUtil.isVectorized()) {
                throw new AnalysisException(
                        "ORDER BY for arguments only support in vec exec engine");
            } else if (!AggregateFunction.SUPPORT_ORDER_BY_AGGREGATE_FUNCTION_NAME_SET.contains(
                    fnName.getFunction().toLowerCase())) {
                throw new AnalysisException(
                        "ORDER BY not support for the function:" + fnName.getFunction().toLowerCase());
            }
        }
        setChildren();
    }

    private FunctionCallExpr(
            FunctionName fnName, FunctionParams params, boolean isMergeAggFn) {
        super();
        this.fnName = fnName;
        fnParams = params;
        this.isMergeAggFn = isMergeAggFn;
        if (params.exprs() != null) {
            children.addAll(params.exprs());
        }
        originChildSize = children.size();
    }

    // Constructs the same agg function with new params.
    public FunctionCallExpr(FunctionCallExpr e, FunctionParams params) {
        Preconditions.checkState(e.isAnalyzed);
        Preconditions.checkState(e.isAggregateFunction() || e.isAnalyticFnCall);
        fnName = e.fnName;
        // aggOp = e.aggOp;
        isAnalyticFnCall = e.isAnalyticFnCall;
        fnParams = params;
        aggFnParams = e.aggFnParams;
        // Just inherit the function object from 'e'.
        fn = e.fn;
        this.isMergeAggFn = e.isMergeAggFn;
        if (params.exprs() != null) {
            children.addAll(params.exprs());
        }
    }

    protected FunctionCallExpr(FunctionCallExpr other) {
        super(other);
        fnName = other.fnName;
        orderByElements = other.orderByElements;
        isAnalyticFnCall = other.isAnalyticFnCall;
        // aggOp = other.aggOp;
        // fnParams = other.fnParams;
        // Clone the params in a way that keeps the children_ and the params.exprs()
        // in sync. The children have already been cloned in the super c'tor.
        fnParams = other.fnParams.clone(children);
        aggFnParams = other.aggFnParams;

        this.isMergeAggFn = other.isMergeAggFn;
        fn = other.fn;
        this.isTableFnCall = other.isTableFnCall;
    }

    public String parseJsonDataType(boolean useKeyCheck) throws AnalysisException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < children.size(); ++i) {
            Type type = getChild(i).getType();
            if (type.isNull()) { // Not to return NULL directly, so save string, but flag is '0'
                if (((i & 1) == 0) && useKeyCheck == true) {
                    throw new AnalysisException("json_object key can't be NULL: " + this.toSql());
                }
                children.set(i, new StringLiteral("NULL"));
                sb.append("0");
            } else if (type.isBoolean()) {
                sb.append("1");
            } else if (type.isFixedPointType()) {
                sb.append("2");
            } else if (type.isFloatingPointType() || type.isDecimalV2() || type.isDecimalV3()) {
                sb.append("3");
            } else if (type.isTime()) {
                sb.append("4");
            } else {
                sb.append("5");
            }
        }
        return sb.toString();
    }

    public boolean isMergeAggFn() {
        return isMergeAggFn;
    }

    @Override
    protected Expr substituteImpl(ExprSubstitutionMap smap, Analyzer analyzer)
            throws AnalysisException {
        if (aggFnParams != null && aggFnParams.exprs() != null) {
            ArrayList<Expr> newParams = new ArrayList<Expr>();
            for (Expr expr : aggFnParams.exprs()) {
                Expr substExpr = smap.get(expr);
                if (substExpr != null) {
                    newParams.add(substExpr.clone());
                } else {
                    newParams.add(expr);
                }
            }
            aggFnParams = aggFnParams
                    .clone(newParams);
        }
        return super.substituteImpl(smap, analyzer);
    }

    @Override
    public Expr clone() {
        return new FunctionCallExpr(this);
    }

    @Override
    public void resetAnalysisState() {
        isAnalyzed = false;
        // Resolving merge agg functions after substitution may fail e.g., if the
        // intermediate agg type is not the same as the output type. Preserve the original
        // fn_ such that analyze() hits the special-case code for merge agg fns that
        // handles this case.
        if (!isMergeAggFn) {
            fn = null;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        FunctionCallExpr o = (FunctionCallExpr) obj;
        return /*opcode == o.opcode && aggOp == o.aggOp &&*/ fnName.equals(o.fnName)
                && fnParams.isDistinct() == o.fnParams.isDistinct()
                && fnParams.isStar() == o.fnParams.isStar();
    }

    private String paramsToSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");

        if (fnParams.isStar()) {
            sb.append("*");
        }
        if (fnParams.isDistinct()) {
            sb.append("DISTINCT ");
        }
        int len = children.size();
        List<String> result = Lists.newArrayList();
        if (fnName.getFunction().equalsIgnoreCase("json_array")
                || fnName.getFunction().equalsIgnoreCase("json_object")) {
            len = len - 1;
        }
        if (fnName.getFunction().equalsIgnoreCase("aes_decrypt")
                || fnName.getFunction().equalsIgnoreCase("aes_encrypt")
                || fnName.getFunction().equalsIgnoreCase("sm4_decrypt")
                || fnName.getFunction().equalsIgnoreCase("sm4_encrypt")) {
            len = len - 1;
        }
        for (int i = 0; i < len; ++i) {
            if (i == 1 && (fnName.getFunction().equalsIgnoreCase("aes_decrypt")
                    || fnName.getFunction().equalsIgnoreCase("aes_encrypt")
                    || fnName.getFunction().equalsIgnoreCase("sm4_decrypt")
                    || fnName.getFunction().equalsIgnoreCase("sm4_encrypt"))) {
                result.add("\'***\'");
            } else if (orderByElements.size() > 0 && i == len - orderByElements.size()) {
                result.add("ORDER BY " + children.get(i).toSql());
            } else {
                result.add(children.get(i).toSql());
            }
        }
        sb.append(Joiner.on(", ").join(result)).append(")");
        return sb.toString();
    }

    @Override
    public String toSqlImpl() {
        Expr expr;
        if (originStmtFnExpr != null) {
            expr = originStmtFnExpr;
        } else {
            expr = this;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(((FunctionCallExpr) expr).fnName);
        sb.append(paramsToSql());
        if (fnName.getFunction().equalsIgnoreCase("json_quote")
                || fnName.getFunction().equalsIgnoreCase("json_array")
                || fnName.getFunction().equalsIgnoreCase("json_object")) {
            return forJSON(sb.toString());
        }
        return sb.toString();
    }

    private String paramsToDigest() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");

        if (fnParams.isStar()) {
            sb.append("*");
        }
        if (fnParams.isDistinct()) {
            sb.append("DISTINCT ");
        }
        int len = children.size();
        List<String> result = Lists.newArrayList();
        if (fnName.getFunction().equalsIgnoreCase("json_array")
                || fnName.getFunction().equalsIgnoreCase("json_object")) {
            len = len - 1;
        }
        if (fnName.getFunction().equalsIgnoreCase("aes_decrypt")
                || fnName.getFunction().equalsIgnoreCase("aes_encrypt")
                || fnName.getFunction().equalsIgnoreCase("sm4_decrypt")
                || fnName.getFunction().equalsIgnoreCase("sm4_encrypt")) {
            len = len - 1;
        }
        for (int i = 0; i < len; ++i) {
            if (i == 1 && (fnName.getFunction().equalsIgnoreCase("aes_decrypt")
                    || fnName.getFunction().equalsIgnoreCase("aes_encrypt")
                    || fnName.getFunction().equalsIgnoreCase("sm4_decrypt")
                    || fnName.getFunction().equalsIgnoreCase("sm4_encrypt"))) {
                result.add("\'***\'");
            } else {
                result.add(children.get(i).toDigest());
            }
        }
        sb.append(Joiner.on(", ").join(result)).append(")");
        return sb.toString();
    }

    @Override
    public String toDigestImpl() {
        Expr expr;
        if (originStmtFnExpr != null) {
            expr = originStmtFnExpr;
        } else {
            expr = this;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(((FunctionCallExpr) expr).fnName);
        sb.append(paramsToDigest());
        if (fnName.getFunction().equalsIgnoreCase("json_quote")
                || fnName.getFunction().equalsIgnoreCase("json_array")
                || fnName.getFunction().equalsIgnoreCase("json_object")) {
            return forJSON(sb.toString());
        }
        return sb.toString();
    }

    @Override
    public String debugString() {
        return MoreObjects.toStringHelper(this)/*.add("op", aggOp)*/.add("name", fnName).add("isStar",
                fnParams.isStar()).add("isDistinct", fnParams.isDistinct()).addValue(
                super.debugString()).toString();
    }

    public FunctionParams getParams() {
        return fnParams;
    }

    public boolean isScalarFunction() {
        Preconditions.checkState(fn != null);
        return fn instanceof ScalarFunction;
    }

    public boolean isAggregateFunction() {
        Preconditions.checkState(fn != null);
        return fn instanceof AggregateFunction && !isAnalyticFnCall;
    }

    public boolean isBuiltin() {
        Preconditions.checkState(fn != null);
        return fn instanceof BuiltinAggregateFunction && !isAnalyticFnCall;
    }

    /**
     * Returns true if this is a call to an aggregate function that returns
     * non-null on an empty input (e.g. count).
     */
    public boolean returnsNonNullOnEmpty() {
        Preconditions.checkNotNull(fn);
        return fn instanceof AggregateFunction
                && ((AggregateFunction) fn).returnsNonNullOnEmpty();
    }

    public boolean isDistinct() {
        Preconditions.checkState(isAggregateFunction());
        return fnParams.isDistinct();
    }

    public boolean isCountDistinctBitmapOrHLL() {
        if (!fnParams.isDistinct()) {
            return false;
        }

        if (!fnName.getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            return false;
        }

        if (children.size() != 1) {
            return false;
        }

        Type type = getChild(0).getType();
        return type.isBitmapType() || type.isHllType();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // TODO: we never serialize this to thrift if it's an aggregate function
        // except in test cases that do it explicitly.
        if (isAggregate() || isAnalyticFnCall) {
            msg.node_type = TExprNodeType.AGG_EXPR;
            if (aggFnParams == null) {
                aggFnParams = fnParams;
            }
            msg.setAggExpr(aggFnParams.createTAggregateExpr(isMergeAggFn));
        } else {
            msg.node_type = TExprNodeType.FUNCTION_CALL;
        }
    }

    private void analyzeBuiltinAggFunction(Analyzer analyzer) throws AnalysisException {
        if (fnParams.isStar() && !fnName.getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            throw new AnalysisException(
                    "'*' can only be used in conjunction with COUNT: " + this.toSql());
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            // for multiple exprs count must be qualified with distinct
            if (children.size() > 1 && !fnParams.isDistinct()) {
                throw new AnalysisException(
                        "COUNT must have DISTINCT for multiple arguments: " + this.toSql());
            }

            for (Expr child : children) {
                if (child.type.isOnlyMetricType()) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
            }
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase("json_array")) {
            String res = parseJsonDataType(false);
            if (children.size() == originChildSize) {
                children.add(new StringLiteral(res));
            }
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase("json_object")) {
            if ((children.size() & 1) == 1 && (originChildSize == children.size())) {
                throw new AnalysisException("json_object can't be odd parameters, need even parameters: "
                        + this.toSql());
            }
            String res = parseJsonDataType(true);
            if (children.size() == originChildSize) {
                children.add(new StringLiteral(res));
            }
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase("group_concat")) {
            if (children.size() - orderByElements.size() > 2 || children.isEmpty()) {
                throw new AnalysisException(
                        "group_concat requires one or two parameters: " + this.toSql());
            }

            Expr arg0 = getChild(0);
            if (!arg0.type.isStringType() && !arg0.type.isNull()) {
                throw new AnalysisException(
                        "group_concat requires first parameter to be of type STRING: " + this.toSql());
            }

            if (children.size() - orderByElements.size() == 2) {
                Expr arg1 = getChild(1);
                if (!arg1.type.isStringType() && !arg1.type.isNull()) {
                    throw new AnalysisException(
                            "group_concat requires second parameter to be of type STRING: " + this.toSql());
                }
            }

            return;
        }

        if (fnName.getFunction().equalsIgnoreCase("lag")
                || fnName.getFunction().equalsIgnoreCase("lead")) {
            if (!isAnalyticFnCall) {
                throw new AnalysisException(fnName.getFunction() + " only used in analytic function");
            } else {
                if (children.size() > 2) {
                    if (!getChild(1).isConstant() || !getChild(2).isConstant()) {
                        throw new AnalysisException(
                                "The default parameter (parameter 2 or parameter 3) of LEAD/LAG must be a constant: "
                                        + this.toSql());
                    }
                    uncheckedCastChild(Type.BIGINT, 1);
                    if (!getChild(2).type.matchesType(getChild(0).type) && !getChild(2).type.matchesType(Type.NULL)) {
                        uncheckedCastChild(getChild(0).type, 2);
                    }
                }
                return;
            }
        }

        if (fnName.getFunction().equalsIgnoreCase("dense_rank")
                || fnName.getFunction().equalsIgnoreCase("rank")
                || fnName.getFunction().equalsIgnoreCase("row_number")
                || fnName.getFunction().equalsIgnoreCase("first_value")
                || fnName.getFunction().equalsIgnoreCase("last_value")
                || fnName.getFunction().equalsIgnoreCase("first_value_rewrite")
                || fnName.getFunction().equalsIgnoreCase("ntile")) {
            if (!isAnalyticFnCall) {
                throw new AnalysisException(fnName.getFunction() + " only used in analytic function");
            }
        }

        // Function's arg can't be null for the following functions.
        Expr arg = getChild(0);
        if (arg == null) {
            return;
        }

        // SUM and AVG cannot be applied to non-numeric types
        if ((fnName.getFunction().equalsIgnoreCase("sum")
                || fnName.getFunction().equalsIgnoreCase("avg"))
                && ((!arg.type.isNumericType() && !arg.type.isNull()) || arg.type.isOnlyMetricType())) {
            throw new AnalysisException(fnName.getFunction() + " requires a numeric parameter: " + this.toSql());
        }
        if (fnName.getFunction().equalsIgnoreCase("sum_distinct")
                && ((!arg.type.isNumericType() && !arg.type.isNull()) || arg.type.isOnlyMetricType())) {
            throw new AnalysisException(
                    "SUM_DISTINCT requires a numeric parameter: " + this.toSql());
        }

        if ((fnName.getFunction().equalsIgnoreCase("min")
                || fnName.getFunction().equalsIgnoreCase("max")
                || fnName.getFunction().equalsIgnoreCase("DISTINCT_PC")
                || fnName.getFunction().equalsIgnoreCase("DISTINCT_PCSA")
                || fnName.getFunction().equalsIgnoreCase("NDV"))
                && arg.type.isOnlyMetricType()) {
            throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
        }

        if ((fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_UNION_INT) && !arg.type.isInteger32Type())) {
            throw new AnalysisException("BITMAP_UNION_INT params only support TINYINT or SMALLINT or INT");
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.INTERSECT_COUNT) || fnName.getFunction()
                .equalsIgnoreCase(FunctionSet.ORTHOGONAL_BITMAP_INTERSECT) || fnName.getFunction()
                .equalsIgnoreCase(FunctionSet.ORTHOGONAL_BITMAP_INTERSECT_COUNT)) {
            if (children.size() <= 2) {
                throw new AnalysisException(fnName + "(bitmap_column, column_to_filter, filter_values) "
                        + "function requires at least three parameters");
            }

            Type inputType = getChild(0).getType();
            if (!inputType.isBitmapType()) {
                throw new AnalysisException(
                        fnName + "function first argument should be of BITMAP type, but was " + inputType);
            }

            for (int i = 2; i < children.size(); i++) {
                if (!getChild(i).isConstant()) {
                    throw new AnalysisException(fnName + " function filter_values arg must be constant");
                }
            }
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_COUNT)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_UNION)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_UNION_COUNT)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_INTERSECT)) {
            if (children.size() != 1) {
                throw new AnalysisException(fnName + " function could only have one child");
            }
            Type inputType = getChild(0).getType();
            if (!inputType.isBitmapType()) {
                throw new AnalysisException(fnName
                        + " function's argument should be of BITMAP type, but was " + inputType);
            }
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.QUANTILE_UNION)) {
            if (children.size() != 1) {
                throw new AnalysisException(fnName + "function could only have one child");
            }
            Type inputType = getChild(0).getType();
            if (!inputType.isQuantileStateType()) {
                throw new AnalysisException(fnName
                        + " function's argument should be of QUANTILE_STATE type, but was" + inputType);
            }
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.TO_QUANTILE_STATE)) {
            if (children.size() != 2) {
                throw new AnalysisException(fnName + "function must have two children");
            }
            if (!getChild(1).isConstant()) {
                throw new AnalysisException(fnName + "function's second argument should be constant");
            }
        }

        if ((fnName.getFunction().equalsIgnoreCase("HLL_UNION_AGG")
                || fnName.getFunction().equalsIgnoreCase("HLL_CARDINALITY")
                || fnName.getFunction().equalsIgnoreCase("HLL_RAW_AGG"))
                && !arg.type.isHllType()) {
            throw new AnalysisException(
                    "HLL_UNION_AGG, HLL_RAW_AGG and HLL_CARDINALITY's params must be hll column");
        }

        if (fnName.getFunction().equalsIgnoreCase("min")
                || fnName.getFunction().equalsIgnoreCase("max")) {
            fnParams.setIsDistinct(false);  // DISTINCT is meaningless here
        } else if (fnName.getFunction().equalsIgnoreCase("DISTINCT_PC")
                || fnName.getFunction().equalsIgnoreCase("DISTINCT_PCSA")
                || fnName.getFunction().equalsIgnoreCase("NDV")
                || fnName.getFunction().equalsIgnoreCase("HLL_UNION_AGG")) {
            fnParams.setIsDistinct(false);
        }

        if (fnName.getFunction().equalsIgnoreCase("percentile")) {
            if (children.size() != 2) {
                throw new AnalysisException("percentile(expr, DOUBLE) requires two parameters");
            }
            if (!getChild(1).isConstant()) {
                throw new AnalysisException("percentile requires second parameter must be a constant : "
                        + this.toSql());
            }
        }

        if (fnName.getFunction().equalsIgnoreCase("percentile_approx")) {
            if (children.size() != 2 && children.size() != 3) {
                throw new AnalysisException("percentile_approx(expr, DOUBLE [, B]) requires two or three parameters");
            }
            if (!getChild(1).isConstant()) {
                throw new AnalysisException("percentile_approx requires second parameter must be a constant : "
                        + this.toSql());
            }
            if (children.size() == 3) {
                if (!getChild(2).isConstant()) {
                    throw new AnalysisException("percentile_approx requires the third parameter must be a constant : "
                            + this.toSql());
                }
            }
        }

        if (fnName.getFunction().equalsIgnoreCase("topn")) {
            if (children.size() != 2 && children.size() != 3) {
                throw new AnalysisException("topn(expr, INT [, B]) requires two or three parameters");
            }
            if (!getChild(1).isConstant() || !getChild(1).getType().isIntegerType()) {
                throw new AnalysisException("topn requires second parameter must be a constant Integer Type: "
                        + this.toSql());
            }
            if (!getChild(1).getType().equals(ScalarType.INT)) {
                Expr e = getChild(1).castTo(ScalarType.INT);
                setChild(1, e);
            }
            if (children.size() == 3) {
                if (!getChild(2).isConstant() || !getChild(2).getType().isIntegerType()) {
                    throw new AnalysisException("topn requires the third parameter must be a constant Integer Type: "
                            + this.toSql());
                }
                if (!getChild(2).getType().equals(ScalarType.INT)) {
                    Expr e = getChild(2).castTo(ScalarType.INT);
                    setChild(2, e);
                }
            }
        }
        if ((fnName.getFunction().equalsIgnoreCase("aes_decrypt")
                || fnName.getFunction().equalsIgnoreCase("aes_encrypt")
                || fnName.getFunction().equalsIgnoreCase("sm4_decrypt")
                || fnName.getFunction().equalsIgnoreCase("sm4_encrypt"))
                && children.size() == 3) {
            String blockEncryptionMode = "";
            Set<String> aesModes = new HashSet<>(Arrays.asList(
                    "AES_128_ECB",
                    "AES_192_ECB",
                    "AES_256_ECB",
                    "AES_128_CBC",
                    "AES_192_CBC",
                    "AES_256_CBC",
                    "AES_128_CFB",
                    "AES_192_CFB",
                    "AES_256_CFB",
                    "AES_128_CFB1",
                    "AES_192_CFB1",
                    "AES_256_CFB1",
                    "AES_128_CFB8",
                    "AES_192_CFB8",
                    "AES_256_CFB8",
                    "AES_128_CFB128",
                    "AES_192_CFB128",
                    "AES_256_CFB128",
                    "AES_128_CTR",
                    "AES_192_CTR",
                    "AES_256_CTR",
                    "AES_128_OFB",
                    "AES_192_OFB",
                    "AES_256_OFB"
            ));
            Set<String> sm4Modes = new HashSet<>(Arrays.asList(
                    "SM4_128_ECB",
                    "SM4_128_CBC",
                    "SM4_128_CFB128",
                    "SM4_128_OFB",
                    "SM4_128_CTR"));

            if (ConnectContext.get() != null) {
                blockEncryptionMode = ConnectContext.get().getSessionVariable().getBlockEncryptionMode();
                if (fnName.getFunction().equalsIgnoreCase("aes_decrypt")
                        || fnName.getFunction().equalsIgnoreCase("aes_encrypt")) {
                    if (StringUtils.isAllBlank(blockEncryptionMode)) {
                        blockEncryptionMode = "AES_128_ECB";
                    }
                    if (!aesModes.contains(blockEncryptionMode.toUpperCase())) {
                        throw new AnalysisException("session variable block_encryption_mode is invalid with aes");

                    }
                }
                if (fnName.getFunction().equalsIgnoreCase("sm4_decrypt")
                        || fnName.getFunction().equalsIgnoreCase("sm4_encrypt")) {
                    if (StringUtils.isAllBlank(blockEncryptionMode)) {
                        blockEncryptionMode = "SM4_128_ECB";
                    }
                    if (!sm4Modes.contains(blockEncryptionMode.toUpperCase())) {
                        throw new AnalysisException("session variable block_encryption_mode is invalid with sm4");

                    }
                }
            }
            children.add(new StringLiteral(blockEncryptionMode));
        }

    }

    // Provide better error message for some aggregate builtins. These can be
    // a bit more user friendly than a generic function not found.
    // TODO: should we bother to do this? We could also improve the general
    // error messages. For example, listing the alternatives.
    protected String getFunctionNotFoundError(Type[] argTypes) {
        // Some custom error message for builtins
        if (fnParams.isStar()) {
            return "'*' can only be used in conjunction with COUNT";
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            if (!fnParams.isDistinct() && argTypes.length > 1) {
                return "COUNT must have DISTINCT for multiple arguments: " + toSql();
            }
        }

        if (fnName.getFunction().equalsIgnoreCase("sum")) {
            return "SUM requires a numeric parameter: " + toSql();
        }

        if (fnName.getFunction().equalsIgnoreCase("avg")) {
            return "AVG requires a numeric or timestamp parameter: " + toSql();
        }

        String[] argTypesSql = new String[argTypes.length];
        for (int i = 0; i < argTypes.length; ++i) {
            argTypesSql[i] = argTypes[i].toSql();
        }

        return String.format(
                "No matching function with signature: %s(%s).",
                fnName, fnParams.isStar() ? "*" : Joiner.on(", ").join(argTypesSql));
    }

    /**
     * This analyzeImp used for DefaultValueExprDef
     * to generate a builtinFunction.
     *
     * @throws AnalysisException
     */
    public void analyzeImplForDefaultValue(Type type) throws AnalysisException {
        fn = getBuiltinFunction(fnName.getFunction(), new Type[0], Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        fn.setReturnType(type);
        this.type = type;
        for (int i = 0; i < children.size(); ++i) {
            if (getChild(i).getType().isNull()) {
                uncheckedCastChild(Type.BOOLEAN, i);
            }
        }
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        if (isMergeAggFn) {
            // This is the function call expr after splitting up to a merge aggregation.
            // The function has already been analyzed so just do the minimal sanity
            // check here.
            AggregateFunction aggFn = (AggregateFunction) fn;
            Preconditions.checkNotNull(aggFn);
            Type intermediateType = aggFn.getIntermediateType();
            if (intermediateType == null) {
                intermediateType = type;
            }
            // Preconditions.checkState(!type.isWildcardDecimal());
            return;
        }

        if (fnName.getFunction().equals(FunctionSet.COUNT) && fnParams.isDistinct()) {
            // Treat COUNT(DISTINCT ...) special because of how we do the equal.
            // There is no version of COUNT() that takes more than 1 argument but after
            // the equal, we only need count(*).
            // TODO: fix how we equal count distinct.
            fn = getBuiltinFunction(fnName.getFunction(), new Type[0], Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            type = fn.getReturnType();

            // Make sure BE doesn't see any TYPE_NULL exprs
            for (int i = 0; i < children.size(); ++i) {
                if (getChild(i).getType().isNull()) {
                    uncheckedCastChild(Type.BOOLEAN, i);
                }
            }
            return;
        }
        Type[] argTypes = new Type[this.children.size()];
        for (int i = 0; i < this.children.size(); ++i) {
            this.children.get(i).analyze(analyzer);
            argTypes[i] = this.children.get(i).getType();
        }

        analyzeBuiltinAggFunction(analyzer);

        if (fnName.getFunction().equalsIgnoreCase("sum")) {
            if (this.children.isEmpty()) {
                throw new AnalysisException("The " + fnName + " function must has one input param");
            }
            // Prevent the cast type in vector exec engine
            Type type = getChild(0).type;
            if (!VectorizedUtil.isVectorized()) {
                type = getChild(0).type.getMaxResolutionType();
            }
            fn = getBuiltinFunction(fnName.getFunction(), new Type[] {type},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if (fnName.getFunction().equalsIgnoreCase("count_distinct")) {
            Type compatibleType = this.children.get(0).getType();
            for (int i = 1; i < this.children.size(); ++i) {
                Type type = this.children.get(i).getType();
                compatibleType = Type.getAssignmentCompatibleType(compatibleType, type, true);
                if (compatibleType.isInvalid()) {
                    compatibleType = Type.VARCHAR;
                    break;
                }
            }

            fn = getBuiltinFunction(fnName.getFunction(), new Type[] {compatibleType},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if (fnName.getFunction().equalsIgnoreCase(FunctionSet.WINDOW_FUNNEL)) {
            if (fnParams.exprs() == null || fnParams.exprs().size() < 4) {
                throw new AnalysisException("The " + fnName + " function must have at least four params");
            }

            if (!children.get(0).type.isIntegerType()) {
                throw new AnalysisException("The window params of " + fnName + " function must be integer");
            }
            if (!children.get(1).type.isStringType()) {
                throw new AnalysisException("The mode params of " + fnName + " function must be integer");
            }
            if (!children.get(2).type.isDateType()) {
                throw new AnalysisException("The 3rd param of " + fnName + " function must be DATE or DATETIME");
            }

            Type[] childTypes = new Type[children.size()];
            for (int i = 0; i < 3; i++) {
                childTypes[i] = children.get(i).type;
            }
            for (int i = 3; i < children.size(); i++) {
                if (children.get(i).type != Type.BOOLEAN) {
                    throw new AnalysisException("The 4th and subsequent params of "
                            + fnName + " function must be boolean");
                }
                childTypes[i] = children.get(i).type;
            }
            fn = getBuiltinFunction(fnName.getFunction(), childTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (fn != null && fn.getArgs()[2].isDatetime() && childTypes[2].isDatetimeV2()) {
                fn.setArgType(childTypes[2], 2);
            } else if (fn != null && fn.getArgs()[2].isDatetime() && childTypes[2].isDateV2()) {
                fn.setArgType(ScalarType.DATETIMEV2, 2);
            }
            if (fn != null && childTypes[2].isDate()) {
                // cast date to datetime
                uncheckedCastChild(ScalarType.DATETIME, 2);
            } else if (fn != null && childTypes[2].isDateV2()) {
                // cast date to datetime
                uncheckedCastChild(ScalarType.DATETIMEV2, 2);
            }
        } else if (fnName.getFunction().equalsIgnoreCase(FunctionSet.RETENTION)) {
            if (this.children.isEmpty()) {
                throw new AnalysisException("The " + fnName + " function must have at least one param");
            }

            Type[] childTypes = new Type[children.size()];
            for (int i = 0; i < children.size(); i++) {
                if (children.get(i).type != Type.BOOLEAN) {
                    throw new AnalysisException("All params of "
                            + fnName + " function must be boolean");
                }
                childTypes[i] = children.get(i).type;
            }
            fn = getBuiltinFunction(fnName.getFunction(), childTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if (fnName.getFunction().equalsIgnoreCase("if")) {
            Type[] childTypes = collectChildReturnTypes();
            Type assignmentCompatibleType = ScalarType.getAssignmentCompatibleType(childTypes[1], childTypes[2], true);
            childTypes[1] = assignmentCompatibleType;
            childTypes[2] = assignmentCompatibleType;
            fn = getBuiltinFunction(fnName.getFunction(), childTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (assignmentCompatibleType.isDatetimeV2()) {
                fn.setReturnType(assignmentCompatibleType);
            }
        } else if (AggregateFunction.SUPPORT_ORDER_BY_AGGREGATE_FUNCTION_NAME_SET.contains(
                fnName.getFunction().toLowerCase())) {
            // order by elements add as child like windows function. so if we get the
            // param of arg, we need remove the order by elements
            Type[] childTypes = collectChildReturnTypes();
            Type[] newChildTypes = new Type[children.size() - orderByElements.size()];
            System.arraycopy(childTypes, 0, newChildTypes, 0, newChildTypes.length);
            fn = getBuiltinFunction(fnName.getFunction(), newChildTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else {
            // now first find table function in table function sets
            if (isTableFnCall) {
                Type[] childTypes = collectChildReturnTypes();
                fn = getTableFunction(fnName.getFunction(), childTypes,
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                if (fn == null) {
                    throw new AnalysisException(getFunctionNotFoundError(argTypes));
                }
            } else {
                // now first find function in built-in functions
                if (Strings.isNullOrEmpty(fnName.getDb())) {
                    Type[] childTypes = collectChildReturnTypes();
                    fn = getBuiltinFunction(fnName.getFunction(), childTypes,
                            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                }

                // find user defined functions
                if (fn == null) {
                    if (!analyzer.isUDFAllowed()) {
                        throw new AnalysisException(
                                "Does not support non-builtin functions, or function does not exist: "
                                        + this.toSqlImpl());
                    }

                    String dbName = fnName.analyzeDb(analyzer);
                    if (!Strings.isNullOrEmpty(dbName)) {
                        // check operation privilege
                        if (!Env.getCurrentEnv().getAuth()
                                .checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.SELECT)) {
                            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SELECT");
                        }
                        // TODO(gaoxin): ExternalDatabase not implement udf yet.
                        DatabaseIf db = Env.getCurrentEnv().getInternalCatalog().getDbNullable(dbName);
                        if (db != null && (db instanceof Database)) {
                            Function searchDesc =
                                    new Function(fnName, Arrays.asList(collectChildReturnTypes()), Type.INVALID, false);
                            fn = ((Database) db).getFunction(searchDesc,
                                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                        }
                    }
                }
            }
        }

        if (fn == null) {
            LOG.warn("fn {} not exists", this.toSqlImpl());
            throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
        }

        applyAutoTypeConversionForDatetimeV2();

        if (fnName.getFunction().equalsIgnoreCase("from_unixtime")
                || fnName.getFunction().equalsIgnoreCase("date_format")) {
            // if has only one child, it has default time format: yyyy-MM-dd HH:mm:ss.SSSSSS
            if (children.size() > 1) {
                final StringLiteral fmtLiteral = (StringLiteral) children.get(1);
                if (fmtLiteral.getStringValue().equals("yyyyMMdd")) {
                    children.set(1, new StringLiteral("%Y%m%d"));
                } else if (fmtLiteral.getStringValue().equals("yyyy-MM-dd")) {
                    children.set(1, new StringLiteral("%Y-%m-%d"));
                } else if (fmtLiteral.getStringValue().equals("yyyy-MM-dd HH:mm:ss")) {
                    children.set(1, new StringLiteral("%Y-%m-%d %H:%i:%s"));
                }
            }
        }

        if (fn.getFunctionName().getFunction().equals("timediff")) {
            fn.getReturnType().getPrimitiveType().setTimeType();
        }

        if (isAggregateFunction()) {
            final String functionName = fnName.getFunction();
            // subexprs must not contain aggregates
            if (Expr.containsAggregate(children)) {
                throw new AnalysisException(
                        "aggregate function cannot contain aggregate parameters: " + this.toSql());
            }

            if (STDDEV_FUNCTION_SET.contains(functionName) && argTypes[0].isDateType()) {
                throw new AnalysisException("Stddev/variance function do not support Date/Datetime type");
            }

            if (functionName.equalsIgnoreCase("multi_distinct_sum") && argTypes[0].isDateType()) {
                throw new AnalysisException("Sum in multi distinct functions do not support Date/Datetime type");
            }

        } else {
            if (fnParams.isStar()) {
                throw new AnalysisException("Cannot pass '*' to scalar function.");
            }
            if (fnParams.isDistinct()) {
                throw new AnalysisException("Cannot pass 'DISTINCT' to scalar function.");
            }
        }

        if (!fn.getFunctionName().getFunction().equals(ELEMENT_EXTRACT_FN_NAME)) {
            Type[] args = fn.getArgs();
            if (args.length > 0) {
                // Implicitly cast all the children to match the function if necessary
                for (int i = 0; i < argTypes.length - orderByElements.size(); ++i) {
                    // For varargs, we must compare with the last type in callArgs.argTypes.
                    int ix = Math.min(args.length - 1, i);
                    if (!argTypes[i].matchesType(args[ix]) && Config.enable_date_conversion
                            && !argTypes[i].isDateType() && (args[ix].isDate() || args[ix].isDatetime())) {
                        uncheckedCastChild(ScalarType.getDefaultDateType(args[ix]), i);
                    } else if (!argTypes[i].matchesType(args[ix]) && Config.enable_decimalv3
                            && Config.enable_decimal_conversion
                            && argTypes[i].isDecimalV3() && args[ix].isDecimalV2()) {
                        uncheckedCastChild(ScalarType.createDecimalV3Type(argTypes[i].getPrecision(),
                                ((ScalarType) argTypes[i]).getScalarScale()), i);
                    } else if (!argTypes[i].matchesType(args[ix]) && !(
                            argTypes[i].isDateType() && args[ix].isDateType())) {
                        uncheckedCastChild(args[ix], i);
                    }
                }
            }
        }

        /**
         * The return type of str_to_date depends on whether the time part is included in the format.
         * If included, it is datetime, otherwise it is date.
         * If the format parameter is not constant, the return type will be datetime.
         * The above judgment has been completed in the FE query planning stage,
         * so here we directly set the value type to the return type set in the query plan.
         *
         * For example:
         * A table with one column k1 varchar, and has 2 lines:
         *     "%Y-%m-%d"
         *     "%Y-%m-%d %H:%i:%s"
         * Query:
         *     SELECT str_to_date("2020-09-01", k1) from tbl;
         * Result will be:
         *     2020-09-01 00:00:00
         *     2020-09-01 00:00:00
         *
         * Query:
         *      SELECT str_to_date("2020-09-01", "%Y-%m-%d");
         * Return type is DATE
         *
         * Query:
         *      SELECT str_to_date("2020-09-01", "%Y-%m-%d %H:%i:%s");
         * Return type is DATETIME
         */
        if (fn.getFunctionName().getFunction().equals("str_to_date")) {
            Expr child1Result = getChild(1).getResultValue();
            if (child1Result instanceof StringLiteral) {
                if (DateLiteral.hasTimePart(((StringLiteral) child1Result).getStringValue())) {
                    this.type = ScalarType.getDefaultDateType(Type.DATETIME);
                } else {
                    this.type = ScalarType.getDefaultDateType(Type.DATE);
                }
            } else {
                this.type = ScalarType.getDefaultDateType(Type.DATETIME);
            }
        } else {
            this.type = fn.getReturnType();
        }

        Type[] childTypes = collectChildReturnTypes();
        if ((this.type.isDate() || this.type.isDatetime()) && Config.enable_date_conversion
                && fn.getArgs().length == childTypes.length) {
            boolean implicitCastToDate = false;
            for (int i = 0; i < fn.getArgs().length; i++) {
                implicitCastToDate = Type.canCastTo(childTypes[i], fn.getArgs()[i]);
                if (implicitCastToDate) {
                    break;
                }
            }
            if (implicitCastToDate) {
                this.type = ScalarType.getDefaultDateType(fn.getReturnType());
                fn.setReturnType(ScalarType.getDefaultDateType(fn.getReturnType()));
            }
        }

        if (this.type.isDecimalV2() && Config.enable_decimal_conversion && Config.enable_decimalv3
                && fn.getArgs().length == childTypes.length) {
            boolean implicitCastToDecimalV3 = false;
            for (int i = 0; i < fn.getArgs().length; i++) {
                implicitCastToDecimalV3 = Type.canCastTo(childTypes[i], fn.getArgs()[i]);
                if (implicitCastToDecimalV3) {
                    break;
                }
            }
            if (implicitCastToDecimalV3) {
                this.type = ScalarType.createDecimalV3Type(fn.getReturnType().getPrecision(),
                        ((ScalarType) fn.getReturnType()).getScalarScale());
                fn.setReturnType(this.type);
            }
        }

        if (this.type.isDecimalV3()) {
            // DECIMAL need to pass precision and scale to be
            if (DECIMAL_FUNCTION_SET.contains(fn.getFunctionName().getFunction())
                    && (this.type.isDecimalV2() || this.type.isDecimalV3())) {
                if (DECIMAL_SAME_TYPE_SET.contains(fnName.getFunction())) {
                    this.type = argTypes[0];
                    fn.setReturnType(this.type);
                } else if (DECIMAL_WIDER_TYPE_SET.contains(fnName.getFunction())) {
                    this.type = ScalarType.createDecimalV3Type(ScalarType.MAX_DECIMAL128_PRECISION,
                            ((ScalarType) argTypes[0]).getScalarScale());
                    fn.setReturnType(this.type);
                } else if (STDDEV_FUNCTION_SET.contains(fnName.getFunction())) {
                    // for all stddev function, use decimal(38,9) as computing result
                    this.type = ScalarType.createDecimalV3Type(ScalarType.MAX_DECIMAL128_PRECISION,
                            STDDEV_DECIMAL_SCALE);
                    fn.setReturnType(this.type);
                }
            }
        }
        // rewrite return type if is nested type function
        analyzeNestedFunction();
    }

    private void applyAutoTypeConversionForDatetimeV2() {
        // Rule1: Now we treat datetimev2 with different precisions as different types and we only register functions
        // for datetimev2(0). So we must apply an automatic type conversion from datetimev2(0) to the real type.
        if (fn.getArgs().length == children.size() && fn.getArgs().length > 0) {
            if (fn.getArgs()[0].isDatetimeV2() && children.get(0).getType().isDatetimeV2()) {
                fn.setArgType(children.get(0).getType(), 0);
                if (fn.getReturnType().isDatetimeV2()) {
                    fn.setReturnType(children.get(0).getType());
                }
            }
        }

        // Rule2: For functions in TIME_FUNCTIONS_WITH_PRECISION, we can't figure out which function should be use when
        // searching in FunctionSet. So we adjust the return type by hand here.
        if (TIME_FUNCTIONS_WITH_PRECISION.contains(fnName.getFunction().toLowerCase())
                && fn != null && fn.getReturnType().isDatetimeV2()) {
            if (children.size() == 1 && children.get(0) instanceof IntLiteral) {
                fn.setReturnType(ScalarType.createDatetimeV2Type((int) ((IntLiteral) children.get(0)).getLongValue()));
            } else if (children.size() == 1) {
                fn.setReturnType(ScalarType.createDatetimeV2Type(6));
            }
        }
    }

    // if return type is nested type, need to be determined the sub-element type
    private void analyzeNestedFunction() {
        // array
        if ("array".equalsIgnoreCase(fnName.getFunction())) {
            if (children.size() > 0) {
                this.type = new ArrayType(children.get(0).getType());
            }
        }

        if (this.type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            // Now Array type do not support ARRAY<NOT_NULL>, set it too true temporarily
            boolean containsNull = true;
            for (Expr child : children) {
                Type childType = child.getType();
                if (childType instanceof ArrayType) {
                    containsNull |= ((ArrayType) childType).getContainsNull();
                }
            }
            arrayType.setContainsNull(containsNull);
        }
    }

    /**
     * rewrite alias function to real function
     * reset function name, function params and it's children to real function's
     *
     * @return
     * @throws AnalysisException
     */
    public Expr rewriteExpr() throws AnalysisException {
        if (isRewrote) {
            return this;
        }
        // clone a new functionCallExpr to rewrite
        FunctionCallExpr retExpr = (FunctionCallExpr) clone();
        // clone origin function call expr in origin stmt
        retExpr.originStmtFnExpr = clone();
        // clone alias function origin expr for alias
        FunctionCallExpr oriExpr = (FunctionCallExpr) ((AliasFunction) retExpr.fn).getOriginFunction().clone();
        // reset fn name
        retExpr.fnName = oriExpr.getFnName();
        // reset fn params
        List<Expr> inputParamsExprs = retExpr.fnParams.exprs();
        List<String> parameters = ((AliasFunction) retExpr.fn).getParameters();
        Preconditions.checkArgument(inputParamsExprs.size() == parameters.size(),
                "Alias function [" + retExpr.fn.getFunctionName().getFunction()
                        + "] args number is not equal to it's definition");
        List<Expr> oriParamsExprs = oriExpr.fnParams.exprs();

        // replace origin function params exprs' with input params expr depending on parameter name
        for (int i = 0; i < oriParamsExprs.size(); i++) {
            Expr expr = replaceParams(parameters, inputParamsExprs, oriParamsExprs.get(i));
            oriParamsExprs.set(i, expr);
        }

        retExpr.fnParams = new FunctionParams(oriExpr.fnParams.isDistinct(), oriParamsExprs);

        // reset children
        retExpr.children.clear();
        retExpr.children.addAll(oriExpr.getChildren());
        retExpr.isRewrote = true;
        return retExpr;
    }

    /**
     * replace origin function expr and it's children with input params exprs depending on parameter name
     *
     * @param parameters
     * @param inputParamsExprs
     * @param oriExpr
     * @return
     * @throws AnalysisException
     */
    private Expr replaceParams(List<String> parameters, List<Expr> inputParamsExprs, Expr oriExpr)
            throws AnalysisException {
        for (int i = 0; i < oriExpr.getChildren().size(); i++) {
            Expr retExpr = replaceParams(parameters, inputParamsExprs, oriExpr.getChild(i));
            oriExpr.setChild(i, retExpr);
        }
        if (oriExpr instanceof SlotRef) {
            String columnName = ((SlotRef) oriExpr).getColumnName();
            int index = parameters.indexOf(columnName);
            if (index != -1) {
                return inputParamsExprs.get(index);
            }
        }
        // Initialize literalExpr without type information, because literalExpr does not save type information
        // when it is persisted, so after fe restart, read the image,
        // it will be missing type and report an error during analyze.
        if (oriExpr instanceof LiteralExpr && oriExpr.getType().equals(Type.INVALID)) {
            oriExpr = LiteralExpr.init((LiteralExpr) oriExpr);
        }
        return oriExpr;
    }

    @Override
    public boolean isVectorized() {
        return false;
    }

    public static FunctionCallExpr createMergeAggCall(
            FunctionCallExpr agg, List<Expr> intermediateParams, List<Expr> realParams) {
        Preconditions.checkState(agg.isAnalyzed);
        Preconditions.checkState(agg.isAggregateFunction());
        FunctionCallExpr result = new FunctionCallExpr(
                agg.fnName, new FunctionParams(false, intermediateParams), true);
        // Inherit the function object from 'agg'.
        result.fn = agg.fn;
        result.type = agg.type;
        result.setAggFnParams(new FunctionParams(false, realParams));
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        fnName.write(out);
        fnParams.write(out);
        out.writeBoolean(isAnalyticFnCall);
        out.writeBoolean(isMergeAggFn);
    }

    public void readFields(DataInput in) throws IOException {
        fnName = FunctionName.read(in);
        fnParams = FunctionParams.read(in);
        if (fnParams.exprs() != null) {
            children.addAll(fnParams.exprs());
        }
        isAnalyticFnCall = in.readBoolean();
        isMergeAggFn = in.readBoolean();
    }

    public static FunctionCallExpr read(DataInput in) throws IOException {
        FunctionCallExpr func = new FunctionCallExpr();
        func.readFields(in);
        return func;
    }

    // Used for store load
    public boolean supportSerializable() {
        for (Expr child : children) {
            if (!child.supportSerializable()) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean isConstantImpl() {
        // TODO: we can't correctly determine const-ness before analyzing 'fn_'. We should
        // rework logic so that we do not call this function on unanalyzed exprs.
        // Aggregate functions are never constant.
        if (fn instanceof AggregateFunction || fn == null) {
            return false;
        }

        final String fnName = this.fnName.getFunction();
        // Non-deterministic functions are never constant.
        if (isNondeterministicBuiltinFnName(fnName)) {
            return false;
        }
        // Sleep is a special function for testing.
        if (fnName.equalsIgnoreCase("sleep")) {
            return false;
        }
        return super.isConstantImpl();
    }

    private static boolean isNondeterministicBuiltinFnName(String fnName) {
        if (fnName.equalsIgnoreCase("rand") || fnName.equalsIgnoreCase("random")
                || fnName.equalsIgnoreCase("uuid")) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hashCode(opcode);
        result = 31 * result + Objects.hashCode(fnName);
        result = 31 * result + Objects.hashCode(fnParams);
        return result;
    }

    public String forJSON(String str) {
        final StringBuilder result = new StringBuilder();
        StringCharacterIterator iterator = new StringCharacterIterator(str);
        char character = iterator.current();
        while (character != StringCharacterIterator.DONE) {
            if (character == '\"') {
                result.append("\\\"");
            } else if (character == '\\') {
                result.append("\\\\");
            } else if (character == '/') {
                result.append("\\/");
            } else if (character == '\b') {
                result.append("\\b");
            } else if (character == '\f') {
                result.append("\\f");
            } else if (character == '\n') {
                result.append("\\n");
            } else if (character == '\r') {
                result.append("\\r");
            } else if (character == '\t') {
                result.append("\\t");
            } else {
                result.append(character);
            }
            character = iterator.next();
        }
        return result.toString();
    }

    public void finalizeImplForNereids() throws AnalysisException {
        // TODO: support other functions
        // TODO: Supports type conversion to match the type of the function's parameters
        if (fnName.getFunction().equalsIgnoreCase("sum")) {
            // Prevent the cast type in vector exec engine
            Type childType = getChild(0).type.getMaxResolutionType();
            fn = getBuiltinFunction(fnName.getFunction(), new Type[] {childType},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            type = fn.getReturnType();
        } else if (fnName.getFunction().equalsIgnoreCase("count")) {
            fn = getBuiltinFunction(fnName.getFunction(), new Type[0], Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            type = fn.getReturnType();
        } else if (fnName.getFunction().equalsIgnoreCase("substring")
                || fnName.getFunction().equalsIgnoreCase("cast")) {
            Type[] childTypes = getChildren().stream().map(t -> t.type).toArray(Type[]::new);
            fn = getBuiltinFunction(fnName.getFunction(), childTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            type = fn.getReturnType();
        } else if (fnName.getFunction().equalsIgnoreCase("year")
                || fnName.getFunction().equalsIgnoreCase("max")
                || fnName.getFunction().equalsIgnoreCase("min")
                || fnName.getFunction().equalsIgnoreCase("avg")
                || fnName.getFunction().equalsIgnoreCase("weekOfYear")) {
            Type childType = getChild(0).type;
            fn = getBuiltinFunction(fnName.getFunction(), new Type[] {childType},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            type = fn.getReturnType();
        }
    }

    /**
     * NOTICE: This function only used for Nereids, should not call it if u don't know what it is mean.
     */
    public void setMergeForNereids(boolean isMergeAggFn) {
        this.isMergeAggFn = isMergeAggFn;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public void setOrderByElements(List<OrderByElement> orderByElements) {
        this.orderByElements = orderByElements;
    }

    private void setChildren() {
        orderByElements.forEach(o -> addChild(o.getExpr()));
    }
}

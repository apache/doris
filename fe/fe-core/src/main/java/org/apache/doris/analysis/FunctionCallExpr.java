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
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.normalize.Normalizer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// TODO: for aggregations, we need to unify the code paths for builtins and UDAs.
public class FunctionCallExpr extends Expr {
    public static final ImmutableSet<String> STDDEV_FUNCTION_SET = new ImmutableSortedSet.Builder(
            String.CASE_INSENSITIVE_ORDER)
            .add("stddev").add("stddev_val").add("stddev_samp").add("stddev_pop").add("variance").add("variance_pop")
            .add("variance_pop").add("var_samp").add("var_pop").add("variance_samp").add("avg_weighted")
            .add("std").build();
    public static final Map<String, java.util.function.BiFunction<ArrayList<Expr>, Type, Type>> PRECISION_INFER_RULE;
    public static final java.util.function.BiFunction<ArrayList<Expr>, Type, Type> DEFAULT_PRECISION_INFER_RULE;
    public static final ImmutableSet<String> ROUND_FUNCTION_SET = new ImmutableSortedSet.Builder(
            String.CASE_INSENSITIVE_ORDER)
            .add("round").add("round_bankers").add("ceil").add("floor")
            .add("truncate").add("dround").add("dceil").add("dfloor").build();
    public static final ImmutableSet<String> STRING_SEARCH_FUNCTION_SET = new ImmutableSortedSet.Builder(
            String.CASE_INSENSITIVE_ORDER)
            .add("multi_search_all_positions").add("multi_match_any").build();

    static {
        java.util.function.BiFunction<ArrayList<Expr>, Type, Type> sumRule = (children, returnType) -> {
            Preconditions.checkArgument(children != null && children.size() > 0);
            if (children.get(0).getType().isDecimalV3()) {
                return ScalarType.createDecimalV3Type(ScalarType.MAX_DECIMAL128_PRECISION,
                        ((ScalarType) children.get(0).getType()).getScalarScale());
            } else {
                return returnType;
            }
        };
        DEFAULT_PRECISION_INFER_RULE = (children, returnType) -> {
            if (children != null && children.size() > 0
                    && children.get(0).getType().isDecimalV3() && returnType.isDecimalV3()) {
                return children.get(0).getType();
            } else if (children != null && children.size() > 0 && children.get(0).getType().isDatetimeV2()
                    && returnType.isDatetimeV2()) {
                return children.get(0).getType();
            } else if (children != null && children.size() > 0 && children.get(0).getType().isDecimalV2()
                    && returnType.isDecimalV2()) {
                return children.get(0).getType();
            } else {
                return returnType;
            }
        };

        java.util.function.BiFunction<ArrayList<Expr>, Type, Type> roundRule = (children, returnType) -> {
            Preconditions.checkArgument(children != null && children.size() > 0);
            if (children.size() == 1 && children.get(0).getType().isDecimalV3()) {
                return ScalarType.createDecimalV3Type(children.get(0).getType().getPrecision(), 0);
            } else if (children.size() == 2) {
                Expr scaleExpr = children.get(1);
                if (scaleExpr instanceof IntLiteral
                        || (scaleExpr instanceof CastExpr && scaleExpr.getChild(0) instanceof IntLiteral)) {
                    if (children.get(1) instanceof CastExpr && children.get(1).getChild(0) instanceof IntLiteral) {
                        children.get(1).getChild(0).setType(children.get(1).getType());
                        children.set(1, children.get(1).getChild(0));
                    } else {
                        children.get(1).setType(Type.INT);
                    }
                    int scaleArg = (int) (((IntLiteral) children.get(1)).getValue());
                    return ScalarType.createDecimalV3Type(children.get(0).getType().getPrecision(),
                            Math.min(Math.max(scaleArg, 0), ((ScalarType) children.get(0).getType()).decimalScale()));
                } else {
                    // Scale argument is a Column, always use same scale with input decimal
                    return ScalarType.createDecimalV3Type(children.get(0).getType().getPrecision(),
                            ((ScalarType) children.get(0).getType()).decimalScale());
                }
            } else {
                return returnType;
            }
        };

        java.util.function.BiFunction<ArrayList<Expr>, Type, Type> arrayDateTimeV2OrDecimalV3Rule
                = (children, returnType) -> {
                    Preconditions.checkArgument(children != null && children.size() > 0);
                    if (children.get(0).getType().isArrayType() && (
                            ((ArrayType) children.get(0).getType()).getItemType().isDecimalV3()
                                    || ((ArrayType) children.get(0)
                                    .getType()).getItemType().isDecimalV2() || ((ArrayType) children.get(0)
                                    .getType()).getItemType().isDatetimeV2())) {
                        return ((ArrayType) children.get(0).getType()).getItemType();
                    } else {
                        return returnType;
                    }
                };
        java.util.function.BiFunction<ArrayList<Expr>, Type, Type> arrayDecimal128Rule
                = (children, returnType) -> {
                    Preconditions.checkArgument(children != null && children.size() > 0);
                    if (children.get(0).getType().isArrayType() && (
                            ((ArrayType) children.get(0).getType()).getItemType().isDecimalV3())) {
                        return ScalarType.createDecimalV3Type(ScalarType.MAX_DECIMAL128_PRECISION,
                                ((ScalarType) ((ArrayType) children.get(0).getType()).getItemType()).getScalarScale());
                    } else {
                        return returnType;
                    }
                };
        java.util.function.BiFunction<ArrayList<Expr>, Type, Type> arrayDecimal128ArrayRule
                = (children, returnType) -> {
                    Preconditions.checkArgument(children != null && children.size() > 0);
                    if (children.get(0).getType().isArrayType() && (
                            ((ArrayType) children.get(0).getType()).getItemType().isDecimalV3())) {
                        ArrayType childArrayType = (ArrayType) children.get(0).getType();
                        Type itemType = ScalarType.createDecimalV3Type(ScalarType.MAX_DECIMAL128_PRECISION,
                                ((ScalarType) childArrayType.getItemType()).getScalarScale());
                        return ArrayType.create(itemType, childArrayType.getContainsNull());
                    } else {
                        return returnType;
                    }
                };
        PRECISION_INFER_RULE = new HashMap<>();
        PRECISION_INFER_RULE.put("sum", sumRule);
        PRECISION_INFER_RULE.put("multi_distinct_sum", sumRule);
        PRECISION_INFER_RULE.put("avg", (children, returnType) -> {
            // TODO: how to set scale?
            Preconditions.checkArgument(children != null && children.size() > 0);
            if (children.get(0).getType().isDecimalV3()) {
                return ScalarType.createDecimalV3Type(ScalarType.MAX_DECIMAL128_PRECISION,
                        Math.max(((ScalarType) children.get(0).getType()).getScalarScale(), 4));
            } else {
                return returnType;
            }
        });
        PRECISION_INFER_RULE.put("if", (children, returnType) -> {
            Preconditions.checkArgument(children != null && children.size() == 3);
            if (children.get(1).getType().isDecimalV3() && children.get(2).getType().isDecimalV3()) {
                return Expr.getAssignmentCompatibleType(children.subList(1, children.size()));
            } else if (children.get(1).getType().isDatetimeV2() && children.get(2).getType().isDatetimeV2()) {
                return Expr.getAssignmentCompatibleType(children.subList(1, children.size()));
            } else {
                return returnType;
            }
        });

        PRECISION_INFER_RULE.put("ifnull", (children, returnType) -> {
            Preconditions.checkArgument(children != null && children.size() == 2);
            if (children.get(0).getType().isDecimalV3() && children.get(1).getType().isDecimalV3()) {
                return Expr.getAssignmentCompatibleType(children);
            } else if (children.get(0).getType().isDatetimeV2() && children.get(1).getType().isDatetimeV2()) {
                return Expr.getAssignmentCompatibleType(children);
            } else {
                return returnType;
            }
        });

        PRECISION_INFER_RULE.put("coalesce", (children, returnType) -> {
            boolean isDecimalV3 = true;
            boolean isDateTimeV2 = true;

            Type assignmentCompatibleType = Expr.getAssignmentCompatibleType(children);
            for (Expr child : children) {
                isDecimalV3 = isDecimalV3 && child.getType().isDecimalV3();
                isDateTimeV2 = isDateTimeV2 && child.getType().isDatetimeV2();
            }
            if ((isDecimalV3 || isDateTimeV2) && assignmentCompatibleType.isValid()) {
                return assignmentCompatibleType;
            } else {
                return returnType;
            }
        });

        PRECISION_INFER_RULE.put("array_min", arrayDateTimeV2OrDecimalV3Rule);
        PRECISION_INFER_RULE.put("array_max", arrayDateTimeV2OrDecimalV3Rule);
        PRECISION_INFER_RULE.put("element_at", arrayDateTimeV2OrDecimalV3Rule);
        PRECISION_INFER_RULE.put("%element_extract%", arrayDateTimeV2OrDecimalV3Rule);
        PRECISION_INFER_RULE.put("array_avg", arrayDecimal128Rule);
        PRECISION_INFER_RULE.put("array_sum", arrayDecimal128Rule);
        PRECISION_INFER_RULE.put("array_product", arrayDecimal128Rule);
        PRECISION_INFER_RULE.put("array_cum_sum", arrayDecimal128ArrayRule);
        PRECISION_INFER_RULE.put("round", roundRule);
        PRECISION_INFER_RULE.put("round_bankers", roundRule);
        PRECISION_INFER_RULE.put("ceil", roundRule);
        PRECISION_INFER_RULE.put("floor", roundRule);
        PRECISION_INFER_RULE.put("dround", roundRule);
        PRECISION_INFER_RULE.put("dceil", roundRule);
        PRECISION_INFER_RULE.put("dfloor", roundRule);
        PRECISION_INFER_RULE.put("truncate", roundRule);
    }

    public static final ImmutableSet<String> TIME_FUNCTIONS_WITH_PRECISION = new ImmutableSortedSet.Builder(
            String.CASE_INSENSITIVE_ORDER)
            .add("now").add("current_timestamp").add("localtime").add("localtimestamp").build();
    public static final int STDDEV_DECIMAL_SCALE = 9;
    private static final String ELEMENT_EXTRACT_FN_NAME = "%element_extract%";

    private static final Logger LOG = LogManager.getLogger(FunctionCallExpr.class);

    @SerializedName("fnn")
    private FunctionName fnName;
    @SerializedName("fnp")
    private FunctionParams fnParams;

    private FunctionParams aggFnParams;

    private List<OrderByElement> orderByElements = Lists.newArrayList();

    // check analytic function
    @SerializedName("iafc")
    private boolean isAnalyticFnCall = false;
    // check table function
    @SerializedName("itfc")
    private boolean isTableFnCall = false;

    // Indicates whether this is a merge aggregation function that should use the
    // merge
    // instead of the update symbol. This flag also affects the behavior of
    // resetAnalysisState() which is used during expr substitution.
    private boolean isMergeAggFn;

    // use to record the num of json_object parameters
    private int originChildSize;
    // Save the functionCallExpr in the original statement
    private Expr originStmtFnExpr;

    private boolean isRewrote = false;

    // this field is set by nereids, so we would not get arg types by the children.
    private Optional<List<Type>> argTypesForNereids = Optional.empty();

    public void setAggFnParams(FunctionParams aggFnParams) {
        this.aggFnParams = aggFnParams;
    }

    public FunctionParams getAggFnParams() {
        return aggFnParams;
    }

    public void setIsAnalyticFnCall(boolean v) {
        isAnalyticFnCall = v;
    }

    public void setTableFnCall(boolean tableFnCall) {
        isTableFnCall = tableFnCall;
    }

    public void setFnName(FunctionName fnName) {
        this.fnName = fnName;
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
    protected FunctionCallExpr() {
        super();
    }

    @Override
    public String getExprName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of(Utils.normalizeName(this.getFnName().getFunction(), DEFAULT_EXPR_NAME));
        }
        return this.exprName.get();
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
            if (!AggregateFunction.SUPPORT_ORDER_BY_AGGREGATE_FUNCTION_NAME_SET
                    .contains(fnName.getFunction().toLowerCase())) {
                throw new AnalysisException(
                        "ORDER BY not support for the function:" + fnName.getFunction().toLowerCase());
            }
        }
        setChildren();
        originChildSize = children.size();
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

    public FunctionCallExpr(String functionName, FunctionParams params, FunctionParams aggFnParams,
            Optional<List<Type>> argTypes) {
        this.fnName = new FunctionName(functionName);
        this.fnParams = params;
        this.isMergeAggFn = false;
        this.aggFnParams = aggFnParams;
        if (fnParams.exprs() != null) {
            children.addAll(fnParams.exprs());
        }
        this.originChildSize = children.size();
        this.argTypesForNereids = argTypes;
    }

    // nereids scalar function call expr constructor without finalize/analyze
    public FunctionCallExpr(Function function, FunctionParams functionParams) {
        this(function, functionParams, null, false, functionParams.exprs());
    }

    // nereids aggregate function call expr constructor without finalize/analyze
    public FunctionCallExpr(Function function, FunctionParams functionParams, FunctionParams aggFnParams,
            boolean isMergeAggFn, List<Expr> children) {
        this.fnName = function.getFunctionName();
        this.fn = function;
        this.type = function.getReturnType();
        this.fnParams = functionParams;
        this.aggFnParams = aggFnParams;
        this.children.addAll(children);
        this.originChildSize = children.size();
        this.isMergeAggFn = isMergeAggFn;
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
        this.originChildSize = children.size();
    }

    protected FunctionCallExpr(FunctionCallExpr other) {
        super(other);
        fnName = other.fnName != null ? other.fnName.clone() : null;
        orderByElements = other.orderByElements;
        isAnalyticFnCall = other.isAnalyticFnCall;
        // aggOp = other.aggOp;
        // fnParams = other.fnParams;
        // Clone the params in a way that keeps the children_ and the params.exprs()
        // in sync. The children have already been cloned in the super c'tor.
        fnParams = other.fnParams.clone(children);
        originChildSize = other.originChildSize;
        aggFnParams = other.aggFnParams;

        this.isMergeAggFn = other.isMergeAggFn;
        fn = other.fn;
        this.isTableFnCall = other.isTableFnCall;
    }

    public static int computeJsonDataType(Type type) {
        if (type.isNull()) {
            return 0;
        } else if (type.isBoolean()) {
            return 1;
        } else if (type.isFixedPointType()) {
            if (type.isInteger32Type()) {
                return 2;
            } else {
                return 5;
            }
        } else if (type.isFloatingPointType() || type.isDecimalV2() || type.isDecimalV3()) {
            return 3;
        } else if (type.isTimeV2()) {
            return 4;
        } else if (type.isComplexType() || type.isJsonbType()) {
            return 7;
        } else {
            // default is string for BE execution
            return 6;
        }
    }

    public boolean isMergeAggFn() {
        return isMergeAggFn;
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
        if (orderByElements == null ^ o.orderByElements == null) {
            return false;
        }
        if (orderByElements != null) {
            if (orderByElements.size() != o.orderByElements.size()) {
                return false;
            }
            for (int i = 0; i < orderByElements.size(); i++) {
                if (!orderByElements.get(i).equals(o.orderByElements.get(i))) {
                    return false;
                }
            }
        }
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

        if (fnName.getFunction().equalsIgnoreCase("char")) {
            for (int i = 1; i < len; ++i) {
                sb.append(children.get(i).toSql());
                if (i < len - 1) {
                    sb.append(", ");
                }
            }
            sb.append(" using ");
            String encodeType = children.get(0).toSql();
            if (encodeType.charAt(0) == '\'') {
                encodeType = encodeType.substring(1, encodeType.length());
            }
            if (encodeType.charAt(encodeType.length() - 1) == '\'') {
                encodeType = encodeType.substring(0, encodeType.length() - 1);
            }
            sb.append(encodeType).append(")");
            return sb.toString();
        }

        // XXX_diff are used by nereids only
        if (fnName.getFunction().equalsIgnoreCase("years_diff") || fnName.getFunction().equalsIgnoreCase("months_diff")
                || fnName.getFunction().equalsIgnoreCase("days_diff")
                || fnName.getFunction().equalsIgnoreCase("hours_diff")
                || fnName.getFunction().equalsIgnoreCase("minutes_diff")
                || fnName.getFunction().equalsIgnoreCase("seconds_diff")
                || fnName.getFunction().equalsIgnoreCase("milliseconds_diff")
                || fnName.getFunction().equalsIgnoreCase("microseconds_diff")) {
            sb.append(children.get(0).toSql()).append(", ");
            sb.append(children.get(1).toSql()).append(")");
            return sb.toString();
        }
        // used by nereids END

        for (int i = 0; i < len; ++i) {
            if (i != 0) {
                if (fnName.getFunction().equalsIgnoreCase("group_concat")
                        && orderByElements.size() > 0 && i == len - orderByElements.size()) {
                    sb.append(" ");
                } else {
                    sb.append(", ");
                }
            }
            if (ConnectContext.get() != null && ConnectContext.get().getState().isQuery() && i == 1
                    && (fnName.getFunction().equalsIgnoreCase("aes_decrypt")
                            || fnName.getFunction().equalsIgnoreCase("aes_encrypt")
                            || fnName.getFunction().equalsIgnoreCase("sm4_decrypt")
                            || fnName.getFunction().equalsIgnoreCase("sm4_encrypt"))) {
                sb.append("\'***\'");
                continue;
            } else if (orderByElements.size() > 0 && i == len - orderByElements.size()) {
                sb.append("ORDER BY ");
            }
            sb.append(children.get(i).toSql());
            if (orderByElements.size() > 0 && i >= len - orderByElements.size()) {
                if (orderByElements.get(i - len + orderByElements.size()).getIsAsc()) {
                    sb.append(" ASC");
                } else {
                    sb.append(" DESC");
                }
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private String paramsToSql(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");

        if (fnParams.isStar()) {
            sb.append("*");
        }
        if (fnParams.isDistinct()) {
            sb.append("DISTINCT ");
        }
        int len = children.size();

        if (fnName.getFunction().equalsIgnoreCase("char")) {
            for (int i = 1; i < len; ++i) {
                sb.append(children.get(i).toSql(disableTableName, needExternalSql, tableType, table));
                if (i < len - 1) {
                    sb.append(", ");
                }
            }
            sb.append(" using ");
            String encodeType = children.get(0).toSql(disableTableName, needExternalSql, tableType, table);
            if (encodeType.charAt(0) == '\'') {
                encodeType = encodeType.substring(1, encodeType.length());
            }
            if (encodeType.charAt(encodeType.length() - 1) == '\'') {
                encodeType = encodeType.substring(0, encodeType.length() - 1);
            }
            sb.append(encodeType).append(")");
            return sb.toString();
        }

        // XXX_diff are used by nereids only
        if (fnName.getFunction().equalsIgnoreCase("years_diff") || fnName.getFunction().equalsIgnoreCase("months_diff")
                || fnName.getFunction().equalsIgnoreCase("days_diff")
                || fnName.getFunction().equalsIgnoreCase("hours_diff")
                || fnName.getFunction().equalsIgnoreCase("minutes_diff")
                || fnName.getFunction().equalsIgnoreCase("seconds_diff")
                || fnName.getFunction().equalsIgnoreCase("milliseconds_diff")
                || fnName.getFunction().equalsIgnoreCase("microseconds_diff")) {
            sb.append(children.get(0).toSql(disableTableName, needExternalSql, tableType, table)).append(", ");
            sb.append(children.get(1).toSql(disableTableName, needExternalSql, tableType, table)).append(")");
            return sb.toString();
        }

        for (int i = 0; i < len; ++i) {
            if (i != 0) {
                if (fnName.getFunction().equalsIgnoreCase("group_concat")
                        && orderByElements.size() > 0 && i == len - orderByElements.size()) {
                    sb.append(" ");
                } else {
                    sb.append(", ");
                }
            }
            if (ConnectContext.get() != null && ConnectContext.get().getState().isQuery() && i == 1
                    && (fnName.getFunction().equalsIgnoreCase("aes_decrypt")
                    || fnName.getFunction().equalsIgnoreCase("aes_encrypt")
                    || fnName.getFunction().equalsIgnoreCase("sm4_decrypt")
                    || fnName.getFunction().equalsIgnoreCase("sm4_encrypt"))) {
                sb.append("\'***\'");
                continue;
            } else if (orderByElements.size() > 0 && i == len - orderByElements.size()) {
                sb.append("ORDER BY ");
            }
            sb.append(children.get(i).toSql(disableTableName, needExternalSql, tableType, table));
            if (orderByElements.size() > 0 && i >= len - orderByElements.size()) {
                if (orderByElements.get(i - len + orderByElements.size()).getIsAsc()) {
                    sb.append(" ASC");
                } else {
                    sb.append(" DESC");
                }
            }
        }
        sb.append(")");
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

        // when function is like or regexp, the expr generated sql should be like this
        // eg: child1 like child2
        if (fnName.getFunction().equalsIgnoreCase("like")
                || fnName.getFunction().equalsIgnoreCase("regexp")) {
            sb.append(children.get(0).toSql());
            sb.append(" ");
            sb.append(((FunctionCallExpr) expr).fnName);
            sb.append(" ");
            sb.append(children.get(1).toSql());
        } else if (fnName.getFunction().equalsIgnoreCase("encryptkeyref")) {
            sb.append("key ");
            for (int i = 0; i < children.size(); i++) {
                String str = ((StringLiteral) children.get(i)).getValue();
                if (str.isEmpty()) {
                    continue;
                }
                sb.append(str);
                sb.append(".");
            }
            sb.deleteCharAt(sb.length() - 1);
        } else {
            sb.append(((FunctionCallExpr) expr).fnName);
            sb.append(paramsToSql());
            if (fnName.getFunction().equalsIgnoreCase("json_quote")
                    || fnName.getFunction().equalsIgnoreCase("json_array")
                    || fnName.getFunction().equalsIgnoreCase("json_object")
                    || fnName.getFunction().equalsIgnoreCase("json_insert")
                    || fnName.getFunction().equalsIgnoreCase("json_replace")
                    || fnName.getFunction().equalsIgnoreCase("json_set")) {
                return forJSON(sb.toString());
            }
        }
        return sb.toString();
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        Expr expr;
        if (originStmtFnExpr != null) {
            expr = originStmtFnExpr;
        } else {
            expr = this;
        }
        StringBuilder sb = new StringBuilder();

        // when function is like or regexp, the expr generated sql should be like this
        // eg: child1 like child2
        if (fnName.getFunction().equalsIgnoreCase("like")
                || fnName.getFunction().equalsIgnoreCase("regexp")) {
            sb.append(children.get(0).toSql(disableTableName, needExternalSql, tableType, table));
            sb.append(" ");
            sb.append(((FunctionCallExpr) expr).fnName);
            sb.append(" ");
            sb.append(children.get(1).toSql(disableTableName, needExternalSql, tableType, table));
        } else if (fnName.getFunction().equalsIgnoreCase("encryptkeyref")) {
            sb.append("key ");
            for (int i = 0; i < children.size(); i++) {
                String str = ((StringLiteral) children.get(i)).getValue();
                if (str.isEmpty()) {
                    continue;
                }
                sb.append(str);
                sb.append(".");
            }
            sb.deleteCharAt(sb.length() - 1);
        } else {
            sb.append(((FunctionCallExpr) expr).fnName);
            sb.append(paramsToSql(disableTableName, needExternalSql, tableType, table));
            if (fnName.getFunction().equalsIgnoreCase("json_quote")
                    || fnName.getFunction().equalsIgnoreCase("json_array")
                    || fnName.getFunction().equalsIgnoreCase("json_object")
                    || fnName.getFunction().equalsIgnoreCase("json_insert")
                    || fnName.getFunction().equalsIgnoreCase("json_replace")
                    || fnName.getFunction().equalsIgnoreCase("json_set")) {
                return forJSON(sb.toString());
            }
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
        if (fnName.getFunction().equalsIgnoreCase("aes_decrypt")
                || fnName.getFunction().equalsIgnoreCase("aes_encrypt")
                || fnName.getFunction().equalsIgnoreCase("sm4_decrypt")
                || fnName.getFunction().equalsIgnoreCase("sm4_encrypt")) {
            len = len - 1;
        }
        for (int i = 0; i < len; ++i) {
            if (i == 1 && (fnName.getFunction().equalsIgnoreCase("aes_decrypt")
                    || fnName.getFunction().equalsIgnoreCase("aes_encrypt")
                    || fnName.getFunction().equalsIgnoreCase("sm4_decrypt"))) {
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
                || fnName.getFunction().equalsIgnoreCase("json_object")
                || fnName.getFunction().equalsIgnoreCase("json_insert")
                || fnName.getFunction().equalsIgnoreCase("json_replace")
                || fnName.getFunction().equalsIgnoreCase("json_set")) {
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

    public boolean isAggregateFunction() {
        Preconditions.checkState(fn != null);
        return fn instanceof AggregateFunction && !isAnalyticFnCall;
    }

    public boolean isDistinct() {
        Preconditions.checkState(isAggregateFunction());
        return fnParams.isDistinct();
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

    /**
     * This analyzeImp used for DefaultValueExprDef
     * to generate a builtinFunction.
     *
     * @throws AnalysisException
     */
    public void analyzeImplForDefaultValue(Type type) throws AnalysisException {
        Type[] childTypes = new Type[children.size()];
        for (int i = 0; i < children.size(); i++) {
            childTypes[i] = children.get(i).type;
        }
        fn = new Function(
                getBuiltinFunction(fnName.getFunction(), childTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));
        fn.setReturnType(type);
        this.type = type;
        for (int i = 0; i < children.size(); ++i) {
            if (getChild(i).getType().isNull()) {
                uncheckedCastChild(Type.BOOLEAN, i);
            }
        }
    }

    private static boolean match(String pattern, int pos, String value) {
        int length = value.length();
        int end = pattern.length();
        return pos + length <= end && pattern.substring(pos, pos + length).equals(value);
    }

    private static int parseNumber(String s) {

        String[] n = s.split(""); // array of strings
        int num = 0;
        for (String value : n) {
            // validating numbers
            if ((value.matches("[0-9]+"))) {
                num++;
            } else {
                return num;
            }
        }
        return num;
    }

    public static boolean parsePattern(String pattern) {
        int pos = 0;
        int len = pattern.length();
        while (pos < len) {
            if (match(pattern, pos, "(?")) {
                pos += 2;
                if (match(pattern, pos, "t")) {
                    pos += 1;
                    if (match(pattern, pos, "<=") || match(pattern, pos, "==")
                            || match(pattern, pos, ">=")) {
                        pos += 2;
                    } else if (match(pattern, pos, ">") || match(pattern, pos, "<")) {
                        pos += 1;
                    } else {
                        return false;
                    }

                    int numLen = parseNumber(pattern.substring(pos));
                    if (numLen == 0) {
                        return false;
                    } else {
                        pos += numLen;
                    }
                } else {
                    int numLen = parseNumber(pattern.substring(pos));
                    if (numLen == 0) {
                        return false;
                    } else {
                        pos += numLen;
                    }
                }
                if (!match(pattern, pos, ")")) {
                    return false;
                }
                pos += 1;
            } else if (match(pattern, pos, ".*")) {
                pos += 2;
            } else if (match(pattern, pos, ".")) {
                pos += 1;
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void normalize(TExprNode msg, Normalizer normalizer) {
        String functionName = fnName.getFunction().toUpperCase();
        if (FunctionSet.nonDeterministicFunctions.contains(functionName)
                || "NOW".equals(functionName)
                || (FunctionSet.nonDeterministicTimeFunctions.contains(functionName) && children.isEmpty())) {
            throw new IllegalStateException("Can not normalize non deterministic functions");
        }
        super.normalize(msg, normalizer);
    }

    @Override
    protected boolean isConstantImpl() {
        // TODO: we can't correctly determine const-ness before analyzing 'fn_'. We
        // should
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

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public void setOrderByElements(List<OrderByElement> orderByElements) {
        this.orderByElements = orderByElements;
    }

    private void setChildren() {
        orderByElements.forEach(o -> addChild(o.getExpr()));
    }

    // eg: date_floor("0001-01-01 00:00:18",interval 5 second) convert to
    // second_floor("0001-01-01 00:00:18", 5, "0001-01-01 00:00:00");
    public static FunctionCallExpr functionWithIntervalConvert(String functionName, Expr str, Expr interval,
            String timeUnitIdent) throws AnalysisException {
        String newFunctionName = timeUnitIdent + "_" + functionName.split("_")[1];
        List<Expr> params = new ArrayList<>();
        Expr defaultDatetime = new DateLiteral(0001, 01, 01, 0, 0, 0, 0, Type.DATETIMEV2);
        params.add(str);
        params.add(interval);
        params.add(defaultDatetime);
        return new FunctionCallExpr(newFunctionName, params);
    }
}

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
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.util.Utils;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO: for aggregations, we need to unify the code paths for builtins and UDAs.
public class FunctionCallExpr extends Expr {
    public static final ImmutableSet<String> STDDEV_FUNCTION_SET = new ImmutableSortedSet.Builder(
            String.CASE_INSENSITIVE_ORDER)
            .add("stddev").add("stddev_val").add("stddev_samp").add("stddev_pop").add("variance").add("variance_pop")
            .add("variance_pop").add("var_samp").add("var_pop").add("variance_samp").add("avg_weighted").build();
    public static final Map<String, java.util.function.BiFunction<ArrayList<Expr>, Type, Type>> PRECISION_INFER_RULE;
    public static final java.util.function.BiFunction<ArrayList<Expr>, Type, Type> DEFAULT_PRECISION_INFER_RULE;
    public static final ImmutableSet<String> ROUND_FUNCTION_SET = new ImmutableSortedSet.Builder(
            String.CASE_INSENSITIVE_ORDER)
            .add("round").add("round_bankers").add("ceil").add("floor")
            .add("truncate").add("dround").add("dceil").add("dfloor").build();

    private final AtomicBoolean addOnce = new AtomicBoolean(false);

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
                Preconditions.checkArgument(children.get(1) instanceof IntLiteral
                        || (children.get(1) instanceof CastExpr
                                && children.get(1).getChild(0) instanceof IntLiteral),
                        "2nd argument of function round/floor/ceil/truncate must be literal");
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

    private FunctionName fnName;
    // private BuiltinAggregateFunction.Operator aggOp;
    private FunctionParams fnParams;

    private FunctionParams aggFnParams;

    private List<OrderByElement> orderByElements = Lists.newArrayList();

    // check analytic function
    private boolean isAnalyticFnCall = false;
    // check table function
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
    private FunctionCallExpr() {
        super();
    }

    @Override
    protected String getExprName() {
        return Utils.normalizeName(this.getFnName().getFunction(), DEFAULT_EXPR_NAME);
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
        fnName = other.fnName;
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

    public String parseJsonValueModifyDataType() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < children.size(); ++i) {
            Type type = getChild(i).getType();
            if (i > 0 && (i & 1) == 0 && type.isNull()) {
                children.set(i, new StringLiteral("NULL"));
            }
            sb.append(computeJsonDataType(type));
        }
        return sb.toString();
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
            }
            sb.append(computeJsonDataType(type));
        }
        return sb.toString();
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
        } else if (type.isTime()) {
            return 4;
        } else {
            return 6;
        }
    }

    public boolean isMergeAggFn() {
        return isMergeAggFn;
    }

    @Override
    protected Expr substituteImpl(ExprSubstitutionMap smap, ExprSubstitutionMap disjunctsMap, Analyzer analyzer) {
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
        if (isImplicitCast()) {
            return getChild(0).substituteImpl(smap, disjunctsMap, analyzer);
        }
        if (smap != null) {
            Expr substExpr = smap.get(this);
            if (substExpr != null) {
                return substExpr.clone();
            }
        }
        if (Expr.IS_OR_PREDICATE.apply(this) && disjunctsMap != null) {
            smap = disjunctsMap;
            disjunctsMap = null;
        }
        for (int i = 0; i < children.size(); ++i) {
            // we shouldn't change literal expr in function call expr
            if (!(children.get(i) instanceof LiteralExpr)) {
                children.set(i, children.get(i).substituteImpl(smap, disjunctsMap, analyzer));
            }
        }
        // SlotRefs must remain analyzed to support substitution across query blocks. All
        // other exprs must be analyzed again after the substitution to add implicit casts
        // and for resolving their correct function signature.
        resetAnalysisState();
        return this;
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
        if (orderByElements.size() != o.orderByElements.size()) {
            return false;
        }
        for (int i = 0; i < orderByElements.size(); i++) {
            if (!orderByElements.get(i).equals(o.orderByElements.get(i))) {
                return false;
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

        if (fnName.getFunction().equalsIgnoreCase("json_array")
                || fnName.getFunction().equalsIgnoreCase("json_object")
                || fnName.getFunction().equalsIgnoreCase("json_insert")
                || fnName.getFunction().equalsIgnoreCase("json_replace")
                || fnName.getFunction().equalsIgnoreCase("json_set")) {
            len = len - 1;
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
                            || fnName.getFunction().equalsIgnoreCase("sm4_encrypt")
                            || fnName.getFunction().equalsIgnoreCase("aes_decrypt_v2")
                            || fnName.getFunction().equalsIgnoreCase("aes_encrypt_v2")
                            || fnName.getFunction().equalsIgnoreCase("sm4_decrypt_v2")
                            || fnName.getFunction().equalsIgnoreCase("sm4_encrypt_v2"))) {
                sb.append("\'***\'");
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
                || fnName.getFunction().equalsIgnoreCase("json_object")
                || fnName.getFunction().equalsIgnoreCase("json_insert")
                || fnName.getFunction().equalsIgnoreCase("json_replace")
                || fnName.getFunction().equalsIgnoreCase("json_set")) {
            len = len - 1;
        }
        if (fnName.getFunction().equalsIgnoreCase("aes_decrypt")
                || fnName.getFunction().equalsIgnoreCase("aes_encrypt")
                || fnName.getFunction().equalsIgnoreCase("sm4_decrypt")
                || fnName.getFunction().equalsIgnoreCase("sm4_encrypt")
                || fnName.getFunction().equalsIgnoreCase("aes_decrypt_v2")
                || fnName.getFunction().equalsIgnoreCase("aes_encrypt_v2")
                || fnName.getFunction().equalsIgnoreCase("sm4_decrypt_v2")
                || fnName.getFunction().equalsIgnoreCase("sm4_encrypt_v2")) {
            len = len - 1;
        }
        for (int i = 0; i < len; ++i) {
            if (i == 1 && (fnName.getFunction().equalsIgnoreCase("aes_decrypt")
                    || fnName.getFunction().equalsIgnoreCase("aes_encrypt")
                    || fnName.getFunction().equalsIgnoreCase("sm4_decrypt")
                    || fnName.getFunction().equalsIgnoreCase("sm4_encrypt")
                    || fnName.getFunction().equalsIgnoreCase("aes_decrypt_v2")
                    || fnName.getFunction().equalsIgnoreCase("aes_encrypt_v2")
                    || fnName.getFunction().equalsIgnoreCase("sm4_decrypt_v2")
                    || fnName.getFunction().equalsIgnoreCase("sm4_encrypt_v2"))) {
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
                if (child.type.isOnlyMetricType() && !child.type.isComplexType()) {
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

        if (fnName.getFunction().equalsIgnoreCase("json_insert")
                || fnName.getFunction().equalsIgnoreCase("json_replace")
                || fnName.getFunction().equalsIgnoreCase("json_set")) {
            if (((children.size() & 1) == 0 || children.size() < 3)
                    && (originChildSize == children.size())) {
                throw new AnalysisException(fnName.getFunction() + " need odd parameters, and >= 3 arguments: "
                    + this.toSql());
            }
            String res = parseJsonValueModifyDataType();
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

            if (fnParams.isDistinct() && !orderByElements.isEmpty()) {
                throw new AnalysisException(
                        "group_concat don't support using distinct with order by together: " + this.toSql());
            }

            return;
        }
        if (fnName.getFunction().equalsIgnoreCase("field")) {
            if (children.size() < 2) {
                throw new AnalysisException(fnName.getFunction() + " function parameter size is less than 2.");
            } else {
                for (int i = 1; i < children.size(); ++i) {
                    if (!getChild(i).isConstant()) {
                        throw new AnalysisException(fnName.getFunction()
                                + " function except for the first argument, other parameter must be a constant.");
                    }
                }
            }
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
                && ((!arg.type.isNumericType() && !arg.type.isNull() && !arg.type.isBoolean())
                        || arg.type.isOnlyMetricType())) {
            throw new AnalysisException(fnName.getFunction() + " requires a numeric parameter: " + this.toSql());
        }
        // DecimalV3 scale lower than DEFAULT_MIN_AVG_DECIMAL128_SCALE should do cast
        if (fnName.getFunction().equalsIgnoreCase("avg") && arg.type.isDecimalV3()
                && arg.type.getDecimalDigits() < ScalarType.DEFAULT_MIN_AVG_DECIMAL128_SCALE) {
            Type t = ScalarType.createDecimalType(arg.type.getPrimitiveType(), arg.type.getPrecision(),
                    ScalarType.DEFAULT_MIN_AVG_DECIMAL128_SCALE);
            Expr e = getChild(0).castTo(t);
            setChild(0, e);
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
                .equalsIgnoreCase(FunctionSet.ORTHOGONAL_BITMAP_INTERSECT_COUNT) || fnName.getFunction()
                .equalsIgnoreCase(FunctionSet.ORTHOGONAL_BITMAP_EXPR_CALCULATE_COUNT) || fnName.getFunction()
                .equalsIgnoreCase(FunctionSet.ORTHOGONAL_BITMAP_EXPR_CALCULATE)) {
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
                throw new AnalysisException(fnName + " function could only have one child");
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
                || fnName.getFunction().equalsIgnoreCase("HLL_RAW_AGG")
                || fnName.getFunction().equalsIgnoreCase("HLL_UNION"))
                && !arg.type.isHllType()) {
            throw new AnalysisException(
                    "HLL_UNION, HLL_UNION_AGG, HLL_RAW_AGG and HLL_CARDINALITY's params must be hll column");
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
                || fnName.getFunction().equalsIgnoreCase("sm4_encrypt")
                || fnName.getFunction().equalsIgnoreCase("aes_decrypt_v2")
                || fnName.getFunction().equalsIgnoreCase("aes_encrypt_v2")
                || fnName.getFunction().equalsIgnoreCase("sm4_decrypt_v2")
                || fnName.getFunction().equalsIgnoreCase("sm4_encrypt_v2"))
                && (children.size() == 2 || children.size() == 3)) {
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
                        || fnName.getFunction().equalsIgnoreCase("aes_encrypt")
                        || fnName.getFunction().equalsIgnoreCase("aes_decrypt_v2")
                        || fnName.getFunction().equalsIgnoreCase("aes_encrypt_v2")) {
                    if (StringUtils.isAllBlank(blockEncryptionMode)) {
                        blockEncryptionMode = "AES_128_ECB";
                    }
                    if (!aesModes.contains(blockEncryptionMode.toUpperCase())) {
                        throw new AnalysisException("session variable block_encryption_mode is invalid with aes");
                    }
                    if (children.size() == 2) {
                        boolean isECB = blockEncryptionMode.equalsIgnoreCase("AES_128_ECB")
                                || blockEncryptionMode.equalsIgnoreCase("AES_192_ECB")
                                || blockEncryptionMode.equalsIgnoreCase("AES_256_ECB");
                        if (fnName.getFunction().equalsIgnoreCase("aes_decrypt_v2")) {
                            if (!isECB) {
                                throw new AnalysisException(
                                        "Incorrect parameter count in the call to native function 'aes_decrypt'");
                            }
                        } else if (fnName.getFunction().equalsIgnoreCase("aes_encrypt_v2")) {
                            if (!isECB) {
                                throw new AnalysisException(
                                        "Incorrect parameter count in the call to native function 'aes_encrypt'");
                            }
                        } else {
                            // if there are only 2 params, we need set encryption mode to AES_128_ECB
                            // this keeps the behavior consistent with old doris ver.
                            blockEncryptionMode = "AES_128_ECB";
                        }
                    }
                }
                if (fnName.getFunction().equalsIgnoreCase("sm4_decrypt")
                        || fnName.getFunction().equalsIgnoreCase("sm4_encrypt")
                        || fnName.getFunction().equalsIgnoreCase("sm4_decrypt_v2")
                        || fnName.getFunction().equalsIgnoreCase("sm4_encrypt_v2")) {
                    if (StringUtils.isAllBlank(blockEncryptionMode)) {
                        blockEncryptionMode = "SM4_128_ECB";
                    }
                    if (!sm4Modes.contains(blockEncryptionMode.toUpperCase())) {
                        throw new AnalysisException(
                                "session variable block_encryption_mode is invalid with sm4");
                    }
                    if (children.size() == 2) {
                        if (fnName.getFunction().equalsIgnoreCase("sm4_decrypt_v2")) {
                            throw new AnalysisException(
                                    "Incorrect parameter count in the call to native function 'sm4_decrypt'");
                        } else if (fnName.getFunction().equalsIgnoreCase("sm4_encrypt_v2")) {
                            throw new AnalysisException(
                                    "Incorrect parameter count in the call to native function 'sm4_encrypt'");
                        } else {
                            // if there are only 2 params, we need add an empty string as the third param
                            // and set encryption mode to SM4_128_ECB
                            // this keeps the behavior consistent with old doris ver.
                            children.add(new StringLiteral(""));
                            blockEncryptionMode = "SM4_128_ECB";
                        }
                    }
                }
            }
            if (!blockEncryptionMode.equals(children.get(children.size() - 1).toString())) {
                children.add(new StringLiteral(blockEncryptionMode));
            }

            if (fnName.getFunction().equalsIgnoreCase("aes_decrypt_v2")) {
                fnName = FunctionName.createBuiltinName("aes_decrypt");
            } else if (fnName.getFunction().equalsIgnoreCase("aes_encrypt_v2")) {
                fnName = FunctionName.createBuiltinName("aes_encrypt");
            } else if (fnName.getFunction().equalsIgnoreCase("sm4_decrypt_v2")) {
                fnName = FunctionName.createBuiltinName("sm4_decrypt");
            } else if (fnName.getFunction().equalsIgnoreCase("sm4_encrypt_v2")) {
                fnName = FunctionName.createBuiltinName("sm4_encrypt");
            }
        }
    }

    private void analyzeArrayFunction(Analyzer analyzer) throws AnalysisException {
        if (fnName.getFunction().equalsIgnoreCase("array_distinct")
                || fnName.getFunction().equalsIgnoreCase("array_max")
                || fnName.getFunction().equalsIgnoreCase("array_min")
                || fnName.getFunction().equalsIgnoreCase("array_sum")
                || fnName.getFunction().equalsIgnoreCase("array_avg")
                || fnName.getFunction().equalsIgnoreCase("array_product")
                || fnName.getFunction().equalsIgnoreCase("array_union")
                || fnName.getFunction().equalsIgnoreCase("array_except")
                || fnName.getFunction().equalsIgnoreCase("array_cum_sum")
                || fnName.getFunction().equalsIgnoreCase("array_intersect")
                || fnName.getFunction().equalsIgnoreCase("arrays_overlap")
                || fnName.getFunction().equalsIgnoreCase("array_concat")
                || fnName.getFunction().equalsIgnoreCase("array")) {
            Type[] childTypes = collectChildReturnTypes();
            Type compatibleType = childTypes[0];
            for (int i = 1; i < childTypes.length; ++i) {
                compatibleType = Type.getAssignmentCompatibleType(compatibleType, childTypes[i], true);
                if (compatibleType == Type.INVALID) {
                    throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
                }
            }
            // Make sure BE doesn't see any TYPE_NULL exprs
            if (compatibleType.isNull()) {
                compatibleType = Type.BOOLEAN;
            }
            for (int i = 0; i < childTypes.length; i++) {
                uncheckedCastChild(compatibleType, i);
            }
        } else if (fnName.getFunction().equalsIgnoreCase("array_exists")) {
            Type[] newArgTypes = new Type[1];
            if (!(getChild(0) instanceof CastExpr)) {
                Expr castExpr = getChild(0).castTo(ArrayType.create(Type.BOOLEAN, true));
                this.setChild(0, castExpr);
                newArgTypes[0] = castExpr.getType();
            }

            fn = getBuiltinFunction(fnName.getFunction(), newArgTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (fn == null) {
                LOG.warn("fn {} not exists", this.toSqlImpl());
                throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
            }
            fn.setReturnType(getChild(0).getType());
        } else if (fnName.getFunction().equalsIgnoreCase("array_position")
                || fnName.getFunction().equalsIgnoreCase("array_contains")
                || fnName.getFunction().equalsIgnoreCase("countequal")) {
            // make nested type with function param can be Compatible otherwise be will not deal with type
            Type[] childTypes = collectChildReturnTypes();
            if (childTypes[0].isNull()) {
                childTypes[0] = new ArrayType(Type.NULL);
            }
            Type compatibleType = ((ArrayType) childTypes[0]).getItemType();
            for (int i = 1; i < childTypes.length; ++i) {
                compatibleType = Type.getAssignmentCompatibleType(compatibleType, childTypes[i], true);
                if (compatibleType == Type.INVALID) {
                    throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
                }
                uncheckedCastChild(compatibleType, i);
            }
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
        fn = new Function(getBuiltinFunction(fnName.getFunction(), new Type[0],
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));
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
            Preconditions.checkNotNull(fn);
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

        analyzeArrayFunction(analyzer);

        if (fnName.getFunction().equalsIgnoreCase("sum")) {
            if (this.children.isEmpty()) {
                throw new AnalysisException("The " + fnName + " function must has one input param");
            }
            // Prevent the cast type in vector exec engine
            Type type = getChild(0).type;
            fn = getBuiltinFunction(fnName.getFunction(), new Type[] { type },
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

            fn = getBuiltinFunction(fnName.getFunction(), new Type[] { compatibleType },
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if (fnName.getFunction().equalsIgnoreCase(FunctionSet.WINDOW_FUNNEL)) {
            if (fnParams.exprs() == null || fnParams.exprs().size() < 4) {
                throw new AnalysisException("The " + fnName + " function must have at least four params");
            }

            if (!children.get(0).type.isIntegerType()) {
                throw new AnalysisException("The window params of " + fnName + " function must be integer");
            }
            if (!children.get(1).type.isStringType()) {
                throw new AnalysisException("The mode params of " + fnName + " function must be string");
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
        } else if (fnName.getFunction().equalsIgnoreCase(FunctionSet.SEQUENCE_MATCH)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.SEQUENCE_COUNT)) {
            if (fnParams.exprs() == null || fnParams.exprs().size() < 4) {
                throw new AnalysisException("The " + fnName + " function must have at least four params");
            }
            if (!children.get(0).type.isStringType()) {
                throw new AnalysisException("The pattern params of " + fnName + " function must be string");
            }
            if (!children.get(1).type.isDateType()) {
                throw new AnalysisException("The timestamp params of " + fnName + " function must be DATE or DATETIME");
            }
            String pattern = children.get(0).toSql();
            int patternLength = pattern.length();
            pattern = pattern.substring(1, patternLength - 1);
            if (!parsePattern(pattern)) {
                throw new AnalysisException("The format of pattern params is wrong");
            }

            Type[] childTypes = new Type[children.size()];
            for (int i = 0; i < 2; i++) {
                childTypes[i] = children.get(i).type;
            }
            for (int i = 2; i < children.size(); i++) {
                if (children.get(i).type != Type.BOOLEAN) {
                    throw new AnalysisException("The 3th and subsequent params of "
                            + fnName + " function must be boolean");
                }
                childTypes[i] = children.get(i).type;
            }
            fn = getBuiltinFunction(fnName.getFunction(), childTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if (fnName.getFunction().equalsIgnoreCase("if")) {
            Type[] childTypes = collectChildReturnTypes();
            Type assignmentCompatibleType = ScalarType.getAssignmentCompatibleType(childTypes[1], childTypes[2], true);
            if (assignmentCompatibleType.isDecimalV3()) {
                if (assignmentCompatibleType.isDecimalV3() && !childTypes[1].equals(assignmentCompatibleType)) {
                    uncheckedCastChild(assignmentCompatibleType, 1);
                }
                if (assignmentCompatibleType.isDecimalV3() && !childTypes[2].equals(assignmentCompatibleType)) {
                    uncheckedCastChild(assignmentCompatibleType, 2);
                }
            }
            childTypes[0] = Type.BOOLEAN;
            childTypes[1] = assignmentCompatibleType;
            childTypes[2] = assignmentCompatibleType;

            if (childTypes[1].isDecimalV3() && childTypes[2].isDecimalV3()) {
                argTypes[1] = assignmentCompatibleType;
                argTypes[2] = assignmentCompatibleType;
            }
            fn = getBuiltinFunction(fnName.getFunction(), childTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (assignmentCompatibleType.isDatetimeV2()) {
                fn.setReturnType(assignmentCompatibleType);
            }

        } else if (fnName.getFunction().equalsIgnoreCase("ifnull")) {
            Type[] childTypes = collectChildReturnTypes();
            Type assignmentCompatibleType = ScalarType.getAssignmentCompatibleType(childTypes[0], childTypes[1], true);
            if (assignmentCompatibleType != Type.INVALID) {
                if (assignmentCompatibleType.isDecimalV3()) {
                    if (assignmentCompatibleType.isDecimalV3() && !childTypes[0].equals(assignmentCompatibleType)) {
                        uncheckedCastChild(assignmentCompatibleType, 0);
                    }
                    if (assignmentCompatibleType.isDecimalV3() && !childTypes[1].equals(assignmentCompatibleType)) {
                        uncheckedCastChild(assignmentCompatibleType, 1);
                    }
                }
                childTypes[0] = assignmentCompatibleType;
                childTypes[1] = assignmentCompatibleType;

                if (childTypes[1].isDecimalV3() && childTypes[0].isDecimalV3()) {
                    argTypes[1] = assignmentCompatibleType;
                    argTypes[0] = assignmentCompatibleType;
                }
            }
            fn = getBuiltinFunction(fnName.getFunction(), childTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if ((fnName.getFunction().equalsIgnoreCase("coalesce")
                || fnName.getFunction().equalsIgnoreCase("least")
                || fnName.getFunction().equalsIgnoreCase("greatest")) && children.size() > 1) {
            Type[] childTypes = collectChildReturnTypes();
            Type assignmentCompatibleType = childTypes[0];
            for (int i = 1; i < childTypes.length; i++) {
                assignmentCompatibleType = ScalarType
                        .getAssignmentCompatibleType(assignmentCompatibleType, childTypes[i], true);
            }
            if (assignmentCompatibleType.isDecimalV3()) {
                for (int i = 0; i < childTypes.length; i++) {
                    if (assignmentCompatibleType.isDecimalV3()
                            && !childTypes[i].equals(assignmentCompatibleType)) {
                        uncheckedCastChild(assignmentCompatibleType, i);
                        argTypes[i] = assignmentCompatibleType;
                    }
                }
            }
            fn = getBuiltinFunction(fnName.getFunction(), argTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if (fnName.getFunction().equalsIgnoreCase("array_apply")
                && ((ArrayType) children.get(0).getType()).getItemType().isDecimalV3()) {
            uncheckedCastChild(((ArrayType) children.get(0).getType()).getItemType(), 2);
            fn = getBuiltinFunction(fnName.getFunction(), collectChildReturnTypes(),
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if (AggregateFunction.SUPPORT_ORDER_BY_AGGREGATE_FUNCTION_NAME_SET.contains(
                fnName.getFunction().toLowerCase())) {
            // order by elements add as child like windows function. so if we get the
            // param of arg, we need remove the order by elements
            Type[] childTypes = collectChildReturnTypes();
            Type[] newChildTypes = new Type[children.size() - orderByElements.size()];
            System.arraycopy(childTypes, 0, newChildTypes, 0, newChildTypes.length);
            fn = getBuiltinFunction(fnName.getFunction(), newChildTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if (STDDEV_FUNCTION_SET.contains(fnName.getFunction().toLowerCase())
                && collectChildReturnTypes()[0].isDecimalV3()) {
            Type[] childrenTypes = collectChildReturnTypes();
            Type[] args = new Type[childrenTypes.length];
            args[0] = Type.DOUBLE;
            System.arraycopy(childrenTypes, 1, args, 1, childrenTypes.length - 1);
            fn = getBuiltinFunction(fnName.getFunction(), args, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
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
                if (ROUND_FUNCTION_SET.contains(fnName.getFunction()) && children.size() == 2
                        && children.get(0).getType().isDecimalV3() && children.get(1) instanceof IntLiteral) {
                    children.get(1).setType(Type.INT);
                }
                // now first find function in built-in functions
                if (Strings.isNullOrEmpty(fnName.getDb())) {
                    Type[] childTypes = collectChildReturnTypes();
                    fn = getBuiltinFunction(fnName.getFunction(), childTypes,
                            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                }

                // find user defined functions
                if (fn == null) {
                    fn = findUdf(fnName, analyzer);
                    if (analyzer.isReAnalyze() && fn instanceof AliasFunction) {
                        throw new AnalysisException("a UDF in the original function of a alias function");
                    }
                }
            }
        }

        if (fn == null) {
            LOG.warn("fn {} not exists", this.toSqlImpl());
            throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
        }

        if (fnName.getFunction().equalsIgnoreCase("collect_list")
                || fnName.getFunction().equalsIgnoreCase("collect_set")
                || fnName.getFunction().equalsIgnoreCase("array_agg")) {
            fn.setReturnType(new ArrayType(getChild(0).type));
        }

        if (fnName.getFunction().equalsIgnoreCase("map_agg")) {
            fn.setReturnType(new MapType(getChild(0).type, getChild(1).type));
        }

        if (fnName.getFunction().equalsIgnoreCase("group_uniq_array")
                || fnName.getFunction().equalsIgnoreCase("group_array")) {
            fn.setReturnType(new ArrayType(getChild(0).type));
        }

        if (fnName.getFunction().equalsIgnoreCase("from_unixtime")
                || fnName.getFunction().equalsIgnoreCase("date_format")
                || fnName.getFunction().equalsIgnoreCase("unix_timestamp")) {
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
        if (fnName.getFunction().equalsIgnoreCase("convert_to")) {
            if (children.size() < 2 || !getChild(1).isConstant()) {
                throw new AnalysisException(
                        fnName.getFunction() + " needs two params, and the second is must be a constant: " + this
                                .toSql());
            }
        }
        if (fnName.getFunction().equalsIgnoreCase("date_trunc")) {
            if ((children.size() != 2) || (getChild(1).isConstant() == false)
                    || !(getChild(1) instanceof StringLiteral)) {
                throw new AnalysisException(
                        fnName.getFunction() + " needs two params, and the second is must be a string constant: "
                                + this.toSql());
            }
            final String constParam = ((StringLiteral) getChild(1)).getValue().toLowerCase();
            if (!Lists.newArrayList("year", "quarter", "month", "week", "day", "hour", "minute", "second")
                    .contains(constParam)) {
                throw new AnalysisException("date_trunc function second param only support argument is "
                        + "year|quarter|month|week|day|hour|minute|second");
            }
        }
        if (fnName.getFunction().equalsIgnoreCase("char")) {
            if (!getChild(0).isConstant()) {
                throw new AnalysisException(
                        fnName.getFunction() + " charset name must be a constant: " + this
                                .toSql());
            }
            LiteralExpr literal = (LiteralExpr) getChild(0);
            if (!literal.getStringValue().equalsIgnoreCase("utf8")) {
                throw new AnalysisException(
                        fnName.getFunction() + " function currently only support charset name 'utf8': " + this
                                .toSql());
            }
        }
        if (fn.getFunctionName().getFunction().equals("timediff")) {
            fn.getReturnType().getPrimitiveType().setTimeType();
            ScalarType left = (ScalarType) argTypes[0];
            ScalarType right = (ScalarType) argTypes[1];
            if (left.isDatetimeV2() || right.isDatetimeV2() || left.isDateV2() || right.isDateV2()) {
                Type ret = ScalarType.createTimeV2Type(Math.max(left.getScalarScale(), right.getScalarScale()));
                fn.setReturnType(ret);
                fn.getReturnType().getPrimitiveType().setTimeType();
            }
        }

        if (fn.getFunctionName().getFunction().equals("from_microsecond")) {
            Type ret = ScalarType.createDatetimeV2Type(6);
            fn.setReturnType(ret);
        }

        if (fn.getFunctionName().getFunction().equals("from_millisecond")) {
            Type ret = ScalarType.createDatetimeV2Type(3);
            fn.setReturnType(ret);
        }

        if (fn.getFunctionName().getFunction().equals("from_second")) {
            Type ret = ScalarType.createDatetimeV2Type(0);
            fn.setReturnType(ret);
        }

        if (fnName.getFunction().equalsIgnoreCase("map")) {
            if ((children.size() & 1) == 1) {
                throw new AnalysisException("map can't be odd parameters, need even parameters: "
                        + this.toSql());
            }
        }

        if (fnName.getFunction().equalsIgnoreCase("named_struct")) {
            if ((children.size() & 1) == 1) {
                throw new AnalysisException("named_struct can't be odd parameters, need even parameters: "
                        + this.toSql());
            }
            for (int i = 0; i < children.size(); i++) {
                if ((i & 1) == 0) {
                    if (!(getChild(i) instanceof StringLiteral)) {
                        throw new AnalysisException(
                                "named_struct only allows constant string parameter in odd position: " + this.toSql());
                    }
                }
            }
        }

        if (fn.getFunctionName().getFunction().equals("struct_element")) {
            if (children.size() < 2) {
                throw new AnalysisException(fnName.getFunction() + " needs two parameters: " + this.toSql());
            }
            if (getChild(0).type instanceof StructType) {
                StructType s = ((StructType) children.get(0).type);
                if (getChild(1) instanceof StringLiteral) {
                    String fieldName = children.get(1).getStringValue();
                    if (s.getField(fieldName) == null) {
                        throw new AnalysisException(
                                "the specified field name " + fieldName + " was not found: " + this.toSql());
                    }
                } else if (getChild(1) instanceof IntLiteral) {
                    int pos = (int) ((IntLiteral) children.get(1)).getValue();
                    if (pos < 1 || pos > s.getFields().size()) { // the index start from 1
                        throw new AnalysisException(
                                "the specified field index out of bound: " + this.toSql());
                    }
                } else {
                    throw new AnalysisException(
                            "struct_element only allows constant int or string second parameter: " + this.toSql());
                }
            }
        }

        if (fn.getFunctionName().getFunction().equals("sha2")) {
            if ((children.size() != 2) || (getChild(1).isConstant() == false)
                    || !(getChild(1) instanceof IntLiteral)) {
                throw new AnalysisException(
                        fnName.getFunction() + " needs two params, and the second is must be a integer constant: "
                                + this.toSql());
            }
            final Integer constParam = (int) ((IntLiteral) getChild(1)).getValue();
            if (!Lists.newArrayList(224, 256, 384, 512).contains(constParam)) {
                throw new AnalysisException("sha2 functions only support digest length of 224/256/384/512");
            }
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

        Type[] args = fn.getArgs();
        if (args.length > 0) {
            // Implicitly cast all the children to match the function if necessary
            for (int i = 0; i < argTypes.length - orderByElements.size(); ++i) {
                // For varargs, we must compare with the last type in callArgs.argTypes.
                int ix = Math.min(args.length - 1, i);
                // map varargs special case map(key_type, value_type, ...)
                if (i >= args.length && i >= 2 && args.length >= 2
                        && fnName.getFunction().equalsIgnoreCase("map")) {
                    ix = i % 2 == 0 ? 0 : 1;
                }

                if (i == 0 && (fnName.getFunction().equalsIgnoreCase("char"))) {
                    continue;
                }

                if ((fnName.getFunction().equalsIgnoreCase("money_format") || fnName.getFunction()
                        .equalsIgnoreCase("histogram")
                        || fnName.getFunction().equalsIgnoreCase("hist"))
                        && children.get(0).getType().isDecimalV3() && args[ix].isDecimalV3()) {
                    continue;
                } else if ((fnName.getFunction().equalsIgnoreCase("array_min") || fnName.getFunction()
                        .equalsIgnoreCase("array_max") || fnName.getFunction().equalsIgnoreCase("element_at"))
                        && ((
                        children.get(0).getType().isDecimalV3() && ((ArrayType) args[ix]).getItemType()
                                .isDecimalV3())
                        || (children.get(0).getType().isDatetimeV2()
                        && ((ArrayType) args[ix]).getItemType().isDatetimeV2())
                        || (children.get(0).getType().isDecimalV2()
                        && ((ArrayType) args[ix]).getItemType().isDecimalV2()))) {
                    continue;
                } else if ((fnName.getFunction().equalsIgnoreCase("array")
                        || fnName.getFunction().equalsIgnoreCase("array_distinct")
                        || fnName.getFunction().equalsIgnoreCase("array_remove")
                        || fnName.getFunction().equalsIgnoreCase("array_sort")
                        || fnName.getFunction().equalsIgnoreCase("array_reverse_sort")
                        || fnName.getFunction().equalsIgnoreCase("array_overlap")
                        || fnName.getFunction().equalsIgnoreCase("array_union")
                        || fnName.getFunction().equalsIgnoreCase("array_intersect")
                        || fnName.getFunction().equalsIgnoreCase("array_compact")
                        || fnName.getFunction().equalsIgnoreCase("array_slice")
                        || fnName.getFunction().equalsIgnoreCase("array_popback")
                        || fnName.getFunction().equalsIgnoreCase("array_popfront")
                        || fnName.getFunction().equalsIgnoreCase("array_pushfront")
                        || fnName.getFunction().equalsIgnoreCase("array_pushback")
                        || fnName.getFunction().equalsIgnoreCase("array_cum_sum")
                        || fnName.getFunction().equalsIgnoreCase("reverse")
                        || fnName.getFunction().equalsIgnoreCase("%element_slice%")
                        || fnName.getFunction().equalsIgnoreCase("array_concat")
                        || fnName.getFunction().equalsIgnoreCase("array_shuffle")
                        || fnName.getFunction().equalsIgnoreCase("shuffle")
                        || fnName.getFunction().equalsIgnoreCase("array_except")
                        || fnName.getFunction().equalsIgnoreCase("width_bucket"))
                        && (args[ix].isDecimalV3() || (children.get(0).getType().isArrayType()
                        && (((ArrayType) children.get(0).getType()).getItemType().isDecimalV3())
                        && (args[ix].isArrayType())
                        && ((ArrayType) args[ix]).getItemType().isDecimalV3()))) {
                    continue;
                } else if (!argTypes[i].matchesType(args[ix])
                        && ROUND_FUNCTION_SET.contains(fnName.getFunction())
                        && ConnectContext.get() != null
                        && ConnectContext.get().getSessionVariable().roundPreciseDecimalV2Value
                        && argTypes[i].isDecimalV2()
                        && args[ix].isDecimalV3()) {
                    uncheckedCastChild(ScalarType.createDecimalV3Type(ScalarType.MAX_DECIMALV2_PRECISION,
                            ((ScalarType) argTypes[i]).getScalarScale()), i);
                } else if (!argTypes[i].matchesType(args[ix])
                        && !(argTypes[i].isDecimalV3OrContainsDecimalV3()
                        && args[ix].isDecimalV3OrContainsDecimalV3())) {
                    // Do not do this cast if types are both decimalv3 with different precision/scale.
                    uncheckedCastChild(args[ix], i);
                } else if (fnName.getFunction().equalsIgnoreCase("if")
                        && argTypes[i].isArrayType() && ((ArrayType) argTypes[i]).getItemType().isNull()) {
                    uncheckedCastChild(args[ix], i);
                }
            }
        }

        /**
         * The return type of str_to_date depends on whether the time part is included
         * in the format.
         * If included, it is datetime, otherwise it is date.
         * If the format parameter is not constant, the return type will be datetime.
         * The above judgment has been completed in the FE query planning stage,
         * so here we directly set the value type to the return type set in the query
         * plan.
         *
         * For example:
         * A table with one column k1 varchar, and has 2 lines:
         * "%Y-%m-%d"
         * "%Y-%m-%d %H:%i:%s"
         * Query:
         * SELECT str_to_date("2020-09-01", k1) from tbl;
         * Result will be:
         * 2020-09-01 00:00:00
         * 2020-09-01 00:00:00
         *
         * Query:
         * SELECT str_to_date("2020-09-01", "%Y-%m-%d");
         * Return type is DATE
         *
         * Query:
         * SELECT str_to_date("2020-09-01", "%Y-%m-%d %H:%i:%s");
         * Return type is DATETIME
         */
        if (fn.getFunctionName().getFunction().equals("str_to_date")) {
            Expr child1Result = getChild(1).getResultValue(false);
            if (child1Result instanceof StringLiteral) {
                if (DateLiteral.hasTimePart(child1Result.getStringValue())) {
                    this.type = Type.DATETIME;
                } else {
                    this.type = Type.DATE;
                }
            } else {
                this.type = Type.DATETIME;
            }
        } else if (TIME_FUNCTIONS_WITH_PRECISION.contains(fnName.getFunction().toLowerCase())
                && fn.getReturnType().isDatetimeV2()) {
            if (children.size() == 1 && children.get(0) instanceof IntLiteral) {
                this.type = ScalarType.createDatetimeV2Type((int) ((IntLiteral) children.get(0)).getLongValue());
            } else if (children.size() == 1) {
                this.type = ScalarType.createDatetimeV2Type(6);
            }
        } else {
            this.type = fn.getReturnType();
        }

        if (this.type.isDecimalV2()) {
            this.type = Type.MAX_DECIMALV2_TYPE;
            fn.setReturnType(Type.MAX_DECIMALV2_TYPE);
        }

        if (this.type.isDecimalV3() || (this.type.isArrayType()
                && ((ArrayType) this.type).getItemType().isDecimalV3())
                || (this.type.isDatetimeV2()
                && !TIME_FUNCTIONS_WITH_PRECISION.contains(fnName.getFunction().toLowerCase()))) {
            // TODO(gabriel): If type exceeds max precision of DECIMALV3, we should change
            // it to a double function
            this.type = PRECISION_INFER_RULE.getOrDefault(fnName.getFunction(), DEFAULT_PRECISION_INFER_RULE)
                    .apply(children, this.type);
        }

        // cast(xx as char(N)/varchar(N)) will be handled as substr(cast(xx as char, varchar), 1, N),
        // but type is varchar(*), we change it to varchar(N);
        if (fn.getFunctionName().getFunction().equals("substr")
                && children.size() == 3
                && children.get(1) instanceof IntLiteral
                && children.get(2) instanceof IntLiteral) {
            long len = ((IntLiteral) children.get(2)).getValue();
            if (type.isWildcardChar()) {
                this.type = ScalarType.createCharType(((int) (len)));
            } else if (type.isWildcardVarchar()) {
                this.type = ScalarType.createVarchar(((int) (len)));
            }
        }
        // rewrite return type if is nested type function
        analyzeNestedFunction();
        for (OrderByElement o : orderByElements) {
            if (!o.getExpr().isAnalyzed) {
                o.getExpr().analyzeImpl(analyzer);
            }
        }
    }

    // if return type is nested type, need to be determined the sub-element type
    private void analyzeNestedFunction() {
        // array
        if (fnName.getFunction().equalsIgnoreCase("array")) {
            if (children.size() > 0) {
                this.type = new ArrayType(children.get(0).getType());
            }
        } else if (fnName.getFunction().equalsIgnoreCase("map")) {
            if (children.size() > 1) {
                this.type = new MapType(children.get(0).getType(), children.get(1).getType());
            }
        } else if (fnName.getFunction().equalsIgnoreCase("if")) {
            if (children.get(1).getType().isArrayType() && (
                    ((ArrayType) children.get(1).getType()).getItemType().isDecimalV3()
                            || ((ArrayType) children.get(1)
                            .getType()).getItemType().isDecimalV2() || ((ArrayType) children.get(1)
                            .getType()).getItemType().isDatetimeV2())) {
                this.type = children.get(1).getType();
            }
        } else if (fnName.getFunction().equalsIgnoreCase("named_struct")) {
            ArrayList<StructField> newFields = Lists.newArrayList();
            ArrayList<StructField> originFields = ((StructType) type).getFields();
            for (int i = 0; i < children.size() && i + 1 < children.size(); i += 2) {
                Type fieldType = originFields.get(i + i >> 2).getType();
                if (fieldType.isDecimalV3() || fieldType.isDatetimeV2()) {
                    fieldType = children.get(i + 1).type;
                }
                StringLiteral nameLiteral = (StringLiteral) children.get(i);
                newFields.add(new StructField(nameLiteral.getStringValue(), fieldType));
            }
            this.type = new StructType(newFields);
        } else if (fnName.getFunction().equalsIgnoreCase("struct")) {
            ArrayList<StructField> newFields = Lists.newArrayList();
            ArrayList<StructField> originFields = ((StructType) type).getFields();
            for (int i = 0; i < children.size(); i++) {
                Type fieldType = originFields.get(i).getType();
                if (originFields.get(i).getType().isDecimalV3() || originFields.get(i).getType().isDatetimeV2()) {
                    fieldType = children.get(i).type;
                }
                newFields.add(new StructField(fieldType));
            }
            this.type = new StructType(newFields);
        } else if (fnName.getFunction().equalsIgnoreCase("topn_array")) {
            this.type = new ArrayType(children.get(0).getType());
        } else if (fnName.getFunction().equalsIgnoreCase("struct_element")) {
            if (children.get(1) instanceof StringLiteral) {
                String fieldName = children.get(1).getStringValue();
                fn.setReturnType(((StructType) children.get(0).type).getField(fieldName).getType());
            } else if (children.get(1) instanceof IntLiteral) {
                int pos = (int) ((IntLiteral) children.get(1)).getValue();
                fn.setReturnType(((StructType) children.get(0).type).getFields().get(pos - 1).getType());
            }
            this.type = fn.getReturnType();
        } else if (fnName.getFunction().equalsIgnoreCase("array_distinct") || fnName.getFunction()
                .equalsIgnoreCase("array_remove") || fnName.getFunction().equalsIgnoreCase("array_sort")
                || fnName.getFunction().equalsIgnoreCase("array_reverse_sort")
                || fnName.getFunction().equalsIgnoreCase("array_overlap")
                || fnName.getFunction().equalsIgnoreCase("array_union")
                || fnName.getFunction().equalsIgnoreCase("array_intersect")
                || fnName.getFunction().equalsIgnoreCase("array_compact")
                || fnName.getFunction().equalsIgnoreCase("array_slice")
                || fnName.getFunction().equalsIgnoreCase("array_popback")
                || fnName.getFunction().equalsIgnoreCase("array_popfront")
                || fnName.getFunction().equalsIgnoreCase("array_pushfront")
                || fnName.getFunction().equalsIgnoreCase("array_pushback")
                || fnName.getFunction().equalsIgnoreCase("reverse")
                || fnName.getFunction().equalsIgnoreCase("%element_slice%")
                || fnName.getFunction().equalsIgnoreCase("array_shuffle")
                || fnName.getFunction().equalsIgnoreCase("shuffle")
                || fnName.getFunction().equalsIgnoreCase("array_except")
                || fnName.getFunction().equalsIgnoreCase("array_concat")
                || fnName.getFunction().equalsIgnoreCase("array_apply")) {
            if (children.size() > 0) {
                this.type = children.get(0).getType();
            }
        } else if (fnName.getFunction().equalsIgnoreCase("array_zip")) {
            // collect the child types to make a STRUCT type
            Type[] childTypes = collectChildReturnTypes();
            ArrayList<StructField> fields = new ArrayList<>();

            for (int i = 0; i < childTypes.length; i++) {
                fields.add(new StructField(((ArrayType) childTypes[i]).getItemType()));
            }

            this.type = new ArrayType(new StructType(fields));
        }

        if (this.type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            // Now Array type do not support ARRAY<NOT_NULL>, set it to true temporarily
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

    /**
     * rewrite alias function to real function
     * reset function name, function params and it's children to real function's
     *
     * @return
     * @throws AnalysisException
     */
    public Expr rewriteExpr(Analyzer analyzer) throws AnalysisException {
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

        // replace origin function params exprs' with input params expr depending on
        // parameter name
        for (int i = 0; i < oriParamsExprs.size(); i++) {
            Expr expr = replaceParams(parameters, inputParamsExprs, oriParamsExprs.get(i));
            oriParamsExprs.set(i, expr);
        }

        retExpr.fnParams = new FunctionParams(oriExpr.fnParams.isDistinct(), oriParamsExprs);

        // retExpr changed to original function, should be analyzed again.
        retExpr.fn = null;

        // reset children
        retExpr.children.clear();
        retExpr.children.addAll(oriExpr.getChildren());
        retExpr.isRewrote = true;
        retExpr.analyze(analyzer);
        return retExpr;
    }

    /**
     * replace origin function expr and it's children with input params exprs
     * depending on parameter name
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
        // Initialize literalExpr without type information, because literalExpr does not
        // save type information
        // when it is persisted, so after fe restart, read the image,
        // it will be missing type and report an error during analyze.
        if (oriExpr instanceof LiteralExpr && oriExpr.getType().equals(Type.INVALID)) {
            oriExpr = LiteralExpr.init((LiteralExpr) oriExpr);
        }
        return oriExpr;
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

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public void setOrderByElements(List<OrderByElement> orderByElements) {
        this.orderByElements = orderByElements;
    }

    private void setChildren() {
        orderByElements.forEach(o -> addChild(o.getExpr()));
    }

    @Override
    public boolean haveFunction(String functionName) {
        if (fnName.toString().equalsIgnoreCase(functionName)) {
            return true;
        }
        return super.haveFunction(functionName);
    }

    public Function findUdf(FunctionName fnName, Analyzer analyzer) throws AnalysisException {
        if (!analyzer.isUDFAllowed()) {
            throw new AnalysisException(
                    "Does not support non-builtin functions, or function does not exist: "
                            + this.toSqlImpl());
        }

        Function fn = null;
        String dbName = fnName.analyzeDb(analyzer);
        if (!Strings.isNullOrEmpty(dbName)) {
            // check operation privilege
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.SELECT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SELECT");
            }
            // TODO(gaoxin): ExternalDatabase not implement udf yet.
            DatabaseIf db = Env.getCurrentEnv().getInternalCatalog().getDbNullable(dbName);
            if (db != null && (db instanceof Database)) {
                Function searchDesc = new Function(fnName, Arrays.asList(collectChildReturnTypes()),
                        Type.INVALID, false);
                fn = ((Database) db).getFunction(searchDesc,
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            }
        }
        // find from the internal database first, if not, then from the global functions
        if (fn == null) {
            Function searchDesc =
                    new Function(fnName, Arrays.asList(collectChildReturnTypes()), Type.INVALID, false);
            fn = Env.getCurrentEnv().getGlobalFunctionMgr().getFunction(searchDesc,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        }
        return fn;
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

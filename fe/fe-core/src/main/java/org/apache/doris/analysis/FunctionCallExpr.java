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
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.normalize.Normalizer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.text.StringCharacterIterator;
import java.util.List;
import java.util.Optional;

// TODO: for aggregations, we need to unify the code paths for builtins and UDAs.
public class FunctionCallExpr extends Expr {

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

    public FunctionCallExpr(String fnName, FunctionParams params) {
        this(new FunctionName(fnName), params, false);
    }

    public FunctionCallExpr(FunctionName fnName, FunctionParams params) {
        this(fnName, params, false);
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
        if (this instanceof FunctionCallExpr && ((FunctionCallExpr) this).isAggregateFunction()
                || isAnalyticFnCall) {
            msg.node_type = TExprNodeType.AGG_EXPR;
            if (aggFnParams == null) {
                aggFnParams = fnParams;
            }
            msg.setAggExpr(aggFnParams.createTAggregateExpr(isMergeAggFn));
        } else {
            msg.node_type = TExprNodeType.FUNCTION_CALL;
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
}

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
import org.apache.doris.catalog.FunctionName;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.text.StringCharacterIterator;
import java.util.List;

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

    public void setIsAnalyticFnCall(boolean v) {
        isAnalyticFnCall = v;
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

    public boolean isAnalyticFnCall() {
        return isAnalyticFnCall;
    }

    public FunctionParams getAggFnParams() {
        return aggFnParams;
    }

    public boolean isMergeAggFn() {
        return isMergeAggFn;
    }

    // only used restore from readFields.
    protected FunctionCallExpr() {
        super();
    }

    public FunctionCallExpr(String functionName, List<Expr> params, boolean nullable) {
        super();
        this.fnName = new FunctionName(functionName);
        fnParams = new FunctionParams(params);
        this.isMergeAggFn = false;
        if (fnParams.exprs() != null) {
            children.addAll(fnParams.exprs());
        }
        originChildSize = children.size();
        this.nullable = nullable;
    }

    // nereids scalar function call expr constructor without finalize/analyze
    public FunctionCallExpr(Function function, FunctionParams functionParams, boolean nullable) {
        this(function, functionParams, null, false, functionParams.exprs(), nullable);
    }

    // nereids aggregate function call expr constructor without finalize/analyze
    public FunctionCallExpr(Function function, FunctionParams functionParams, FunctionParams aggFnParams,
            boolean isMergeAggFn, List<Expr> children, boolean nullable) {
        this.fnName = function.getFunctionName();
        this.fn = function;
        this.type = function.getReturnType();
        this.fnParams = functionParams;
        this.aggFnParams = aggFnParams;
        this.children.addAll(children);
        this.originChildSize = children.size();
        this.isMergeAggFn = isMergeAggFn;
        this.nullable = nullable;
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

    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitFunctionCallExpr(this, context);
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
}

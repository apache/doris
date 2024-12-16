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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/IsNullPredicate.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

public class IsNullPredicate extends Predicate {
    private static final String IS_NULL = "is_null_pred";
    private static final String IS_NOT_NULL = "is_not_null_pred";

    public static void initBuiltins(FunctionSet functionSet) {
        for (Type t : Type.getSupportedTypes()) {
            if (t.isNull()) {
                continue;
            }

            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(IS_NULL, null,
                    Lists.newArrayList(t), Type.BOOLEAN, NullableMode.ALWAYS_NOT_NULLABLE));

            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(IS_NOT_NULL,
                    null, Lists.newArrayList(t), Type.BOOLEAN, NullableMode.ALWAYS_NOT_NULLABLE));
        }
        // for array type
        for (Type complexType : Lists.newArrayList(Type.ARRAY, Type.MAP, Type.GENERIC_STRUCT)) {
            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(IS_NULL, null,
                    Lists.newArrayList(complexType), Type.BOOLEAN, NullableMode.ALWAYS_NOT_NULLABLE));

            functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(IS_NOT_NULL, null,
                    Lists.newArrayList(complexType), Type.BOOLEAN, NullableMode.ALWAYS_NOT_NULLABLE));
        }
    }

    @SerializedName("inn")
    private boolean isNotNull;

    private IsNullPredicate() {
        // use for serde only
    }

    public IsNullPredicate(Expr e, boolean isNotNull) {
        this(e, isNotNull, false);
    }

    /**
     * use for Nereids ONLY
     */
    public IsNullPredicate(Expr e, boolean isNotNull, boolean isNereids) {
        super();
        this.isNotNull = isNotNull;
        Preconditions.checkNotNull(e);
        children.add(e);
        if (isNereids) {
            fn = new Function(new FunctionName(isNotNull ? IS_NOT_NULL : IS_NULL),
                    Lists.newArrayList(e.getType()), Type.BOOLEAN, false, true, NullableMode.ALWAYS_NOT_NULLABLE);
            Preconditions.checkState(fn != null, "tupleisNull fn == NULL");
        }
    }

    protected IsNullPredicate(IsNullPredicate other) {
        super(other);
        this.isNotNull = other.isNotNull;
    }

    public boolean isNotNull() {
        return isNotNull;
    }

    @Override
    public Expr clone() {
        return new IsNullPredicate(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((IsNullPredicate) obj).isNotNull == isNotNull;
    }

    @Override
    public String toSqlImpl() {
        return getChild(0).toSql() + (isNotNull ? " IS NOT NULL" : " IS NULL");
    }

    @Override
    public String toDigestImpl() {
        return getChild(0).toDigest() + (isNotNull ? " IS NOT NULL" : " IS NULL");
    }

    public boolean isSlotRefChildren() {
        return (children.get(0) instanceof SlotRef);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);
        if (isNotNull) {
            fn = getBuiltinFunction(IS_NOT_NULL, collectChildReturnTypes(), Function.CompareMode.IS_INDISTINGUISHABLE);
        } else {
            fn = getBuiltinFunction(IS_NULL, collectChildReturnTypes(), Function.CompareMode.IS_INDISTINGUISHABLE);
        }
        Preconditions.checkState(fn != null, "tupleisNull fn == NULL");

        // determine selectivity
        selectivity = 0.1;
        // LOG.debug(toSql() + " selectivity: " + Double.toString(selectivity));
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.FUNCTION_CALL;
    }

    /**
     * Negates an IsNullPredicate.
     */
    @Override
    public Expr negate() {
        return new IsNullPredicate(getChild(0), !isNotNull);
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    /**
     * fix issue 6390
     */
    @Override
    public Expr getResultValue(boolean forPushDownPredicatesToView) throws AnalysisException {
        // Don't push down predicate to view for is null predicate because the value can contain null
        // after outer join
        recursiveResetChildrenResult(!forPushDownPredicatesToView);
        final Expr childValue = getChild(0);
        if (forPushDownPredicatesToView || !(childValue instanceof LiteralExpr)) {
            return this;
        }
        return childValue instanceof NullLiteral ? new BoolLiteral(!isNotNull) : new BoolLiteral(isNotNull);
    }
}

// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.Function;
import com.baidu.palo.catalog.FunctionSet;
import com.baidu.palo.catalog.PrimitiveType;
import com.baidu.palo.catalog.ScalarFunction;
import com.baidu.palo.catalog.Type;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.thrift.TExprNode;
import com.baidu.palo.thrift.TExprNodeType;
import com.baidu.palo.thrift.TIsNullPredicate;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class IsNullPredicate extends Predicate {
    private static final Logger LOG = LogManager.getLogger(IsNullPredicate.class);
    private static final String IS_NULL = "is_null_pred";
    private static final String IS_NOT_NULL = "is_not_null_pred";

    public static void initBuiltins(FunctionSet functionSet) {
        for (Type t: Type.getSupportedTypes()) {
            if (t.isNull()) continue;
            String isNullSymbol;
            if (t == Type.BOOLEAN) {
                isNullSymbol = "_ZN4palo15IsNullPredicate7is_nullIN8palo_udf10BooleanValE" +
                        "EES3_PNS2_15FunctionContextERKT_";
            } else {
                String udfType = Function.getUdfType(t.getPrimitiveType());
                isNullSymbol = "_ZN4palo15IsNullPredicate7is_nullIN8palo_udf" +
                        udfType.length() + udfType +
                        "EEENS2_10BooleanValEPNS2_15FunctionContextERKT_";
            }

            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    IS_NULL, isNullSymbol, Lists.newArrayList(t), Type.BOOLEAN));

            String isNotNullSymbol = isNullSymbol.replace("7is_null", "11is_not_null");
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    IS_NOT_NULL, isNotNullSymbol, Lists.newArrayList(t), Type.BOOLEAN));
        }
    }


    private final boolean isNotNull;

    public IsNullPredicate(Expr e, boolean isNotNull) {
        super();
        this.isNotNull = isNotNull;
        Preconditions.checkNotNull(e);
        children.add(e);
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
    public String toSql() {
        return getChild(0).toSql() + (isNotNull ? " IS NOT NULL" : " IS NULL");
    }

    public boolean isSlotRefChildren() {
        return (children.get(0) instanceof SlotRef);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);
        if (isNotNull) {
            fn = getBuiltinFunction(
                    analyzer, IS_NOT_NULL, collectChildReturnTypes(), Function.CompareMode.IS_IDENTICAL);
        } else {
            fn = getBuiltinFunction(
                    analyzer, IS_NULL, collectChildReturnTypes(), Function.CompareMode.IS_IDENTICAL);
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

}

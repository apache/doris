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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** Entropy */
public class Entropy extends NullableAggregateFunction implements ExplicitlyCastableSignature {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).varArgs(AnyDataType.INSTANCE_WITHOUT_INDEX)
    );

    public Entropy(Expression arg0, Expression... varArgs) {
        this(false, false, arg0, varArgs);
    }

    public Entropy(boolean distinct, Expression arg0, Expression... varArgs) {
        this(distinct, false, arg0, varArgs);
    }

    public Entropy(boolean distinct, boolean alwaysNullable, Expression arg0, Expression... varArgs) {
        this(distinct, alwaysNullable, false, ExpressionUtils.mergeArguments(arg0, varArgs));
    }

    private Entropy(boolean distinct, boolean alwaysNullable, boolean isSkew, List<Expression> expressions) {
        super("entropy", distinct, alwaysNullable, isSkew, expressions);
    }

    private Entropy(NullableAggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public Entropy withDistinctAndChildren(boolean distinct, List<Expression> children) {
        return new Entropy(getFunctionParams(distinct, children));
    }

    @Override
    public Entropy withAlwaysNullable(boolean alwaysNullable) {
        return new Entropy(getAlwaysNullableFunctionParams(alwaysNullable));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitEntropy(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}

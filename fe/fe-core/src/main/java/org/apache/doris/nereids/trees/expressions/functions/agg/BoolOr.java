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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AggregateFunction 'bool_or'.
 */
public class BoolOr extends NullableAggregateFunction
        implements UnaryExpression, ExplicitlyCastableSignature {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE).args(BooleanType.INSTANCE)
    );

    public BoolOr(Expression child) {
        this(false, false, child);
    }

    private BoolOr(boolean distinct, boolean alwaysNullable, Expression arg) {
        super("bool_or", distinct, alwaysNullable, arg);
    }

    /**
     * constructor for withChildren and reuse signature
     */
    private BoolOr(NullableAggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public BoolOr withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new BoolOr(getFunctionParams(distinct, children));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        DataType argType = child().getDataType();
        if (!(argType.isBooleanType() || argType.isNumericType())) {
            throw new AnalysisException("bool_or requires a boolean or numeric argument");
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBoolOr(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean alwaysNullable) {
        return new BoolOr(getAlwaysNullableFunctionParams(alwaysNullable));
    }
}

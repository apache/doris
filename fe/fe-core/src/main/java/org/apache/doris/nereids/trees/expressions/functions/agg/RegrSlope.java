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
import org.apache.doris.nereids.trees.expressions.functions.window.SupportWindowAnalytic;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AggregateFunction 'regr_slope'.
 */

public class RegrSlope extends NullableAggregateFunction
        implements BinaryExpression, ExplicitlyCastableSignature, SupportWindowAnalytic {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(TinyIntType.INSTANCE, TinyIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(SmallIntType.INSTANCE, SmallIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(IntegerType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(BigIntType.INSTANCE, BigIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(LargeIntType.INSTANCE, LargeIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(DoubleType.INSTANCE, DoubleType.INSTANCE));

    /**
     * Constructor with 2 arguments.
     */
    public RegrSlope(Expression arg1, Expression arg2) {
        this(false, true, arg1, arg2);
    }

    /**
     * Constructor with distinct flag and 2 arguments.
     */
    public RegrSlope(boolean distinct, Expression arg1, Expression arg2) {
        this(distinct, true, arg1, arg2);
    }

    public RegrSlope(boolean distinct, boolean alwaysNullable, Expression arg1, Expression arg2) {
        super("regr_slope", distinct, alwaysNullable, arg1, arg2);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() throws AnalysisException {
        DataType arg0Type = left().getDataType();
        DataType arg1Type = right().getDataType();
        if ((!arg0Type.isNumericType() && !arg0Type.isNullType())
                || arg0Type.isOnlyMetricType()) {
            throw new AnalysisException("regr_slope requires numeric for first parameter: " + toSql());
        } else if ((!arg1Type.isNumericType() && !arg1Type.isNullType())
                || arg1Type.isOnlyMetricType()) {
            throw new AnalysisException("regr_slope requires numeric for second parameter: " + toSql());
        }
    }

    @Override
    public RegrSlope withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new RegrSlope(distinct, alwaysNullable, children.get(0), children.get(1));
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean alwaysNullable) {
        return new RegrSlope(distinct, alwaysNullable, children().get(0), children().get(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNullableAggregateFunction(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}

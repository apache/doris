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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ComputePrecision;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.window.SupportWindowAnalytic;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AggregateFunction 'regr_avgx'.
 */
public class RegrAvgX extends NullableAggregateFunction
        implements BinaryExpression, ExplicitlyCastableSignature, ComputePrecision, SupportWindowAnalytic {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(TinyIntType.INSTANCE, TinyIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(SmallIntType.INSTANCE, SmallIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(IntegerType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(BigIntType.INSTANCE, BigIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(LargeIntType.INSTANCE, LargeIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(DoubleType.INSTANCE, DoubleType.INSTANCE),
            FunctionSignature.ret(DecimalV3Type.WILDCARD).args(DecimalV3Type.WILDCARD, DecimalV3Type.WILDCARD));

    /**
     * constructor with 2 argument.
     */
    public RegrAvgX(Expression arg1, Expression arg2) {
        this(false, arg1, arg2);
    }

    /**
     * constructor with 3 argument.
     */
    public RegrAvgX(boolean distinct, Expression arg1, Expression arg2) {
        this(distinct, false, arg1, arg2);
    }

    private RegrAvgX(boolean distinct, boolean alwaysNullable, Expression arg1, Expression arg2) {
        super("regr_avgx", distinct, alwaysNullable, arg1, arg2);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        DataType arg0Type = left().getDataType();
        DataType arg1Type = right().getDataType();
        if ((!arg0Type.isNumericType() && !arg0Type.isNullType())
                || arg0Type.isOnlyMetricType()) {
            throw new AnalysisException("regr_avgx requires numeric for first parameter: " + toSql());
        } else if ((!arg1Type.isNumericType() && !arg1Type.isNullType())
                || arg1Type.isOnlyMetricType()) {
            throw new AnalysisException("regr_avgx requires numeric for second parameter: " + toSql());
        }
    }

    @Override
    public FunctionSignature computePrecision(FunctionSignature signature) {
        DataType argumentType = getArgumentType(1);
        if (signature.getArgType(1) instanceof DecimalV3Type) {
            boolean enableDecimal256 = false;
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null) {
                enableDecimal256 = connectContext.getSessionVariable().isEnableDecimal256();
            }
            DecimalV3Type decimalV3Type = DecimalV3Type.forType(argumentType);
            // DecimalV3 scale lower than DEFAULT_MIN_AVG_DECIMAL128_SCALE should do cast
            int precision = decimalV3Type.getPrecision();
            int scale = decimalV3Type.getScale();
            if (decimalV3Type.getScale() < ScalarType.DEFAULT_MIN_AVG_DECIMAL128_SCALE) {
                scale = ScalarType.DEFAULT_MIN_AVG_DECIMAL128_SCALE;
                precision = precision - decimalV3Type.getScale() + scale;
                if (enableDecimal256) {
                    if (precision > DecimalV3Type.MAX_DECIMAL256_PRECISION) {
                        precision = DecimalV3Type.MAX_DECIMAL256_PRECISION;
                    }
                } else {
                    if (precision > DecimalV3Type.MAX_DECIMAL128_PRECISION) {
                        precision = DecimalV3Type.MAX_DECIMAL128_PRECISION;
                    }
                }
            }
            decimalV3Type = DecimalV3Type.createDecimalV3Type(precision, scale);
            return signature.withArgumentType(1, decimalV3Type)
                    .withReturnType(DecimalV3Type.createDecimalV3Type(
                            enableDecimal256 ? DecimalV3Type.MAX_DECIMAL256_PRECISION
                                    : DecimalV3Type.MAX_DECIMAL128_PRECISION,
                            decimalV3Type.getScale()));
        } else {
            return signature;
        }
    }

    /**
     * withDistinctAndChildren.
     */
    @Override
    public AggregateFunction withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new RegrAvgX(distinct, alwaysNullable, left(), right());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitRegrAvgX(this, context);
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean alwaysNullable) {
        return new RegrAvgX(distinct, alwaysNullable, left(), right());
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}

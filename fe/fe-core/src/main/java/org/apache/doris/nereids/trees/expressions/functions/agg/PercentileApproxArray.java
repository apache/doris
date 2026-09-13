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
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DoubleType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * AggregateFunction 'percentile_approx_array'.
 */
public class PercentileApproxArray extends NotNullableAggregateFunction
        implements ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(ArrayType.of(DoubleType.INSTANCE))
                    .args(DoubleType.INSTANCE, ArrayType.of(DoubleType.INSTANCE)),
            FunctionSignature.ret(ArrayType.of(DoubleType.INSTANCE))
                    .args(DoubleType.INSTANCE, ArrayType.of(DoubleType.INSTANCE), DoubleType.INSTANCE)
    );

    public PercentileApproxArray(Expression arg0, Expression arg1) {
        this(false, arg0, arg1);
    }

    public PercentileApproxArray(boolean distinct, Expression arg0, Expression arg1) {
        super("percentile_approx_array", distinct, arg0, arg1);
    }

    public PercentileApproxArray(Expression arg0, Expression arg1, Expression arg2) {
        this(false, arg0, arg1, arg2);
    }

    public PercentileApproxArray(boolean distinct, Expression arg0, Expression arg1,
            Expression arg2) {
        super("percentile_approx_array", distinct, arg0, arg1, arg2);
    }

    /** constructor for withChildren and reuse signature */
    private PercentileApproxArray(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!getArgument(1).isConstant()) {
            throw new AnalysisException(
                    "percentile_approx_array requires second parameter must be a constant : " + this.toSql());
        }
        if (arity() == 3 && !getArgument(2).isConstant()) {
            throw new AnalysisException(
                    "percentile_approx_array requires the third parameter must be a constant : " + this.toSql());
        }
    }

    @Override
    public void checkLegalityAfterRewrite() {
        checkLegalityBeforeTypeCoercion();
        Expression quantiles = getArgument(1);
        if (!(quantiles instanceof ArrayLiteral)) {
            return;
        }
        for (Expression item : ((ArrayLiteral) quantiles).getValue()) {
            if (item instanceof NullLiteral) {
                throw new AnalysisException(
                        "percentile_approx_array quantile should not be null : " + this.toSql());
            }
            if (!(item instanceof Literal) || !item.getDataType().isNumericType()) {
                continue;
            }
            double value = ((Literal) item).getDouble();
            if (!Double.isFinite(value) || value < 0.0 || value > 1.0) {
                throw new AnalysisException("percentile_approx_array quantile must be in [0, 1], but got "
                        + value + ": " + this.toSql());
            }
        }
    }

    @Override
    public PercentileApproxArray withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2 || children.size() == 3);
        return new PercentileApproxArray(getFunctionParams(distinct, children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPercentileApproxArray(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public Expression resultForEmptyInput() {
        return new ArrayLiteral(new ArrayList<>(), this.getDataType());
    }
}

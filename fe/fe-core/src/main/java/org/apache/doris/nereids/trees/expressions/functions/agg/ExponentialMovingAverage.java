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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DoubleType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Exponential Moving Average aggregate function.
 *
 * <p>Computes the exponentially smoothed moving average over time-indexed values.
 * The half_decay parameter controls the half-life period: the time after which the
 * exponential weight of a past value decays by a factor of 1/2.
 *
 * <p>Signature: {@code exponential_moving_average(half_decay DOUBLE, value DOUBLE,
 * timeunit DOUBLE) -> DOUBLE}
 *
 * <p>The timeunit argument is a numeric time index, not a raw timestamp. For
 * timestamp columns use {@code intDiv(toUnixTimestamp(ts), interval_seconds)}.
 */
public class ExponentialMovingAverage extends NullableAggregateFunction
        implements ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE)
                    .args(DoubleType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE)
    );

    /**
     * Constructor with 3 arguments: (half_decay, value, timeunit).
     */
    public ExponentialMovingAverage(Expression halfDecay, Expression value, Expression timeunit) {
        this(false, halfDecay, value, timeunit);
    }

    /**
     * Constructor with distinct flag and 3 arguments.
     */
    public ExponentialMovingAverage(boolean distinct, Expression halfDecay,
            Expression value, Expression timeunit) {
        this(distinct, false, halfDecay, value, timeunit);
    }

    /**
     * Full constructor.
     */
    public ExponentialMovingAverage(boolean distinct, boolean alwaysNullable,
            Expression halfDecay, Expression value, Expression timeunit) {
        super("exponential_moving_average", distinct, alwaysNullable, halfDecay, value, timeunit);
    }

    /** Constructor for withChildren and reuse signature. */
    private ExponentialMovingAverage(NullableAggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!getArgument(0).isConstant()) {
            throw new AnalysisException("The half_decay argument of "
                    + getName() + " must be a constant");
        }
        if (!getArgumentType(0).isNumericType()) {
            throw new AnalysisException("The half_decay argument of "
                    + getName() + " must be numeric");
        }
        if (!getArgumentType(1).isNumericType()) {
            throw new AnalysisException("The value argument of "
                    + getName() + " must be numeric");
        }
        if (!getArgumentType(2).isNumericType()) {
            throw new AnalysisException("The timeunit argument of "
                    + getName() + " must be numeric");
        }
    }

    @Override
    public ExponentialMovingAverage withDistinctAndChildren(boolean distinct,
            List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new ExponentialMovingAverage(getFunctionParams(distinct, children));
    }

    @Override
    public ExponentialMovingAverage withAlwaysNullable(boolean alwaysNullable) {
        return new ExponentialMovingAverage(getAlwaysNullableFunctionParams(alwaysNullable));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitExponentialMovingAverage(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}

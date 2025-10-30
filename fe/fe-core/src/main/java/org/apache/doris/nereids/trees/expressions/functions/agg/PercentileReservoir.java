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
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DoubleType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AggregateFunction 'percentile_reservoir'
 */
public class PercentileReservoir extends NullableAggregateFunction
        implements BinaryExpression, ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(DoubleType.INSTANCE, DoubleType.INSTANCE)

    );

    /**
     * constructor with 2 arguments.
     */
    public PercentileReservoir(Expression arg0, Expression arg1) {
        this(false, arg0, arg1);
    }

    /**
     * constructor with 2 arguments.
     */
    public PercentileReservoir(boolean distinct, Expression arg0, Expression arg1) {
        this(distinct, false, arg0, arg1);
    }

    public PercentileReservoir(boolean distinct, boolean alwaysNullable, Expression arg0, Expression arg1) {
        super("percentile_reservoir", distinct, alwaysNullable, arg0, arg1);
    }

    /** constructor for withChildren and reuse signature */
    private PercentileReservoir(NullableAggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!getArgument(1).isConstant()) {
            throw new AnalysisException(
                    "percentile_reservoir requires second parameter must be a constant : " + this.toSql());
        }
        if (child(1) instanceof Literal) {
            double value = ((Literal) child(1)).getDouble();
            if (value < 0 || value > 1) {
                throw new AnalysisException(
                        "percentile_reservoir level must be in [0, 1], but got " + value + ": " + this.toSql());
            }
        } else {
            throw new AnalysisException(
                "percentile_reservoir requires second parameter must be a constant: " + this.toSql());
        }
    }

    /**
     * withDistinctAndChildren.
     */
    @Override
    public PercentileReservoir withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new PercentileReservoir(getFunctionParams(distinct, children));
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean alwaysNullable) {
        return new PercentileReservoir(getAlwaysNullableFunctionParams(alwaysNullable));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPercentileReservoir(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}

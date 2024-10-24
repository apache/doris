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
 * AggregateFunction 'percentile_approx_weighted'.
 */
public class PercentileApproxWeighted extends NullableAggregateFunction
        implements ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(

            FunctionSignature.ret(DoubleType.INSTANCE)
                    .args(DoubleType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE),

            FunctionSignature.ret(DoubleType.INSTANCE)
                    .args(DoubleType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE));

    /**
     * constructor with 3 arguments.
     */
    public PercentileApproxWeighted(Expression arg0, Expression arg1, Expression arg2) {
        this(false, arg0, arg1, arg2);
    }

    /**
     * constructor with 3 arguments.
     */
    public PercentileApproxWeighted(boolean distinct, Expression arg0, Expression arg1, Expression arg2) {
        this(distinct, false, arg0, arg1, arg2);
    }

    public PercentileApproxWeighted(boolean distinct, boolean alwaysNullable, Expression arg0,
            Expression arg1, Expression arg2) {
        super("percentile_approx_weighted", distinct, alwaysNullable, arg0, arg1, arg2);
    }

    /**
     * constructor with 4 arguments.
     */
    public PercentileApproxWeighted(Expression arg0, Expression arg1, Expression arg2, Expression arg3) {
        this(false, arg0, arg1, arg2, arg3);
    }

    /**
     * constructor with 4 arguments.
     */
    public PercentileApproxWeighted(boolean distinct, Expression arg0, Expression arg1, Expression arg2,
            Expression arg3) {
        this(distinct, false, arg0, arg1, arg2, arg3);
    }

    public PercentileApproxWeighted(boolean distinct, boolean alwaysNullable, Expression arg0,
            Expression arg1, Expression arg2, Expression arg3) {
        super("percentile_approx_weighted", distinct, alwaysNullable, arg0, arg1, arg2, arg3);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!getArgument(2).isConstant()) {
            throw new AnalysisException(
                    "percentile_approx_weighted requires the third parameter must be a constant : " + this.toSql());
        }
        if (arity() == 4) {
            if (!getArgument(3).isConstant()) {
                throw new AnalysisException(
                        "percentile_approx_weighted requires the fourth parameter must be a constant : "
                                + this.toSql());
            }
        }
    }

    /**
     * withDistinctAndChildren.
     */
    @Override
    public PercentileApproxWeighted withDistinctAndChildren(boolean distinct,
            List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3 || children.size() == 4);
        if (children.size() == 3) {
            return new PercentileApproxWeighted(distinct, alwaysNullable, children.get(0),
                    children.get(1), children.get(2));
        } else {
            return new PercentileApproxWeighted(distinct, alwaysNullable, children.get(0),
                    children.get(1), children.get(2), children.get(3));
        }
    }

    @Override
    public PercentileApproxWeighted withAlwaysNullable(boolean alwaysNullable) {
        if (children.size() == 3) {
            return new PercentileApproxWeighted(distinct, alwaysNullable, children.get(0),
                    children.get(1), children.get(2));
        } else {
            return new PercentileApproxWeighted(distinct, alwaysNullable, children.get(0),
                    children.get(1), children.get(2), children.get(3));
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPercentileApprox(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}

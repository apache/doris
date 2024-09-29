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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** regr_sxx agg function. */
public class RegrSxx extends NullableAggregateFunction
        implements ExplicitlyCastableSignature, SupportWindowAnalytic {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(DoubleType.INSTANCE, DoubleType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(BigIntType.INSTANCE, BigIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(IntegerType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(SmallIntType.INSTANCE, SmallIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(TinyIntType.INSTANCE, TinyIntType.INSTANCE));

    public RegrSxx(Expression arg0, Expression arg1) {
        this(false, false, arg0, arg1);
    }

    public RegrSxx(boolean distinct, Expression arg0, Expression arg1) {
        this(distinct, false, arg0, arg1);
    }

    public RegrSxx(boolean distinct, boolean alwaysNullable, Expression arg0, Expression arg1) {
        super("regr_sxx", distinct, alwaysNullable, arg0, arg1);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        DataType regrSxxTypeFirst = child(0).getDataType();
        DataType regrSxxTypeSecond = child(1).getDataType();
        if ((!regrSxxTypeFirst.isNumericType() && !regrSxxTypeFirst.isNullType())
                || regrSxxTypeFirst.isOnlyMetricType()) {
            throw new AnalysisException("regr_sxx requires numeric for first parameter");
        } else if ((!regrSxxTypeSecond.isNumericType() && !regrSxxTypeSecond.isNullType())
                || regrSxxTypeSecond.isOnlyMetricType()) {
            throw new AnalysisException("regr_sxx requires numeric for second parameter");

        }
    }

    @Override
    public RegrSxx withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new RegrSxx(distinct, alwaysNullable, children.get(0), children.get(1));
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean alwaysNullable) {
        return new RegrSxx(distinct, alwaysNullable, children.get(0), children.get(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitRegrSxx(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}

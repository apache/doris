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
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** regr_sxy agg function. */
public class RegrSxy extends AggregateFunction
        implements BinaryExpression, ExplicitlyCastableSignature, AlwaysNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(DoubleType.INSTANCE, DoubleType.INSTANCE));

    public RegrSxy(Expression arg0, Expression arg1) {
        this(false, arg0, arg1);
    }

    public RegrSxy(boolean distinct, Expression arg0, Expression arg1) {
        super("regr_sxy", distinct, arg0, arg1);
    }

    public RegrSxy(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        DataType yType = left().getDataType();
        DataType xType = right().getDataType();
        if (yType.isOnlyMetricType() || xType.isOnlyMetricType()) {
            throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
        }
        if (!yType.isNumericType() && !yType.isNullType()) {
            throw new AnalysisException("regr_sxy requires numeric for first parameter: " + toSql());
        }
        if (!xType.isNumericType() && !xType.isNullType()) {
            throw new AnalysisException("regr_sxy requires numeric for second parameter: " + toSql());
        }
    }

    @Override
    public RegrSxy withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new RegrSxy(getFunctionParams(distinct, children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitRegrSxy(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}

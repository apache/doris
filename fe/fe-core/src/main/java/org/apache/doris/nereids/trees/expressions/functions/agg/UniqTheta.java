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
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AggregateFunction 'uniq_theta'. Theta-sketch based approximate distinct count.
 */
public class UniqTheta extends NotNullableAggregateFunction
        implements UnaryExpression, ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE).args(AnyDataType.INSTANCE_WITHOUT_INDEX)
    );

    /**
     * constructor with 1 argument.
     */
    public UniqTheta(Expression arg) {
        super("uniq_theta", arg);
    }

    public UniqTheta(boolean distinct, Expression arg) {
        this(arg);
    }

    /** constructor for withChildren and reuse signature */
    private UniqTheta(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (getArgumentType(0).isOnlyMetricType()) {
            throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
        }
    }

    /**
     * withDistinctAndChildren.
     */
    @Override
    public UniqTheta withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new UniqTheta(getFunctionParams(distinct, children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUniqTheta(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public Expression resultForEmptyInput() {
        return new BigIntLiteral(0);
    }
}

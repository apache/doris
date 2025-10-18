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

import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * AggregateFunction 'agg_array_sum'
 */
public class AggArraySum extends NotNullableAggregateFunction
        implements UnaryExpression, ExplicitlyCastableSignature {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.retArgType(0).args(ArrayType.of(DoubleType.INSTANCE)),
            FunctionSignature.retArgType(0).args(ArrayType.of(FloatType.INSTANCE)),
            FunctionSignature.retArgType(0).args(ArrayType.of(LargeIntType.INSTANCE)),
            FunctionSignature.retArgType(0).args(ArrayType.of(BigIntType.INSTANCE)),
            FunctionSignature.retArgType(0).args(ArrayType.of(IntegerType.INSTANCE)),
            FunctionSignature.retArgType(0).args(ArrayType.of(SmallIntType.INSTANCE)),
            FunctionSignature.retArgType(0).args(ArrayType.of(TinyIntType.INSTANCE)),
            FunctionSignature.retArgType(0).args(ArrayType.of(DecimalV3Type.WILDCARD))
    );

    public AggArraySum(Expression child) {
        this(false, false, child);
    }

    public AggArraySum(boolean distinct, Expression arg) {
        this(distinct, false, arg);
    }

    public AggArraySum(boolean distinct, boolean isSkew, Expression arg) {
        super(FunctionSet.AGG_ARRAY_SUM, distinct, isSkew, arg);
    }

    private AggArraySum(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        DataType argType = child().getDataType();
        if (!(argType instanceof ArrayType)
                || !((ArrayType) argType).getItemType().isNumericType()) {
            throw new AnalysisException("arr_array_sum requires a array of numeric parameter: " + this.toSql());
        }
    }

    @Override
    public AggregateFunction withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new AggArraySum(getFunctionParams(distinct, children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAggArraySum(this, context);
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

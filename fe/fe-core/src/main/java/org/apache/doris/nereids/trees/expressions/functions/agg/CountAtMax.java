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
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * count_at_max(expr): the number of input rows whose value equals MAX(expr).
 *
 * <p>NULL inputs are ignored. An empty input, or an input whose values are all NULL, returns 0.
 * This is a Doris extension; it is equivalent to
 * {@code COUNT(*) KEEP (DENSE_RANK LAST ORDER BY expr)} in Oracle.
 */
public class CountAtMax extends NotNullableAggregateFunction
        implements UnaryExpression, CustomSignature, NullIgnoringAggregateFunction {

    public CountAtMax(Expression arg) {
        this(false, false, arg);
    }

    public CountAtMax(boolean distinct, Expression arg) {
        this(distinct, false, arg);
    }

    private CountAtMax(boolean distinct, boolean alwaysNullable, Expression arg) {
        super("count_at_max", distinct, false, arg);
    }

    /** constructor for withChildren and reuse signature */
    private CountAtMax(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (distinct) {
            throw new AnalysisException("COUNT_AT_MAX does not support DISTINCT");
        }
        if (getArgumentType(0).isOnlyMetricType() && !getArgumentType(0).isArrayType()) {
            throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
        }
    }

    @Override
    public void checkLegalityAfterRewrite() {
        checkLegalityBeforeTypeCoercion();
    }

    @Override
    public FunctionSignature customSignature() {
        DataType dataType = getArgument(0).getDataType();
        if (dataType instanceof DecimalV2Type) {
            dataType = DecimalV3Type.forType(dataType);
        }
        return FunctionSignature.ret(BigIntType.INSTANCE).args(dataType);
    }

    @Override
    protected List<DataType> intermediateTypes() {
        DataType dataType = getArgument(0).getDataType();
        if (dataType instanceof DecimalV2Type) {
            dataType = DecimalV3Type.forType(dataType);
        }
        return ImmutableList.of(dataType, BigIntType.INSTANCE);
    }

    @Override
    public Expression resultForEmptyInput() {
        return new BigIntLiteral(0);
    }

    @Override
    public CountAtMax withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new CountAtMax(getFunctionParams(distinct, children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCountAtMax(this, context);
    }
}

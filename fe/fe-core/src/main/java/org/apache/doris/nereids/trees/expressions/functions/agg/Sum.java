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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.FractionalType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.NumericType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** sum agg function. */
public class Sum extends AggregateFunction implements UnaryExpression, ImplicitCastInputTypes {

    // used in interface expectedInputTypes to avoid new list in each time it be called
    private static final List<AbstractDataType> EXPECTED_INPUT_TYPES = ImmutableList.of(NumericType.INSTANCE);

    public Sum(Expression child) {
        super("sum", child);
    }

    public Sum(AggregateParam aggregateParam, Expression child) {
        super("sum", aggregateParam, child);
    }

    @Override
    public DataType getFinalType() {
        DataType dataType = child().getDataType();
        if (dataType instanceof LargeIntType) {
            return dataType;
        } else if (dataType instanceof DecimalType) {
            return DecimalType.SYSTEM_DEFAULT;
        } else if (dataType instanceof IntegralType) {
            return BigIntType.INSTANCE;
        } else if (dataType instanceof FractionalType) {
            return DoubleType.INSTANCE;
        } else {
            throw new IllegalStateException("Unsupported sum type: " + dataType);
        }
    }

    @Override
    public DataType getIntermediateType() {
        return getFinalType();
    }

    @Override
    public boolean nullable() {
        return child().nullable();
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        return EXPECTED_INPUT_TYPES;
    }

    @Override
    public Sum withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Sum(getAggregateParam(), children.get(0));
    }

    @Override
    public Sum withAggregateParam(AggregateParam aggregateParam) {
        return new Sum(aggregateParam, child());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSum(this, context);
    }
}

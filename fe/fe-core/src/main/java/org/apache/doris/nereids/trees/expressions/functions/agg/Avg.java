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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.nereids.types.coercion.TypeCollection;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** avg agg function. */
public class Avg extends AggregateFunction implements UnaryExpression, ImplicitCastInputTypes {

    // used in interface expectedInputTypes to avoid new list in each time it be called
    private static final List<AbstractDataType> EXPECTED_INPUT_TYPES = ImmutableList.of(
            new TypeCollection(NumericType.INSTANCE, DateTimeType.INSTANCE, DateType.INSTANCE)
    );

    public Avg(Expression child) {
        super("avg", child);
    }

    public Avg(AggregateParam aggregateParam, Expression child) {
        super("avg", aggregateParam, child);
    }

    @Override
    public DataType getFinalType() {
        DataType argumentType = inputTypesBeforeDissemble()
                .map(types -> types.get(0))
                .orElse(child().getDataType());
        if (argumentType instanceof DecimalType) {
            return DecimalType.SYSTEM_DEFAULT;
        } else if (argumentType.isDate()) {
            return DateType.INSTANCE;
        } else if (argumentType.isDateTime()) {
            return DateTimeType.INSTANCE;
        } else {
            return DoubleType.INSTANCE;
        }
    }

    // TODO: We should return a complex type: PartialAggType(bufferTypes=[Double, Int], inputTypes=[Int])
    //       to denote sum(double) and count(int)
    @Override
    public DataType getIntermediateType() {
        return VarcharType.createVarcharType(-1);
    }

    @Override
    public boolean nullable() {
        return child().nullable();
    }

    @Override
    public Avg withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Avg(getAggregateParam(), children.get(0));
    }

    @Override
    public Avg withAggregateParam(AggregateParam aggregateParam) {
        return new Avg(aggregateParam, child());
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        if (isGlobal() && inputTypesBeforeDissemble().isPresent()) {
            return ImmutableList.of();
        } else {
            return EXPECTED_INPUT_TYPES;
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAvg(this, context);
    }
}

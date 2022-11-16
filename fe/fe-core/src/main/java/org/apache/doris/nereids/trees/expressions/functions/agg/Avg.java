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
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** avg agg function. */
public class Avg extends AggregateFunction implements UnaryExpression, PropagateNullable, CustomSignature {

    public Avg(Expression child) {
        super("avg", child);
    }

    public Avg(AggregateParam aggregateParam, Expression child) {
        super("avg", aggregateParam, child);
    }

    @Override
    public DataType signatureReturnType(List<DataType> argumentTypes, List<Expression> arguments) {
        DataType argumentType = argumentTypes.get(0);
        if (argumentType instanceof DecimalV2Type) {
            return DecimalV2Type.SYSTEM_DEFAULT;
        } else if (argumentType.isDate()) {
            return DateType.INSTANCE;
        } else if (argumentType.isDateTime()) {
            return DateTimeType.INSTANCE;
        } else {
            return DoubleType.INSTANCE;
        }
    }

    @Override
    protected List<DataType> intermediateTypes(List<DataType> argumentTypes, List<Expression> arguments) {
        DoubleType sumType = DoubleType.INSTANCE;
        IntegerType countType = IntegerType.INSTANCE;
        return ImmutableList.of(sumType, countType);
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
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAvg(this, context);
    }
}

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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;

/** min agg function. */
public class Min extends AggregateFunction implements UnaryExpression {

    public Min(Expression child) {
        super("min", child);
    }

    public Min(AggregateParam aggregateParam, Expression child) {
        super("min", aggregateParam, child);
    }

    @Override
    public DataType getFinalType() {
        return child().getDataType();
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
    public Min withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Min(getAggregateParam(), children.get(0));
    }

    @Override
    public Min withAggregateParam(AggregateParam aggregateParam) {
        return new Min(aggregateParam, child());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMin(this, context);
    }
}

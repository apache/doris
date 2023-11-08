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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/**
 * AggregateExpression.
 *
 * It is used to wrap some physical information for the aggregate function,
 * so the aggregate function don't need to care about the phase of
 * aggregate.
 */
public class AggregateExpression extends Expression implements UnaryExpression {

    private final AggregateFunction function;
    private final AggregateParam aggregateParam;

    /** local aggregate */
    public AggregateExpression(AggregateFunction aggregate, AggregateParam aggregateParam) {
        this(aggregate, aggregateParam, aggregate);
    }

    /** aggregate maybe consume a buffer, so the child could be a slot, not an aggregate function */
    public AggregateExpression(AggregateFunction aggregate, AggregateParam aggregateParam, Expression child) {
        super(child);
        this.function = Objects.requireNonNull(aggregate, "function cannot be null");
        this.aggregateParam = Objects.requireNonNull(aggregateParam, "aggregateParam cannot be null");
    }

    public AggregateFunction getFunction() {
        return function;
    }

    public AggregateParam getAggregateParam() {
        return aggregateParam;
    }

    public boolean isDistinct() {
        return function.isDistinct();
    }

    @Override
    public DataType getDataType() {
        if (aggregateParam.aggMode.productAggregateBuffer) {
            // buffer type
            return VarcharType.SYSTEM_DEFAULT;
        } else {
            // final result type
            return function.getDataType();
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAggregateExpression(this, context);
    }

    @Override
    public AggregateExpression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        Expression child = children.get(0);
        if (!aggregateParam.aggMode.consumeAggregateBuffer) {
            Preconditions.checkArgument(child instanceof AggregateFunction,
                    "when aggregateMode is " + aggregateParam.aggMode.name()
                            + ", the child of AggregateExpression should be AggregateFunction, but "
                            + child.getClass());
            return new AggregateExpression((AggregateFunction) child, aggregateParam);
        } else {
            return new AggregateExpression(function, aggregateParam, child);
        }
    }

    @Override
    public String toSql() {
        if (aggregateParam.aggMode.productAggregateBuffer) {
            return "partial_" + function.toSql();
        } else {
            return function.toSql();
        }
    }

    @Override
    public String toString() {
        AggMode aggMode = aggregateParam.aggMode;
        String prefix = aggMode.productAggregateBuffer ? "partial_" : "";
        if (aggMode.consumeAggregateBuffer) {
            return prefix + function.getName() + "(" + child().toString() + ")";
        } else {
            return prefix + child().toString();
        }
    }

    @Override
    public String getExpressionName() {
        return Utils.normalizeName(function.getName(), DEFAULT_EXPRESSION_NAME);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        AggregateExpression that = (AggregateExpression) o;
        return Objects.equals(function, that.function)
                && Objects.equals(aggregateParam, that.aggregateParam)
                && Objects.equals(child(), that.child());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), function, aggregateParam, child());
    }

    @Override
    public boolean nullable() {
        return function.nullable();
    }
}

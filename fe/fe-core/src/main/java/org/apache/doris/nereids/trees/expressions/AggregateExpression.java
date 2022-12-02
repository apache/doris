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

import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/** AggregateExpression */
public class AggregateExpression extends Expression implements UnaryExpression, PropagateNullable {
    private final AggregateFunction function;

    private final AggregateParam aggregateParam;

    /** local aggregate */
    public AggregateExpression(AggregateFunction localAggregate, AggregateParam aggregateParam) {
        this(localAggregate, aggregateParam, localAggregate);
    }

    /** aggregate maybe consume a buffer, so the child could be a slot, not an aggregate function */
    public AggregateExpression(AggregateFunction localAggregate, AggregateParam aggregateParam, Expression child) {
        super(child);
        this.function = Objects.requireNonNull(localAggregate, "function cannot be null");
        this.aggregateParam = Objects.requireNonNull(aggregateParam, "aggregateParam cannot be null");
    }

    public AggregateFunction getFunction() {
        return function;
    }

    public AggregateParam getAggregateParam() {
        return aggregateParam;
    }

    @Override
    public DataType getDataType() {
        if (aggregateParam.aggregateMode.productAggregateBuffer) {
            // buffer type
            return VarcharType.SYSTEM_DEFAULT;
        } else {
            // final result type
            return function.getDataType();
        }
    }

    @Override
    public AggregateExpression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        Expression child = children.get(0);
        if (!aggregateParam.aggregateMode.consumeAggregateBuffer) {
            Preconditions.checkArgument(child instanceof AggregateFunction,
                    "when aggregateMode is " + aggregateParam.aggregateMode.name()
                            + ", the child of AggregateExpression should be AggregateFunction, but "
                            + child.getClass());
            return new AggregateExpression((AggregateFunction) child, aggregateParam);
        } else {
            return new AggregateExpression(function, aggregateParam, child);
        }
    }

    @Override
    public String toSql() {
        // alias come from the origin aggregate function
        return function.toSql();
    }

    @Override
    public String toString() {
        if (aggregateParam.aggregateMode.consumeAggregateBuffer) {
            String functionName = function.getName();
            return functionName + "(buffer " + child().toString() + ")";
        } else {
            return function.toString();
        }
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
}

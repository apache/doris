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

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.typecoercion.ExpectsInputTypes;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.PartialAggType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The function which consume arguments in lots of rows and product one value.
 */
public abstract class AggregateFunction extends BoundFunction implements ExpectsInputTypes {

    protected final boolean isDistinct;

    public AggregateFunction(String name, Expression... arguments) {
        this(name, false, arguments);
    }

    public AggregateFunction(String name, boolean isDistinct, Expression... arguments) {
        super(name, arguments);
        this.isDistinct = isDistinct;
    }

    public AggregateFunction(String name, List<Expression> children) {
        this(name, false, children);
    }

    public AggregateFunction(String name, boolean isDistinct, List<Expression> children) {
        super(name, children);
        this.isDistinct = isDistinct;
    }

    @Override
    public abstract AggregateFunction withChildren(List<Expression> children);

    protected List<DataType> intermediateTypes() {
        return ImmutableList.of(VarcharType.SYSTEM_DEFAULT);
    }

    public abstract AggregateFunction withDistinctAndChildren(boolean isDistinct, List<Expression> children);

    /** getIntermediateTypes */
    public final PartialAggType getIntermediateTypes() {
        return new PartialAggType(getArguments(), intermediateTypes());
    }

    @Override
    public final DataType getDataType() {
        return getSignature().returnType;
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        return getSignature().argumentsTypes;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AggregateFunction that = (AggregateFunction) o;
        return Objects.equals(isDistinct, that.isDistinct)
                && Objects.equals(getName(), that.getName())
                && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isDistinct, getName(), children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAggregateFunction(this, context);
    }

    @Override
    public boolean hasVarArguments() {
        return false;
    }

    @Override
    public String toSql() throws UnboundException {
        String args = children()
                .stream()
                .map(Expression::toSql)
                .collect(Collectors.joining(", "));
        return getName() + "(" + (isDistinct ? "DISTINCT " : "") + args + ")";
    }

    @Override
    public String toString() {
        String args = children()
                .stream()
                .map(Expression::toString)
                .collect(Collectors.joining(", "));
        return getName() + "(" + (isDistinct ? "DISTINCT " : "") + args + ")";
    }
}

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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.GroupingSetShapes;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * it is not a real column exist in table.
 */
public class VirtualSlotReference extends SlotReference implements SlotNotFromChildren {
    // arguments of GroupingScalarFunction
    private final List<Expression> realExpressions;

    // if this VirtualSlotReference come from the GroupingScalarFunction, we will save it.
    private final Optional<GroupingScalarFunction> originExpression;

    // save the method to compute the long value list, and then backend can fill the long
    // value result for this VirtualSlotReference.
    // this long values can compute by the shape of grouping sets.
    private final Function<GroupingSetShapes, List<Long>> computeLongValueMethod;

    public VirtualSlotReference(String name, DataType dataType, Optional<GroupingScalarFunction> originExpression,
            Function<GroupingSetShapes, List<Long>> computeLongValueMethod) {
        this(StatementScopeIdGenerator.newExprId(), name, dataType, false, ImmutableList.of(),
                originExpression, computeLongValueMethod);
    }

    /** VirtualSlotReference */
    public VirtualSlotReference(ExprId exprId, String name, DataType dataType,
            boolean nullable, List<String> qualifier, Optional<GroupingScalarFunction> originExpression,
            Function<GroupingSetShapes, List<Long>> computeLongValueMethod) {
        super(exprId, name, dataType, nullable, qualifier);
        this.originExpression = Objects.requireNonNull(originExpression, "originExpression can not be null");
        this.realExpressions = originExpression.isPresent()
                ? ImmutableList.copyOf(originExpression.get().getArguments())
                : ImmutableList.of();
        this.computeLongValueMethod =
                Objects.requireNonNull(computeLongValueMethod, "computeLongValueMethod can not be null");
    }

    public List<Expression> getRealExpressions() {
        return realExpressions;
    }

    public Optional<GroupingScalarFunction> getOriginExpression() {
        return originExpression;
    }

    public Function<GroupingSetShapes, List<Long>> getComputeLongValueMethod() {
        return computeLongValueMethod;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitVirtualReference(this, context);
    }

    @Override
    public String computeToSql() {
        return getName();
    }

    @Override
    public String toString() {
        // Just return name and exprId, add another method to show fully qualified name when it's necessary.
        String str = getName() + "#" + getExprId();

        if (originExpression.isPresent()) {
            str += " originExpression=" + originExpression.get();
        }
        return str;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VirtualSlotReference that = (VirtualSlotReference) o;
        return Objects.equals(realExpressions, that.realExpressions)
                && Objects.equals(originExpression, that.originExpression)
                && super.equals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(realExpressions, originExpression, getExprId());
    }

    @Override
    public boolean nullable() {
        return false;
    }

    public VirtualSlotReference withNullable(boolean nullable) {
        if (this.nullable == nullable) {
            return this;
        }
        return new VirtualSlotReference(exprId, name.get(), dataType, nullable, qualifier,
                originExpression, computeLongValueMethod);
    }

    @Override
    public Slot withNullableAndDataType(boolean nullable, DataType dataType) {
        if (this.nullable == nullable && this.dataType.equals(dataType)) {
            return this;
        }
        return new VirtualSlotReference(exprId, name.get(), dataType, nullable, qualifier,
                originExpression, computeLongValueMethod);
    }

    @Override
    public VirtualSlotReference withQualifier(List<String> qualifier) {
        return new VirtualSlotReference(exprId, name.get(), dataType, nullable, qualifier,
                originExpression, computeLongValueMethod);
    }

    @Override
    public VirtualSlotReference withName(String name) {
        return new VirtualSlotReference(exprId, name, dataType, nullable, qualifier,
                originExpression, computeLongValueMethod);
    }

    @Override
    public VirtualSlotReference withExprId(ExprId exprId) {
        return new VirtualSlotReference(exprId, name.get(), dataType, nullable, qualifier,
                originExpression, computeLongValueMethod);
    }

    @Override
    public Slot withIndexInSql(Pair<Integer, Integer> index) {
        return this;
    }
}

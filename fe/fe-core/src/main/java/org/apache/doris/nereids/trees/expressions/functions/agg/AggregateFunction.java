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
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.typecoercion.ExpectsInputTypes;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.PartialAggType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * The function which consume arguments in lots of rows and product one value.
 */
public abstract class AggregateFunction extends BoundFunction implements ExpectsInputTypes {

    private final AggregateParam aggregateParam;

    public AggregateFunction(String name, Expression... arguments) {
        this(name, AggregateParam.finalPhase(), arguments);
    }

    public AggregateFunction(String name, AggregateParam aggregateParam, Expression... arguments) {
        super(name, arguments);
        this.aggregateParam = Objects.requireNonNull(aggregateParam, "aggregateParam can not be null");
    }

    @Override
    public List<Expression> getOriginArguments() {
        return getArgumentsBeforeDisassembled();
    }

    @Override
    public List<DataType> getOriginArgumentTypes() {
        return getArgumentTypesBeforeDisassembled();
    }

    @Override
    public abstract AggregateFunction withChildren(List<Expression> children);

    public abstract AggregateFunction withAggregateParam(AggregateParam aggregateParam);

    protected abstract List<DataType> intermediateTypes(List<DataType> argumentTypes, List<Expression> arguments);

    /** getIntermediateTypes */
    public final PartialAggType getIntermediateTypes() {
        if (isGlobal() && isDisassembled()) {
            return (PartialAggType) child(0).getDataType();
        }
        List<Expression> arguments = getArgumentsBeforeDisassembled();
        List<DataType> types = getArgumentTypesBeforeDisassembled();
        return new PartialAggType(getArguments(), intermediateTypes(types, arguments));
    }

    public final DataType getFinalType() {
        return getSignature().returnType;
    }

    @Override
    public final DataType getDataType() {
        if (aggregateParam.aggPhase.isGlobal() || aggregateParam.isFinalPhase) {
            return getFinalType();
        } else {
            return getIntermediateTypes();
        }
    }

    @Override
    public final List<AbstractDataType> expectedInputTypes() {
        if (isGlobal() && isDisassembled()) {
            return ImmutableList.of(getIntermediateTypes());
        } else {
            return getSignature().argumentsTypes;
        }
    }

    public List<Expression> getArgumentsBeforeDisassembled() {
        if (arity() == 1 && getArgument(0).getDataType() instanceof PartialAggType) {
            return ((PartialAggType) getArgument(0).getDataType()).getOriginArguments();
        }
        return getArguments();
    }

    public List<DataType> getArgumentTypesBeforeDisassembled() {
        return getArgumentsBeforeDisassembled()
                .stream()
                .map(Expression::getDataType)
                .collect(ImmutableList.toImmutableList());
    }

    public boolean isDistinct() {
        return aggregateParam.isDistinct;
    }

    public boolean isGlobal() {
        return aggregateParam.aggPhase.isGlobal();
    }

    public boolean isFinalPhase() {
        return aggregateParam.isFinalPhase;
    }

    public boolean isDisassembled() {
        return aggregateParam.isDisassembled;
    }

    public AggregateParam getAggregateParam() {
        return aggregateParam;
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
        return Objects.equals(aggregateParam, that.aggregateParam)
                && Objects.equals(getName(), that.getName())
                && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregateParam, getName(), children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAggregateFunction(this, context);
    }

    @Override
    public boolean hasVarArguments() {
        return false;
    }
}

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

package org.apache.doris.nereids.trees.expressions.functions.combinator;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AggStateFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.DecimalSamePrecision;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

/**
 * AggState combinator state
 */
public class StateCombinator extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, AlwaysNotNullable, DecimalSamePrecision {

    private AggregateFunction nested;
    private AggStateType returnType;

    /**
     * constructor of StateCombinator
     */
    public StateCombinator(List<Expression> arguments, BoundFunction nested) {
        super(nested.getName() + AggStateFunctionBuilder.COMBINATOR_LINKER + AggStateFunctionBuilder.STATE, arguments);
        Preconditions.checkState(nested instanceof AggregateFunction);
        this.nested = (AggregateFunction) nested;
        this.returnType = new AggStateType(nested.getName(), arguments.stream().map(arg -> {
            return arg.getDataType();
        }).collect(Collectors.toList()), arguments.stream().map(arg -> {
            return arg.nullable();
        }).collect(Collectors.toList()));
    }

    @Override
    public StateCombinator withChildren(List<Expression> children) {
        throw new UnsupportedOperationException("Unimplemented method 'withChildren'");
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return nested.getSignatures().stream().map(sig -> {
            return sig.withReturnType(returnType);
        }).collect(Collectors.toList());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStateCombinator(this, context);
    }

    @Override
    public DataType getDataType() {
        return returnType;
    }

    public AggregateFunction getNestedFunction() {
        return nested;
    }
}

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
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.ComputeNullable;
import org.apache.doris.nereids.trees.expressions.functions.DecimalSamePrecision;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * AggState combinator merge
 */
public class MergeCombinator extends AggregateFunction
        implements UnaryExpression, ExplicitlyCastableSignature, ComputeNullable, DecimalSamePrecision {

    private AggregateFunction nested;

    public MergeCombinator(List<Expression> arguments, BoundFunction nested) {
        super(nested.getName() + AggStateFunctionBuilder.COMBINATOR_LINKER + AggStateFunctionBuilder.MERGE, arguments);
        Preconditions.checkState(nested instanceof AggregateFunction);
        this.nested = (AggregateFunction) nested;
    }

    @Override
    public MergeCombinator withChildren(List<Expression> children) {
        throw new UnsupportedOperationException("Unimplemented method 'withChildren'");
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        AggStateType type = new AggStateType(nested.getName(), nested.getArgumentsTypes(),
                nested.getArguments().stream().map(Expression::nullable).collect(Collectors.toList()));
        return nested.getSignatures().stream().map(sig -> {
            return sig.withArgumentTypes(false, Arrays.asList(type));
        }).collect(Collectors.toList());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMergeCombinator(this, context);
    }

    @Override
    public DataType getDataType() {
        return nested.getDataType();
    }

    public AggregateFunction getNestedFunction() {
        return nested;
    }

    @Override
    public AggregateFunction withDistinctAndChildren(boolean distinct, List<Expression> children) {
        throw new UnsupportedOperationException("Unimplemented method 'withDistinctAndChildren'");
    }

    @Override
    public boolean nullable() {
        return nested.nullable();
    }
}

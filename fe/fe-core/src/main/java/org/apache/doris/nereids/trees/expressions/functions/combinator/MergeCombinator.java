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
import org.apache.doris.nereids.trees.expressions.functions.DecimalSamePrecision;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * AggState combinator merge
 */
public class MergeCombinator extends AggregateFunction
        implements UnaryExpression, ExplicitlyCastableSignature, PropagateNullable, DecimalSamePrecision {

    private AggregateFunction nested;

    public MergeCombinator(List<Expression> arguments, BoundFunction nested) {
        super(nested.getName() + AggStateFunctionBuilder.COMBINATOR_LINKER + AggStateFunctionBuilder.MERGE, arguments);
        Preconditions.checkState(nested instanceof AggregateFunction);
        this.nested = (AggregateFunction) nested;
    }

    @Override
    public MergeCombinator withChildren(List<Expression> children) {
        throw new RuntimeException();
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        throw new RuntimeException();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        throw new RuntimeException();
    }

    @Override
    public AggregateFunction withDistinctAndChildren(boolean distinct, List<Expression> children) {
        throw new UnsupportedOperationException("Unimplemented method 'withDistinctAndChildren'");
    }

    public AggregateFunction getNestedFunction() {
        return nested;
    }
}

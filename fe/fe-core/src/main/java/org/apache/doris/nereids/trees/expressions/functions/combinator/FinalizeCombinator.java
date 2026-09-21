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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AggCombinerFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.ComputeNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunctionParams;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.AggStateType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** Finalize each serialized aggregate state without aggregating rows. */
public class FinalizeCombinator extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, ComputeNullable, Combinator {

    private final AggregateFunction nested;

    public FinalizeCombinator(List<Expression> arguments, AggregateFunction nested) {
        super(nested.getName() + AggCombinerFunctionBuilder.FINALIZE_SUFFIX, arguments);
        this.nested = nested;
        checkStateFunction();
    }

    private FinalizeCombinator(ScalarFunctionParams functionParams, AggregateFunction nested) {
        super(functionParams);
        this.nested = nested;
        checkStateFunction();
    }

    private void checkStateFunction() {
        AggStateType inputType = (AggStateType) getArgument(0).getDataType();
        // Keep the name check because acceptsType alone accepts unrelated aggregate states.
        // TODO: Unify aggregate alias canonicalization across FE and BE. Stored aliases absent
        // from AggStateType's mapping (for example, std) remain unsupported by finalize for now.
        AggStateType expected = new AggStateType(nested.getName(), inputType.getSubTypes(),
                inputType.getSubTypeNullables(), nested.nullable());
        if (!inputType.getFunctionName().equals(expected.getFunctionName())) {
            throw new AnalysisException(getName() + " requires a state of " + expected.getFunctionName()
                    + ", but got " + inputType.toSql());
        }
    }

    @Override
    public FinalizeCombinator withChildren(List<Expression> children) {
        return new FinalizeCombinator(getFunctionParams(children), nested);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return ImmutableList.of(FunctionSignature.ret(nested.getDataType()).args(getArgument(0).getDataType()));
    }

    @Override
    public boolean nullable() {
        return getArgument(0).nullable() || nested.nullable();
    }

    @Override
    public AggregateFunction getNestedFunction() {
        return nested;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitFinalizeCombinator(this, context);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        // Stored states retain serialized parameters, not the original constant expressions.
        if (getArgument(0) instanceof StateCombinator || getArgument(0) instanceof CombineCombinator) {
            nested.checkLegalityBeforeTypeCoercion();
        }
    }
}

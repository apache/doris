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

package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * class for nth_value(column, offset)
 */
public class NthValue extends WindowFunction
        implements AlwaysNullable, ExplicitlyCastableSignature {

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.retArgType(0).args(AnyDataType.INSTANCE_WITHOUT_INDEX, BigIntType.INSTANCE)
    );

    public NthValue(Expression child, Expression offset) {
        super("nth_value", child, offset);
    }

    public NthValue(List<Expression> children) {
        super("nth_value", children);
    }

    /** constructor for withChildren and reuse signature */
    private NthValue(WindowFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public NthValue withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new NthValue(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNthValue(this, context);
    }

    @Override
    public DataType getDataType() {
        return child(0).getDataType();
    }

    /**
    * Check the second parameter of NthValue function.
    * The second parameter must be a constant positive integer.
    */
    public static void checkSecondParameter(NthValue nthValue) {
        Preconditions.checkArgument(nthValue.arity() == 2);
        Expression offset = nthValue.child(1);
        if (offset instanceof Literal) {
            if (((Literal) offset).getDouble() <= 0) {
                throw new AnalysisException(
                        "The offset parameter of NthValue must be a constant positive integer: " + offset);
            }
        } else {
            throw new AnalysisException(
                "The offset parameter of NthValue must be a constant positive integer: " + offset);
        }
    }
}

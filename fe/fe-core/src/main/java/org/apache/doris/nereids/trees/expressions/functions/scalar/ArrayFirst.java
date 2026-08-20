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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.RewriteWhenAnalyze;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'array_first'.
 */
public class ArrayFirst extends ScalarFunction
        implements HighOrderFunction, PropagateNullLiteral, PropagateNullable, RewriteWhenAnalyze {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.retArgType(0).args(ArrayType.of(AnyDataType.INSTANCE_WITHOUT_INDEX),
                    ArrayType.of(BooleanType.INSTANCE))
    );

    /**
     * constructor with arguments.
     */
    public ArrayFirst(Expression arg) {
        super("array_first", arg instanceof Lambda ? arg.child(1).child(0) : arg, new ArrayMap(arg));
        if (!(arg instanceof Lambda)) {
            throw new AnalysisException(
                    String.format("The 1st arg of %s must be lambda but is %s", getName(), arg));
        }
    }

    /** constructor for withChildren and reuse signature */
    private ArrayFirst(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public ArrayFirst withChildren(List<Expression> children) {
        return new ArrayFirst(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getImplSignature() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayFirst(this, context);
    }

    // array_first(lambda, a1, ...) = element_at(array_filter(lambda, a1, ...), 1)
    @Override
    public Expression rewriteWhenAnalyze() {
        return new ElementAt(new ArrayFilter(getArgument(0), getArgument(1)), new BigIntLiteral(1));
    }
}

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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'array_shuffle'
 *  with 1 or 2 arguments : array_shuffle(arr) or array_shuffle(arr, seed)
 */
public class ArrayShuffle extends ScalarFunction
        implements BinaryExpression, ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.retArgType(0).args(ArrayType.of(new AnyDataType(0))),
            FunctionSignature.retArgType(0)
                    .args(ArrayType.of(new AnyDataType(0)), BigIntType.INSTANCE)
    );

    /**
     * constructor with 1 arguments.
     */
    public ArrayShuffle(Expression arg) {
        super("array_shuffle", arg);
    }

    /**
     * constructor with 2 arguments.
     */
    public ArrayShuffle(Expression arg, Expression arg1) {
        super("array_shuffle", arg, arg1);
    }

    /**
     * withChildren.
     */
    @Override
    public ArrayShuffle withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1
                || children.size() == 2);
        if (children.size() == 1) {
            return new ArrayShuffle(children.get(0));
        } else {
            return new ArrayShuffle(children.get(0), children.get(1));
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayShuffle(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

}

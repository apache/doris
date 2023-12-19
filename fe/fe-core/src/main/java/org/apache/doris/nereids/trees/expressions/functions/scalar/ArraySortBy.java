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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'array_sortby'.
 */
public class ArraySortBy extends ScalarFunction
        implements HighOrderFunction {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.retArgType(0).args(ArrayType.of(AnyDataType.INSTANCE_WITHOUT_INDEX),
                ArrayType.of(AnyDataType.INSTANCE_WITHOUT_INDEX))
    );

    private ArraySortBy(List<Expression> expressions) {
        super("array_sortby", expressions);
    }

    /**
     * constructor with arguments.
     * array_sortby(lambda, a1, ...) = array_sortby(a1, array_map(lambda, a1, ...))
     */
    public ArraySortBy(Expression arg) {
        super("array_sortby", arg.child(1).child(0), new ArrayMap(arg));
        if (!(arg instanceof Lambda)) {
            throw new AnalysisException(
                    String.format("The 1st arg of %s must be lambda but is %s", getName(), arg));
        }
    }

    public ArraySortBy(Expression arg1, Expression arg2) {
        super("array_sortby", arg1, arg2);
    }

    @Override
    public ArraySortBy withChildren(List<Expression> children) {
        return new ArraySortBy(children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArraySortBy(this, context);
    }

    @Override
    public List<FunctionSignature> getImplSignature() {
        return SIGNATURES;
    }

    @Override
    public boolean nullable() {
        return child(0).nullable();
    }
}

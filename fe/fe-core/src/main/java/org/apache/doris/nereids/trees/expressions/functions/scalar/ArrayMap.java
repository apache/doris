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
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.LambdaType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'array_map'.
 */
public class ArrayMap extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(new FollowToAnyDataType(0)).args(LambdaType.INSTANCE)
    );

    /**
     * constructor with arguments.
     */
    public ArrayMap(Expression arg) {
        super("array_map", arg);
    }

    public ArrayMap(List<Expression> arg) {
        super("array_map", arg);
    }

    /**
     * withChildren.
     */
    @Override
    public ArrayMap withChildren(List<Expression> children) {
        return new ArrayMap(children);
    }

    @Override
    public DataType getDataType() {
        Preconditions.checkArgument(children.get(0) instanceof Lambda,
                "The first arg of array_map must be lambda");
        return ArrayType.of(((Lambda) children.get(0)).getRetType(), true);
    }

    @Override
    public boolean nullable() {
        return ((Lambda) children.get(0)).getLambdaArguments().stream()
            .map(ArrayItemReference::getArrayExpression)
            .anyMatch(Expression::nullable);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayMap(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}

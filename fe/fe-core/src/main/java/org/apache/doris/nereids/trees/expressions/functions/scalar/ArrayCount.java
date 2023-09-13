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
import org.apache.doris.nereids.trees.expressions.typecoercion.ExpectsInputTypes;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.LambdaType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'array_count'.
 */
public class ArrayCount extends ScalarFunction
        implements ExplicitlyCastableSignature, ExpectsInputTypes {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(new FollowToAnyDataType(0)).args(LambdaType.INSTANCE)
    );

    /**
     * constructor with arguments.
     * array_count(lambda, a1, ...) = array_count(array_map(lambda, a1, ...))
     */
    private ArrayCount(Lambda lambda) {
        super("array_count", new ArrayMap(lambda));
    }

    private ArrayCount(List<Expression> expressions) {
        super("array_count", expressions);
    }

    public ArrayCount(Expression arg) {
        this((Lambda) arg);
    }

    /**
     * withChildren.
     */
    @Override
    public ArrayCount withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1 && !(children.get(0) instanceof Lambda),
                getName() + " accept wrong arguments " + children);
        return new ArrayCount(children);
    }

    @Override
    public DataType getDataType() {
        return BigIntType.INSTANCE;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayCount(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public boolean nullable() {
        return child(0).nullable();
    }

    @Override
    public List<DataType> expectedInputTypes() {
        return ImmutableList.of(ArrayType.of(BooleanType.INSTANCE));
    }

    @Override
    public boolean hasVarArguments() {
        return false;
    }
}

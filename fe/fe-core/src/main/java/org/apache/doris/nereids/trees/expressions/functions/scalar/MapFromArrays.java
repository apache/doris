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
import org.apache.doris.nereids.trees.expressions.PreferPushDownProject;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Construct a Map from key and value arrays with identical per-row offsets. */
public class MapFromArrays extends ScalarFunction
        implements BinaryExpression, ExplicitlyCastableSignature, PropagateNullable, PreferPushDownProject {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(MapType.of(new FollowToAnyDataType(0), new FollowToAnyDataType(1)))
                    .args(ArrayType.of(new AnyDataType(0)), ArrayType.of(new AnyDataType(1))));

    public MapFromArrays(Expression keys, Expression values) {
        super("map_from_arrays", keys, values);
    }

    protected MapFromArrays(String name, Expression keys, Expression values) {
        super(name, keys, values);
    }

    protected MapFromArrays(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public MapFromArrays withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new MapFromArrays(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public FunctionSignature computeSignature(FunctionSignature signature) {
        FunctionSignature resolvedSignature = super.computeSignature(signature);
        resolvedSignature.returnType.validateDataType();
        return resolvedSignature;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMapFromArrays(this, context);
    }
}

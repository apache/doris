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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'approx_cosine_distance'
 * this function will not be pushed down to vector index
 */
public class ApproxCosineDistance extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNullable {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE)
                .args(ArrayType.of(TinyIntType.INSTANCE), ArrayType.of(TinyIntType.INSTANCE)),
            FunctionSignature.ret(DoubleType.INSTANCE)
                .args(ArrayType.of(SmallIntType.INSTANCE), ArrayType.of(SmallIntType.INSTANCE)),
            FunctionSignature.ret(DoubleType.INSTANCE)
                .args(ArrayType.of(IntegerType.INSTANCE), ArrayType.of(IntegerType.INSTANCE)),
            FunctionSignature.ret(DoubleType.INSTANCE)
                .args(ArrayType.of(BigIntType.INSTANCE), ArrayType.of(BigIntType.INSTANCE)),
            FunctionSignature.ret(DoubleType.INSTANCE)
                .args(ArrayType.of(LargeIntType.INSTANCE), ArrayType.of(LargeIntType.INSTANCE)),
            FunctionSignature.ret(DoubleType.INSTANCE)
                .args(ArrayType.of(FloatType.INSTANCE), ArrayType.of(FloatType.INSTANCE)),
            FunctionSignature.ret(DoubleType.INSTANCE)
                .args(ArrayType.of(DoubleType.INSTANCE), ArrayType.of(DoubleType.INSTANCE))
    );

    private static final String NAME = "approx_cosine_distance";

    public ApproxCosineDistance(Expression arg0, Expression arg1) {
        super(NAME, arg0, arg1);
    }

    public static String name() {
        return NAME;
    }

    /**
     * withChildren.
     */
    @Override
    public ApproxCosineDistance withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new ApproxCosineDistance(children.get(0), children.get(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitApproxCosineDistance(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}

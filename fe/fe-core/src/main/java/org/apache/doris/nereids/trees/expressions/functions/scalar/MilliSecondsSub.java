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
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullableOnDateLikeV2Args;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'MilliSeconds_sub'.
 */
public class MilliSecondsSub extends ScalarFunction
        implements BinaryExpression, ExplicitlyCastableSignature, PropagateNullableOnDateLikeV2Args {

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DateTimeV2Type.MAX)
                    .args(DateTimeV2Type.MAX, IntegerType.INSTANCE));

    public MilliSecondsSub(Expression arg0, Expression arg1) {
        super("milliseconds_sub", arg0, arg1);
    }

    @Override
    public MilliSecondsSub withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new MilliSecondsSub(children.get(0), children.get(1));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public FunctionSignature computeSignature(FunctionSignature signature) {
        signature = super.computeSignature(signature);
        return signature.withArgumentType(0, DateTimeV2Type.MAX).withReturnType(DateTimeV2Type.MAX);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMilliSecondsSub(this, context);
    }
}

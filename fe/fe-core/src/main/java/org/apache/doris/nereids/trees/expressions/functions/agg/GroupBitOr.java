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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** min agg function. */
public class GroupBitOr extends AggregateFunction
        implements UnaryExpression, PropagateNullable, ExplicitlyCastableSignature {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(TinyIntType.INSTANCE).args(TinyIntType.INSTANCE),
            FunctionSignature.ret(SmallIntType.INSTANCE).args(SmallIntType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).args(IntegerType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(BigIntType.INSTANCE),
            FunctionSignature.ret(LargeIntType.INSTANCE).args(LargeIntType.INSTANCE)
    );

    public GroupBitOr(Expression child) {
        super("group_bit_or", child);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    protected List<DataType> intermediateTypes() {
        return ImmutableList.of(IntegerType.INSTANCE);
    }

    @Override
    public GroupBitOr withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new GroupBitOr(children.get(0));
    }

    @Override
    public GroupBitOr withDistinctAndChildren(boolean isDistinct, List<Expression> children) {
        return withChildren(children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitGroupBitOr(this, context);
    }
}

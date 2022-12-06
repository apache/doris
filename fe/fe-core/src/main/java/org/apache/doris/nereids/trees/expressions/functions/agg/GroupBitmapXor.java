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
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** GroupBitmapXor */
public class GroupBitmapXor extends AggregateFunction
        implements UnaryExpression, PropagateNullable, ExplicitlyCastableSignature {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BitmapType.INSTANCE).args(BitmapType.INSTANCE)
    );

    public GroupBitmapXor(Expression arg0) {
        super("group_bitmap_xor", arg0);
    }

    public GroupBitmapXor(AggregateParam aggregateParam, Expression arg0) {
        super("group_bitmap_xor", aggregateParam, arg0);
    }

    @Override
    protected List<DataType> intermediateTypes(List<DataType> argumentTypes, List<Expression> arguments) {
        return ImmutableList.of(BitmapType.INSTANCE);
    }

    @Override
    public GroupBitmapXor withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new GroupBitmapXor(getAggregateParam(), children.get(0));
    }

    @Override
    public GroupBitmapXor withAggregateParam(AggregateParam aggregateParam) {
        return new GroupBitmapXor(aggregateParam, child());
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}

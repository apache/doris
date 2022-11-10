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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.TypeCollection;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** BitmapUnionInt */
public class BitmapUnionInt extends AggregateFunction
        implements UnaryExpression, PropagateNullable, ImplicitCastInputTypes {
    public BitmapUnionInt(Expression arg0) {
        super("bitmap_union_int", arg0);
    }

    public BitmapUnionInt(AggregateParam aggregateParam, Expression arg0) {
        super("bitmap_union_int", aggregateParam, arg0);
    }

    @Override
    public BitmapUnionInt withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new BitmapUnionInt(getAggregateParam(), children.get(0));
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        if (isGlobal() && inputTypesBeforeDissemble().isPresent()) {
            return ImmutableList.of();
        } else {
            return ImmutableList.of(new TypeCollection(
                    TinyIntType.INSTANCE, SmallIntType.INSTANCE, IntegerType.INSTANCE, BigIntType.INSTANCE));
        }
    }

    @Override
    public DataType getFinalType() {
        return BigIntType.INSTANCE;
    }

    @Override
    public DataType getIntermediateType() {
        return BitmapType.INSTANCE;
    }

    @Override
    public BitmapUnionInt withAggregateParam(AggregateParam aggregateParam) {
        return new BitmapUnionInt(aggregateParam, child());
    }
}

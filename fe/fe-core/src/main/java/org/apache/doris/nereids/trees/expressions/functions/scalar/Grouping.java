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
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.GroupingSetShape;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.GroupingSetShapes;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Grouping Expression.
 */
public class Grouping extends GroupingScalarFunction implements UnaryExpression, CustomSignature {

    public Grouping(Expression child) {
        super("Grouping", ImmutableList.of(child));
    }

    @Override
    public FunctionSignature customSignature(List<DataType> argumentTypes, List<Expression> arguments) {
        // any argument type
        return FunctionSignature.ret(BigIntType.INSTANCE).args(argumentTypes.get(0));
    }

    @Override
    public List<Long> computeVirtualSlotValue(GroupingSetShapes shapes) {
        int index = shapes.indexOf(child());
        return shapes.shapes.stream()
                .map(groupingSetShape -> computeLongValue(groupingSetShape, index))
                .collect(ImmutableList.toImmutableList());
    }

    private long computeLongValue(GroupingSetShape groupingSetShape, int argumentIndex) {
        boolean shouldBeErasedToNull = groupingSetShape.shouldBeErasedToNull(argumentIndex);
        return shouldBeErasedToNull ? 1 : 0;
    }

    @Override
    public Grouping withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Grouping(children.get(0));
    }
}

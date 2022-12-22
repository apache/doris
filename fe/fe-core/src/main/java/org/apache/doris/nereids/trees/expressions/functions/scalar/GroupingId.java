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
import org.apache.doris.nereids.trees.plans.algebra.Repeat.GroupingSetShape;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.GroupingSetShapes;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.BitUtils;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * GroupingId Expression.
 */
public class GroupingId extends GroupingScalarFunction implements CustomSignature {

    // at least one argument
    public GroupingId(Expression firstArg, Expression... varArgs) {
        super("Grouping_Id", ExpressionUtils.mergeArguments(firstArg, varArgs));
    }

    private GroupingId(List<Expression> children) {
        super("Grouping_Id", children);
    }

    @Override
    public FunctionSignature customSignature() {
        // any arguments type
        return FunctionSignature.of(BigIntType.INSTANCE, (List) getArgumentsTypes());
    }

    @Override
    public List<Long> computeVirtualSlotValue(GroupingSetShapes shapes) {
        List<Expression> arguments = getArguments();
        List<Integer> argumentIndexes = arguments.stream()
                .map(shapes::indexOf)
                .collect(ImmutableList.toImmutableList());

        return shapes.shapes.stream()
                .map(groupingSetShape -> toLongValue(groupingSetShape, argumentIndexes))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public GroupingId withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1);
        return new GroupingId(children);
    }

    private long toLongValue(GroupingSetShape groupingSetShape, List<Integer> argumentIndexes) {
        List<Boolean> bits = argumentIndexes.stream()
                .map(index -> groupingSetShape.shouldBeErasedToNull(index))
                .collect(ImmutableList.toImmutableList());
        return BitUtils.bigEndianBitsToLong(bits);
    }
}

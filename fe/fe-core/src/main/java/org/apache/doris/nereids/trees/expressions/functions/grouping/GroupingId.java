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

package org.apache.doris.nereids.trees.expressions.functions.grouping;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.NumericType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * GroupingId Expression.
 */
public class GroupingId extends GroupingSetsFunction implements UnaryExpression, ImplicitCastInputTypes {
    private static final List<AbstractDataType> EXPECTED_INPUT_TYPES = ImmutableList.of(NumericType.INSTANCE);

    public GroupingId(Expression child) {
        super("Grouping_Id", child);
    }

    public GroupingId(Optional<List<Expression>> realChildren, Expression child) {
        super("Grouping_Id", realChildren, child);
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        return EXPECTED_INPUT_TYPES;
    }

    @Override
    public boolean nullable() {
        return child().nullable();
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new GroupingId(realChildren, children.get(0));
    }

    @Override
    public GroupingId repeatChildrenWithVirtualRef(VirtualSlotReference virtualSlotReference,
            Optional<List<Expression>> realChildren) {
        return new GroupingId(realChildren, virtualSlotReference);
    }
}

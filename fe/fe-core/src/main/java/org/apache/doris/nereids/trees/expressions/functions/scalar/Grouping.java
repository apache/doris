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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * Grouping Expression.
 */
public class Grouping extends GroupingScalarFunction implements UnaryExpression {

    public Grouping(Expression child) {
        super("Grouping", child);
    }

    public Grouping(Optional<List<Expression>> realChildren, Expression child) {
        super("Grouping", realChildren, child);
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Grouping(realChildren, children.get(0));
    }

    @Override
    public Grouping repeatChildrenWithVirtualRef(VirtualSlotReference virtualSlotReference,
            Optional<List<Expression>> realChildren) {
        return new Grouping(realChildren, virtualSlotReference);
    }

    @Override
    public boolean hasVarArguments() {
        return false;
    }
}

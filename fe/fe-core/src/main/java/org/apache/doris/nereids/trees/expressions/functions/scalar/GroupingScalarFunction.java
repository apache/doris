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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.GroupingSetShapes;

import java.util.List;

/**
 * Functions used by all groupingSets.
 */
public abstract class GroupingScalarFunction extends ScalarFunction implements AlwaysNotNullable {

    public GroupingScalarFunction(String name, Expression... arguments) {
        super(name, arguments);
    }

    public GroupingScalarFunction(String name, List<Expression> arguments) {
        super(name, arguments);
    }

    /**
     * compute a long value that backend need to fill to the VirtualSlotRef
     */
    public abstract List<Long> computeVirtualSlotValue(GroupingSetShapes shapes);

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitGroupingScalarFunction(this, context);
    }

    @Override
    public abstract GroupingScalarFunction withChildren(List<Expression> children);
}

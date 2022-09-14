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
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;

import java.util.List;
import java.util.Optional;

/**
 * Functions used by all groupingSets.
 */
public abstract class GroupingSetsFunction extends BoundFunction {
    protected final Optional<List<Expression>> realChildren;

    public GroupingSetsFunction(String name, Expression... arguments) {
        super(name, arguments);
        this.realChildren = Optional.empty();
    }

    public GroupingSetsFunction(String name, Optional<List<Expression>> realChildren, Expression... arguments) {
        super(name, arguments);
        this.realChildren = realChildren;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitGroupingSetsFunction(this, context);
    }

    @Override
    public DataType getDataType() {
        return BigIntType.INSTANCE;
    }

    public Optional<List<Expression>> getRealChildren() {
        return realChildren;
    }

    public abstract GroupingSetsFunction repeatChildrenWithVirtualRef(
            VirtualSlotReference virtualSlotReference,
            Optional<List<Expression>> realChildren);
}

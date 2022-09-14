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

import java.util.List;
import java.util.Optional;

/**
 * GroupingId Expression.
 */
public class GroupingId extends GroupingScalarFunction {

    public GroupingId(Expression... children) {
        super("Grouping_Id", children);
    }

    public GroupingId(Optional<List<Expression>> realChildren, Expression... children) {
        super("Grouping_Id", realChildren, children);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        return new GroupingId(realChildren, children.toArray(new Expression[0]));
    }

    @Override
    public GroupingId repeatChildrenWithVirtualRef(VirtualSlotReference virtualSlotReference,
            Optional<List<Expression>> realChildren) {
        return new GroupingId(realChildren, virtualSlotReference);
    }

    @Override
    public boolean hasVarArguments() {
        return true;
    }
}

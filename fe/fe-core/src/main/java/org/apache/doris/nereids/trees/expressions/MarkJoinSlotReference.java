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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;

import com.google.common.collect.ImmutableList;

/**
 * A special type of column that will be generated to replace the subquery when unnesting the subquery of MarkJoin.
 */
public class MarkJoinSlotReference extends SlotReference implements SlotNotFromChildren {
    final boolean existsHasAgg;

    public MarkJoinSlotReference(String name) {
        super(name, BooleanType.INSTANCE, true);
        this.existsHasAgg = false;
    }

    public MarkJoinSlotReference(String name, boolean existsHasAgg) {
        super(name, BooleanType.INSTANCE, true);
        this.existsHasAgg = existsHasAgg;
    }

    public MarkJoinSlotReference(ExprId exprId, String name, boolean existsHasAgg) {
        super(exprId, name, BooleanType.INSTANCE, true, ImmutableList.of());
        this.existsHasAgg = existsHasAgg;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMarkJoinReference(this, context);
    }

    @Override
    public String toString() {
        return super.toString() + "#" + existsHasAgg;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MarkJoinSlotReference that = (MarkJoinSlotReference) o;
        return this.existsHasAgg == that.existsHasAgg && super.equals(that);
    }

    public boolean isExistsHasAgg() {
        return existsHasAgg;
    }

    @Override
    public MarkJoinSlotReference withExprId(ExprId exprId) {
        return new MarkJoinSlotReference(exprId, name, existsHasAgg);
    }
}

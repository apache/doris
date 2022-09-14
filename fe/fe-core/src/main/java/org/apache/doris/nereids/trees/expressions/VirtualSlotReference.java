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
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * it is not a real column exist in table.
 */
public class VirtualSlotReference extends SlotReference {
    private final List<Expression> realSlots;
    private final boolean hasCast;

    public VirtualSlotReference(String name, DataType dataType, List<Expression> realSlots, boolean hasCast) {
        super(name, dataType, false);
        this.realSlots = ImmutableList.copyOf(realSlots);
        this.hasCast = hasCast;
    }

    public VirtualSlotReference(ExprId exprId, String name, DataType dataType,
            boolean nullable, List<String> qualifier,
            List<Expression> realSlots, boolean hasCast) {
        super(exprId, name, dataType, nullable, qualifier);
        this.realSlots = ImmutableList.copyOf(realSlots);
        this.hasCast = hasCast;
    }

    public List<Expression> getRealSlots() {
        return realSlots;
    }

    public boolean hasCast() {
        return hasCast;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitVirtualReference(this, context);
    }

    @Override
    public String toSql() {
        return getName();
    }

    @Override
    public String toString() {
        // Just return name and exprId, add another method to show fully qualified name when it's necessary.
        return getName() + "#" + getExprId() + ' ' + realSlots;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VirtualSlotReference that = (VirtualSlotReference) o;
        return Objects.equals(realSlots, that.realSlots)
                && hasCast == that.hasCast
                && super.equals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(realSlots, hasCast, getExprId());
    }

    @Override
    public SlotReference withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 0);
        return this;
    }
}

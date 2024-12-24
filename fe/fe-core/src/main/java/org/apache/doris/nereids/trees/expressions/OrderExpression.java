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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * OrderExpression.
 * e.g. group_concat(id, ',' order by num desc), the num desc is order expression
 */
public class OrderExpression extends Expression implements UnaryExpression, PropagateNullable {

    private final OrderKey orderKey;

    public OrderExpression(OrderKey orderKey) {
        super(ImmutableList.of(orderKey.getExpr()));
        this.orderKey = orderKey;
    }

    private OrderExpression(List<Expression> children, OrderKey orderKey) {
        super(children);
        this.orderKey = orderKey;
    }

    public boolean isAsc() {
        return orderKey.isAsc();
    }

    public boolean isNullFirst() {
        return orderKey.isNullFirst();
    }

    public OrderKey getOrderKey() {
        return orderKey;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitOrderExpression(this, context);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new OrderExpression(children, new OrderKey(children.get(0), orderKey.isAsc(), orderKey.isNullFirst()));
    }

    @Override
    public DataType getDataType() {
        return child().getDataType();
    }

    @Override
    public String toString() {
        return orderKey.toString();
    }

    @Override
    public String computeToSql() {
        return orderKey.toSql();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        OrderExpression that = (OrderExpression) o;
        return Objects.equals(orderKey, that.orderKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), orderKey);
    }
}

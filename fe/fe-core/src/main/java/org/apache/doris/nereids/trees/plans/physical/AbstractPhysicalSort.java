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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Sort;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Abstract class for all concrete physical sort plan.
 */
public abstract class AbstractPhysicalSort<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> implements Sort {

    protected final ImmutableList<OrderKey> orderKeys;

    /**
     * Constructor of AbstractPhysicalSort.
     */
    public AbstractPhysicalSort(PlanType type, List<OrderKey> orderKeys,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, child);
        this.orderKeys = ImmutableList.copyOf(Objects.requireNonNull(orderKeys, "orderKeys can not be null"));
    }

    /**
     * Constructor of AbstractPhysicalSort.
     */
    public AbstractPhysicalSort(PlanType type, List<OrderKey> orderKeys,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult, CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, physicalProperties, statsDeriveResult, child);
        this.orderKeys = ImmutableList.copyOf(Objects.requireNonNull(orderKeys, "orderKeys can not be null"));
    }

    public List<OrderKey> getOrderKeys() {
        return orderKeys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractPhysicalSort that = (AbstractPhysicalSort) o;
        return Objects.equals(orderKeys, that.orderKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderKeys);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return orderKeys.stream()
                .map(OrderKey::getExpr)
                .collect(ImmutableList.toImmutableList());
    }
}

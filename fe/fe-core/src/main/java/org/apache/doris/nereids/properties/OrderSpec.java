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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

/**
 * Spec of sort order.
 */
public class OrderSpec {
    // TODO: use a OrderKey with ExprId list to instead of current orderKeys for easy to use.
    private final List<OrderKey> orderKeys;

    public OrderSpec() {
        this.orderKeys = Lists.newArrayList();
    }

    public OrderSpec(List<OrderKey> orderKeys) {
        this.orderKeys = orderKeys;
    }

    /**
     * Whether other `OrderSpec` is satisfied the current `OrderSpec`.
     *
     * @param other another OrderSpec.
     */
    public boolean satisfy(OrderSpec other) {
        if (this.orderKeys.size() < other.getOrderKeys().size()) {
            return false;
        }

        for (int i = 0; i < other.getOrderKeys().size(); ++i) {
            if (!this.orderKeys.get(i).matches(other.getOrderKeys().get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * add a local quick sort as order enforcer on child group.
     */
    public GroupExpression addLocalQuickSortEnforcer(Group child) {
        return new GroupExpression(
                new PhysicalQuickSort<>(orderKeys, SortPhase.LOCAL_SORT, child.getLogicalProperties(),
                        new GroupPlan(child)),
                Lists.newArrayList(child)
        );
    }

    /**
     * add a global quick sort as order enforcer on child group.
     */
    public GroupExpression addGlobalQuickSortEnforcer(Group child) {
        return new GroupExpression(
                new PhysicalQuickSort<>(orderKeys, SortPhase.MERGE_SORT, child.getLogicalProperties(),
                        new GroupPlan(child)),
                Lists.newArrayList(child)
        );
    }

    public List<OrderKey> getOrderKeys() {
        return orderKeys;
    }

    @Override
    public String toString() {
        return "Order: (" + orderKeys + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrderSpec that = (OrderSpec) o;
        return orderKeys.equals(that.orderKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderKeys);
    }

}

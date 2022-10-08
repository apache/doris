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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Sort;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * physical local sort for physical properties enforcer.
 */
public class PhysicalLocalQuickSort<CHILD_TYPE extends Plan> extends AbstractPhysicalSort<CHILD_TYPE> implements Sort {

    public PhysicalLocalQuickSort(List<OrderKey> orderKeys,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(orderKeys, Optional.empty(), logicalProperties, child);
    }

    /**
     * Constructor of PhysicalLocalQuickSort.
     */
    public PhysicalLocalQuickSort(List<OrderKey> orderKeys,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.PHYSICAL_LOCAL_QUICK_SORT, orderKeys, groupExpression, logicalProperties, child);
    }

    /**
     * Constructor of PhysicalLocalQuickSort.
     */
    public PhysicalLocalQuickSort(List<OrderKey> orderKeys,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_LOCAL_QUICK_SORT, orderKeys, groupExpression,
                logicalProperties, physicalProperties, statsDeriveResult, child);
    }

    public List<OrderKey> getOrderKeys() {
        return orderKeys;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalLocalQuickSort(this, context);
    }

    @Override
    public PhysicalLocalQuickSort<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalLocalQuickSort<>(orderKeys, getLogicalProperties(), children.get(0));
    }

    @Override
    public PhysicalLocalQuickSort<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalLocalQuickSort<>(orderKeys, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public PhysicalLocalQuickSort<CHILD_TYPE> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalLocalQuickSort<>(orderKeys, Optional.empty(), logicalProperties.get(), child());
    }

    @Override
    public PhysicalLocalQuickSort<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult) {
        return new PhysicalLocalQuickSort<>(orderKeys, Optional.empty(),
                getLogicalProperties(), physicalProperties, statsDeriveResult, child());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalLocalQuickSort",
                "orderKeys", orderKeys);
    }
}

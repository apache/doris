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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.Sort;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * Physical quick sort plan.
 * There are three types:
 *      LOCAL_SORT: sort the local data
 *      MERGE_SORT: LOCAL_SORT -> GATHER -> MERGESORT
 *      GATHER_SORT: SORT the gather data
 *  MERGE_SORT AND GATHER_SORT need require gather for their child
 */
public class PhysicalQuickSort<CHILD_TYPE extends Plan> extends AbstractPhysicalSort<CHILD_TYPE> implements Sort {
    public PhysicalQuickSort(List<OrderKey> orderKeys,
            SortPhase phase, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(orderKeys, phase, Optional.empty(), logicalProperties, child);
    }

    /**
     * Constructor of PhysicalHashJoinNode.
     */
    public PhysicalQuickSort(List<OrderKey> orderKeys,
            SortPhase phase, Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.PHYSICAL_QUICK_SORT, orderKeys, phase, groupExpression, logicalProperties, child);
    }

    /**
     * Constructor of PhysicalQuickSort.
     */
    public PhysicalQuickSort(List<OrderKey> orderKeys,
            SortPhase phase, Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_QUICK_SORT, orderKeys, phase, groupExpression, logicalProperties, physicalProperties,
                statistics, child);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalQuickSort(this, context);
    }

    @Override
    public PhysicalQuickSort<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalQuickSort<>(orderKeys, phase, groupExpression, getLogicalProperties(), physicalProperties,
                statistics, children.get(0));
    }

    @Override
    public PhysicalQuickSort<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalQuickSort<>(orderKeys, phase, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalQuickSort<>(orderKeys, phase, groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalQuickSort<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalQuickSort<>(orderKeys, phase, groupExpression, getLogicalProperties(), physicalProperties,
                statistics, child());
    }

    @Override
    public String shapeInfo() {
        return this.getClass().getSimpleName() + "[" + phase + "]";
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalQuickSort[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "stats", statistics, "orderKeys", orderKeys,
                "phase", phase.toString()
        );
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public PhysicalQuickSort<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalQuickSort<>(orderKeys, phase, groupExpression, null, physicalProperties,
                statistics, child());
    }
}

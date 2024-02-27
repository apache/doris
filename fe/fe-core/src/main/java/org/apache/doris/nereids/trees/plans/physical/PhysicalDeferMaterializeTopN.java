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
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * use for defer materialize top n
 */
public class PhysicalDeferMaterializeTopN<CHILD_TYPE extends Plan>
        extends AbstractPhysicalSort<CHILD_TYPE> implements TopN {

    private final PhysicalTopN<? extends Plan> physicalTopN;

    ///////////////////////////////////////////////////////////////////////////
    // Members for defer materialize for top-n opt.
    ///////////////////////////////////////////////////////////////////////////
    private final Set<ExprId> deferMaterializeSlotIds;
    private final SlotReference columnIdSlot;

    public PhysicalDeferMaterializeTopN(PhysicalTopN<? extends Plan> physicalTopN,
            Set<ExprId> deferMaterializeSlotIds, SlotReference columnIdSlot,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(physicalTopN, deferMaterializeSlotIds, columnIdSlot,
                groupExpression, logicalProperties, null, null, child);
    }

    public PhysicalDeferMaterializeTopN(PhysicalTopN<? extends Plan> physicalTopN,
            Set<ExprId> deferMaterializeSlotIds, SlotReference columnIdSlot,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, CHILD_TYPE child) {
        super(physicalTopN.getType(), physicalTopN.getOrderKeys(), physicalTopN.getSortPhase(),
                groupExpression, logicalProperties, physicalProperties, statistics, child);
        this.physicalTopN = physicalTopN;
        this.deferMaterializeSlotIds = deferMaterializeSlotIds;
        this.columnIdSlot = columnIdSlot;
    }

    public PhysicalTopN<? extends Plan> getPhysicalTopN() {
        return physicalTopN;
    }

    public Set<ExprId> getDeferMaterializeSlotIds() {
        return deferMaterializeSlotIds;
    }

    public SlotReference getColumnIdSlot() {
        return columnIdSlot;
    }

    @Override
    public long getOffset() {
        return physicalTopN.getOffset();
    }

    @Override
    public long getLimit() {
        return physicalTopN.getLimit();
    }

    public PhysicalDeferMaterializeTopN<? extends Plan> withPhysicalTopN(PhysicalTopN<? extends Plan> physicalTopN) {
        return new PhysicalDeferMaterializeTopN<>(physicalTopN, deferMaterializeSlotIds, columnIdSlot, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, physicalTopN.child());
    }

    @Override
    public PhysicalDeferMaterializeTopN<? extends Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "PhysicalDeferMaterializeTopN's children size must be 1, but real is %s", children.size());
        return new PhysicalDeferMaterializeTopN<>(physicalTopN.withChildren(ImmutableList.of(children.get(0))),
                deferMaterializeSlotIds, columnIdSlot, groupExpression, getLogicalProperties(),
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalDeferMaterializeTopN(this, context);
    }

    @Override
    public PhysicalDeferMaterializeTopN<? extends Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalDeferMaterializeTopN<>(physicalTopN, deferMaterializeSlotIds, columnIdSlot,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public PhysicalDeferMaterializeTopN<? extends Plan> withGroupExprLogicalPropChildren(
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "PhysicalDeferMaterializeTopN's children size must be 1, but real is %s", children.size());
        return new PhysicalDeferMaterializeTopN<>(physicalTopN.withChildren(ImmutableList.of(children.get(0))),
                deferMaterializeSlotIds, columnIdSlot, groupExpression, logicalProperties.get(),
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalDeferMaterializeTopN<? extends Plan> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalDeferMaterializeTopN<>(physicalTopN, deferMaterializeSlotIds, columnIdSlot,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public PhysicalDeferMaterializeTopN<? extends Plan> resetLogicalProperties() {
        return new PhysicalDeferMaterializeTopN<>(physicalTopN, deferMaterializeSlotIds, columnIdSlot,
                groupExpression, null, physicalProperties, statistics, child());
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
        PhysicalDeferMaterializeTopN<?> that = (PhysicalDeferMaterializeTopN<?>) o;
        return Objects.equals(physicalTopN, that.physicalTopN) && Objects.equals(
                deferMaterializeSlotIds, that.deferMaterializeSlotIds) && Objects.equals(columnIdSlot,
                that.columnIdSlot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), physicalTopN, deferMaterializeSlotIds, columnIdSlot);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalDeferMaterializeTopN[" + id.asInt() + "]",
                "physicalTopN", physicalTopN,
                "deferMaterializeSlotIds", deferMaterializeSlotIds,
                "columnIdSlot", columnIdSlot
        );
    }
}

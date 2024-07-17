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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * use for defer materialize top n
 */
public class LogicalDeferMaterializeOlapScan extends LogicalCatalogRelation implements OlapScan {

    private final LogicalOlapScan logicalOlapScan;

    ///////////////////////////////////////////////////////////////////////////
    // Members for defer materialize for top-n opt.
    ///////////////////////////////////////////////////////////////////////////
    private final Set<ExprId> deferMaterializeSlotIds;
    private final SlotReference columnIdSlot;

    public LogicalDeferMaterializeOlapScan(LogicalOlapScan logicalOlapScan,
            Set<ExprId> deferMaterializeSlotIds, SlotReference columnIdSlot) {
        this(logicalOlapScan, deferMaterializeSlotIds, columnIdSlot,
                logicalOlapScan.getGroupExpression(), Optional.empty());
    }

    /**
     * constructor
     */
    public LogicalDeferMaterializeOlapScan(LogicalOlapScan logicalOlapScan,
            Set<ExprId> deferMaterializeSlotIds, SlotReference columnIdSlot,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(logicalOlapScan.getRelationId(), logicalOlapScan.getType(), logicalOlapScan.getTable(),
                logicalOlapScan.getQualifier(), groupExpression, logicalProperties);
        this.logicalOlapScan = Objects.requireNonNull(logicalOlapScan, "logicalOlapScan can not be null");
        this.deferMaterializeSlotIds = ImmutableSet.copyOf(Objects.requireNonNull(deferMaterializeSlotIds,
                "deferMaterializeSlotIds can not be null"));
        this.columnIdSlot = Objects.requireNonNull(columnIdSlot, "columnIdSlot can not be null");
    }

    public LogicalOlapScan getLogicalOlapScan() {
        return logicalOlapScan;
    }

    public Set<ExprId> getDeferMaterializeSlotIds() {
        return deferMaterializeSlotIds;
    }

    public SlotReference getColumnIdSlot() {
        return columnIdSlot;
    }

    @Override
    public OlapTable getTable() {
        return logicalOlapScan.getTable();
    }

    @Override
    public long getSelectedIndexId() {
        return logicalOlapScan.getSelectedIndexId();
    }

    @Override
    public List<Long> getSelectedPartitionIds() {
        return logicalOlapScan.getSelectedPartitionIds();
    }

    @Override
    public List<Long> getSelectedTabletIds() {
        return logicalOlapScan.getSelectedPartitionIds();
    }

    @Override
    public List<Slot> computeOutput() {
        return ImmutableList.<Slot>builder()
                .addAll(logicalOlapScan.getOutput())
                .add(columnIdSlot)
                .build();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalDeferMaterializeOlapScan(logicalOlapScan, deferMaterializeSlotIds, columnIdSlot,
                groupExpression, Optional.of(getLogicalProperties()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.isEmpty(), "LogicalDeferMaterializeOlapScan should have no child");
        return new LogicalDeferMaterializeOlapScan(logicalOlapScan, deferMaterializeSlotIds, columnIdSlot,
                groupExpression, logicalProperties);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.isEmpty(), "LogicalDeferMaterializeOlapScan should have no child");
        return this;
    }

    @Override
    public LogicalDeferMaterializeOlapScan withRelationId(RelationId relationId) {
        throw new RuntimeException("should not call LogicalDeferMaterializeOlapScan's withRelationId method");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalDeferMaterializeOlapScan(this, context);
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
        LogicalDeferMaterializeOlapScan that = (LogicalDeferMaterializeOlapScan) o;
        return Objects.equals(logicalOlapScan, that.logicalOlapScan) && Objects.equals(
                deferMaterializeSlotIds, that.deferMaterializeSlotIds) && Objects.equals(columnIdSlot,
                that.columnIdSlot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), logicalOlapScan, deferMaterializeSlotIds, columnIdSlot);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalDeferMaterializeOlapScan[" + id.asInt() + "]",
                "olapScan", logicalOlapScan,
                "deferMaterializeSlotIds", deferMaterializeSlotIds,
                "columnIdSlot", columnIdSlot
        );
    }
}

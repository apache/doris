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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * LogicalWorkTableReference, recursive union's producer child will scan WorkTableReference in every iteration.
 */
public class LogicalWorkTableReference extends LogicalRelation {
    private CTEId cteId;
    private final List<Slot> outputs;
    private final List<String> nameParts;

    public LogicalWorkTableReference(RelationId relationId, CTEId cteId, List<Slot> outputs, List<String> nameParts) {
        this(relationId, cteId, outputs, nameParts, Optional.empty(), Optional.empty());
    }

    private LogicalWorkTableReference(RelationId relationId, CTEId cteId, List<Slot> outputs, List<String> nameParts,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(relationId, PlanType.LOGICAL_WORK_TABLE_REFERENCE, groupExpression, logicalProperties);
        this.cteId = cteId;
        this.outputs = Objects.requireNonNull(outputs);
        this.nameParts = Objects.requireNonNull(nameParts);
    }

    public List<String> getNameParts() {
        return nameParts;
    }

    public String getTableName() {
        return nameParts.stream().map(Utils::quoteIfNeeded)
                .reduce((left, right) -> left + "." + right).orElse("");
    }

    public CTEId getCteId() {
        return cteId;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalWorkTableReference",
                "cteId", cteId,
                "cteName", getTableName());
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
        LogicalWorkTableReference that = (LogicalWorkTableReference) o;
        return cteId.equals(that.cteId) && nameParts.equals(that.nameParts) && outputs.equals(that.outputs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cteId, nameParts, outputs);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalWorkTableReference(relationId, cteId, outputs, nameParts,
                groupExpression, Optional.ofNullable(getLogicalProperties()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalWorkTableReference(relationId, cteId, outputs, nameParts, groupExpression, logicalProperties);
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {

    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {

    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {

    }

    @Override
    public void computeFd(DataTrait.Builder builder) {

    }

    @Override
    public LogicalWorkTableReference withRelationId(RelationId relationId) {
        return new LogicalWorkTableReference(relationId, cteId, outputs, nameParts,
                groupExpression, Optional.ofNullable(getLogicalProperties()));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalWorkTableReference(this, context);
    }

    @Override
    public List<Slot> computeOutput() {
        ImmutableList.Builder<Slot> slots = ImmutableList.builder();
        for (Slot slot : outputs) {
            slots.add(new SlotReference(slot.getName(), slot.getDataType(), slot.nullable(), nameParts));
        }
        return slots.build();
    }
}

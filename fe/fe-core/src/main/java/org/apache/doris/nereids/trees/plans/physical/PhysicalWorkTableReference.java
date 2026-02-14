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
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * PhysicalRecursiveCteScan.
 */
public class PhysicalWorkTableReference extends PhysicalRelation {
    private CTEId cteId;
    private final List<Slot> outputs;
    private final List<String> nameParts;

    public PhysicalWorkTableReference(RelationId relationId, CTEId cteId, List<Slot> outputs, List<String> nameParts,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties) {
        this(relationId, cteId, outputs, nameParts, groupExpression, logicalProperties, PhysicalProperties.ANY, null);
    }

    public PhysicalWorkTableReference(RelationId relationId, CTEId cteId, List<Slot> outputs, List<String> nameParts,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics) {
        super(relationId, PlanType.PHYSICAL_WORK_TABLE_REFERENCE, groupExpression, logicalProperties,
                physicalProperties, statistics);
        this.cteId = cteId;
        this.outputs = Objects.requireNonNull(outputs);
        this.nameParts = Objects.requireNonNull(nameParts);
    }

    public CTEId getCteId() {
        return cteId;
    }

    public List<String> getNameParts() {
        return nameParts;
    }

    public String getTableName() {
        return nameParts.stream().map(Utils::quoteIfNeeded)
                .reduce((left, right) -> left + "." + right).orElse("");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalWorkTableReference(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalWorkTableReference(relationId, cteId, outputs, nameParts, groupExpression,
                getLogicalProperties(),
                physicalProperties, statistics);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalWorkTableReference(relationId, cteId, outputs, nameParts, groupExpression,
                getLogicalProperties(),
                physicalProperties, statistics);
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
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalWorkTableReference(relationId, cteId, outputs, nameParts, groupExpression,
                getLogicalProperties(),
                physicalProperties, statistics);
    }

    @Override
    public List<Slot> getOutput() {
        return outputs;
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
        PhysicalWorkTableReference that = (PhysicalWorkTableReference) o;
        return cteId.equals(that.cteId) && nameParts.equals(that.nameParts) && outputs.equals(that.outputs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cteId, nameParts, outputs);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalWorkTableReference[" + getTableName() + "]" + getGroupIdWithPrefix(),
                "stats", statistics,
                "cteId", cteId,
                "qualified", nameParts);
    }
}

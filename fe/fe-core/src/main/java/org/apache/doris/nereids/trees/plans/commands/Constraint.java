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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.constraint.Constraint.ConstraintType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Constraint
 */
public class Constraint {
    private final ImmutableList<Slot> slots;
    private final LogicalPlan curTable;
    private final @Nullable LogicalPlan referenceTable;
    private final @Nullable ImmutableList<Slot> referenceSlots;
    private final @Nullable String mappingId;
    private final @Nullable ImmutableList<Slot> distributionSlots;
    private final ConstraintType type;

    Constraint(ConstraintType type, LogicalPlan curTable, ImmutableList<Slot> slots) {
        Preconditions.checkArgument(slots != null && !slots.isEmpty(),
                "slots of constraint can't be null or empty");
        this.type = type;
        this.slots = slots;
        this.curTable = Objects.requireNonNull(curTable,
                "table of constraint can't be null");
        this.referenceTable = null;
        this.referenceSlots = null;
        this.mappingId = null;
        this.distributionSlots = null;
    }

    Constraint(LogicalPlan curTable, ImmutableList<Slot> slots,
            LogicalPlan referenceTable, ImmutableList<Slot> referenceSlotSet) {
        Preconditions.checkArgument(slots != null && !slots.isEmpty(),
                "slots of constraint can't be null or empty");
        this.type = ConstraintType.FOREIGN_KEY;
        this.slots = slots;
        this.curTable = Objects.requireNonNull(curTable,
                "table of constraint can't be null");
        this.referenceTable = Objects.requireNonNull(referenceTable,
                "reference table in foreign key can not be null");
        this.referenceSlots = Objects.requireNonNull(referenceSlotSet,
                "reference slots in foreign key can not be null");
        this.mappingId = null;
        this.distributionSlots = null;
        Preconditions.checkArgument(referenceSlots.size() == slots.size(),
                "Foreign key's size must be same as the size of reference slots");
    }

    Constraint(LogicalPlan curTable, String mappingId, ImmutableList<Slot> determinantSlots,
            ImmutableList<Slot> distributionSlots) {
        Preconditions.checkArgument(determinantSlots != null && !determinantSlots.isEmpty(),
                "determinant slots of distribution mapping constraint can't be null or empty");
        Preconditions.checkArgument(distributionSlots != null && !distributionSlots.isEmpty(),
                "distribution slots of distribution mapping constraint can't be null or empty");
        this.type = ConstraintType.DISTRIBUTION_MAPPING;
        this.slots = determinantSlots;
        this.curTable = Objects.requireNonNull(curTable, "table of constraint can't be null");
        this.referenceTable = null;
        this.referenceSlots = null;
        this.mappingId = Objects.requireNonNull(mappingId, "mapping id can't be null");
        this.distributionSlots = distributionSlots;
    }

    public static Constraint newUniqueConstraint(LogicalPlan curTable, ImmutableList<Slot> slotSet) {
        return new Constraint(ConstraintType.UNIQUE, curTable, slotSet);
    }

    public static Constraint newPrimaryKeyConstraint(LogicalPlan curTable, ImmutableList<Slot> slotSet) {
        return new Constraint(ConstraintType.PRIMARY_KEY, curTable, slotSet);
    }

    public static Constraint newForeignKeyConstraint(
            LogicalPlan curTable, ImmutableList<Slot> slotSet,
            LogicalPlan referenceTable, ImmutableList<Slot> referenceSlotSet) {
        return new Constraint(curTable, slotSet, referenceTable, referenceSlotSet);
    }

    public static Constraint newDistributionMappingConstraint(LogicalPlan curTable, String mappingId,
            ImmutableList<Slot> determinantSlots, ImmutableList<Slot> distributionSlots) {
        return new Constraint(curTable, mappingId, determinantSlots, distributionSlots);
    }

    public boolean isForeignKey() {
        return type == ConstraintType.FOREIGN_KEY;
    }

    public boolean isUnique() {
        return type == ConstraintType.UNIQUE;
    }

    public boolean isPrimaryKey() {
        return type == ConstraintType.PRIMARY_KEY;
    }

    public boolean isDistributionMapping() {
        return type == ConstraintType.DISTRIBUTION_MAPPING;
    }

    public String getMappingId() {
        return Objects.requireNonNull(mappingId, "mapping id is only available for distribution mapping constraint");
    }

    public LogicalPlan toProject() {
        return new LogicalProject<>(ImmutableList.copyOf(slots), curTable);
    }

    public LogicalPlan toReferenceProject() {
        Preconditions.checkArgument(referenceSlots != null, "Reference slot set of foreign key cannot be null");
        return new LogicalProject<>(ImmutableList.copyOf(referenceSlots), referenceTable);
    }

    public LogicalPlan toDistributionProject() {
        Preconditions.checkArgument(distributionSlots != null,
                "distribution slots are only available for distribution mapping constraint");
        return new LogicalProject<>(ImmutableList.copyOf(distributionSlots), curTable);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Constraint Type: ").append(type).append("\n");
        sb.append("Slot Set: ").append(slots).append("\n");
        sb.append("Current Table: ").append(curTable).append("\n");
        if (type.equals(ConstraintType.FOREIGN_KEY)) {
            sb.append("Reference Table: ").append(referenceTable).append("\n");
            sb.append("Reference Slot Set: ").append(referenceSlots);
        } else if (type.equals(ConstraintType.DISTRIBUTION_MAPPING)) {
            sb.append("Mapping Id: ").append(mappingId).append("\n");
            sb.append("Distribution Slot Set: ").append(distributionSlots);
        }
        return sb.toString();
    }
}

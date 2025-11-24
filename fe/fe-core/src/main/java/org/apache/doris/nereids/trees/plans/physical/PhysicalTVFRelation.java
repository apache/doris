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

import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.TVFRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** PhysicalTableValuedFunctionRelation */
public class PhysicalTVFRelation extends PhysicalRelation implements TVFRelation, BlockFuncDepsPropagation {

    protected final TableValuedFunction function;
    protected final ImmutableList<Slot> operativeSlots;

    public PhysicalTVFRelation(RelationId id, TableValuedFunction function, Collection<Slot> operativeSlots,
            LogicalProperties logicalProperties) {
        super(id, PlanType.PHYSICAL_TVF_RELATION, Optional.empty(), logicalProperties);
        this.operativeSlots = ImmutableList.copyOf(operativeSlots);
        this.function = Objects.requireNonNull(function, "function can not be null");
    }

    public PhysicalTVFRelation(RelationId id, TableValuedFunction function, Collection<Slot> operativeSlots,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties, Statistics statistics) {
        super(id, PlanType.PHYSICAL_TVF_RELATION, groupExpression,
                logicalProperties, physicalProperties, statistics);
        this.operativeSlots = ImmutableList.copyOf(operativeSlots);
        this.function = Objects.requireNonNull(function, "function can not be null");
    }

    @Override
    public PhysicalTVFRelation withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalTVFRelation(relationId, function, operativeSlots, groupExpression, getLogicalProperties(),
                physicalProperties, statistics);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalTVFRelation(relationId, function, operativeSlots, groupExpression,
                logicalProperties.get(), physicalProperties, statistics);
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalTVFRelation(relationId, function, operativeSlots, Optional.empty(),
                getLogicalProperties(), physicalProperties, statistics);
    }

    public List<Slot> getOperativeSlots() {
        return operativeSlots;
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
        PhysicalTVFRelation that = (PhysicalTVFRelation) o;
        return Objects.equals(function, that.function) && Objects.equals(operativeSlots, that.operativeSlots);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), function);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalTVFRelation",
                "qualified", Utils.qualifiedName(ImmutableList.of(), function.getTable().getName()),
                "output", getOutput(),
                "function", function.toSql(),
                "operativeSlots", operativeSlots
        );
    }

    @Override
    public List<Slot> computeOutput() {
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        return function.getTable().getBaseSchema()
                .stream()
                .map(col -> SlotReference.fromColumn(
                        exprIdGenerator.getNextId(), function.getTable(), col, ImmutableList.of())
                )
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalTVFRelation(this, context);
    }

    @Override
    public TableValuedFunction getFunction() {
        return function;
    }
}

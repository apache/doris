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

import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
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
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** LogicalTableValuedFunctionRelation */
public class LogicalTVFRelation extends LogicalRelation implements TVFRelation, BlockFuncDepsPropagation {

    private final TableValuedFunction function;
    private final ImmutableList<String> qualifier;
    private final ImmutableList<Slot> operativeSlots;
    private final Optional<List<Slot>> cachedOutputs;

    public LogicalTVFRelation(RelationId id, TableValuedFunction function, ImmutableList<Slot> operativeSlots) {
        super(id, PlanType.LOGICAL_TVF_RELATION);
        this.operativeSlots = operativeSlots;
        this.function = function;
        this.qualifier = ImmutableList.of(TableValuedFunctionIf.TVF_TABLE_PREFIX + function.getName());
        this.cachedOutputs = Optional.empty();
    }

    public LogicalTVFRelation(RelationId id, TableValuedFunction function, ImmutableList<Slot> operativeSlots,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        this(id, function, operativeSlots, Optional.empty(), groupExpression, logicalProperties);
    }

    public LogicalTVFRelation(RelationId id, TableValuedFunction function,
            ImmutableList<Slot> operativeSlots,
            Optional<List<Slot>> cachedOutputs,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        super(id, PlanType.LOGICAL_TVF_RELATION, groupExpression, logicalProperties);
        this.operativeSlots = operativeSlots;
        this.function = function;
        this.cachedOutputs = Objects.requireNonNull(cachedOutputs, "cachedOutputs can not be null");
        this.qualifier = ImmutableList.of(TableValuedFunctionIf.TVF_TABLE_PREFIX + function.getName());
    }

    @Override
    public LogicalTVFRelation withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalTVFRelation(relationId, function, operativeSlots,
                groupExpression, Optional.of(getLogicalProperties()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalTVFRelation(relationId, function, operativeSlots, groupExpression, logicalProperties);
    }

    @Override
    public LogicalTVFRelation withRelationId(RelationId relationId) {
        return new LogicalTVFRelation(relationId, function, operativeSlots, Optional.empty(),
                Optional.of(getLogicalProperties()));
    }

    public LogicalTVFRelation withOperativeSlots(Collection<Slot> operativeSlots) {
        return new LogicalTVFRelation(relationId, function, Utils.fastToImmutableList(operativeSlots),
                Optional.empty(), Optional.of(getLogicalProperties()));
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
        LogicalTVFRelation that = (LogicalTVFRelation) o;
        return Objects.equals(function, that.function) && Objects.equals(operativeSlots, that.operativeSlots);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), function);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalTVFRelation",
                "output", getOutput(),
                "function", function.toSql()
        );
    }

    @Override
    public List<Slot> computeOutput() {
        if (cachedOutputs.isPresent()) {
            return cachedOutputs.get();
        }
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        return function.getTable().getBaseSchema()
                .stream()
                .map(col -> SlotReference.fromColumn(
                        exprIdGenerator.getNextId(), function.getTable(), col, qualifier)
                )
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalTVFRelation(this, context);
    }

    @Override
    public TableValuedFunction getFunction() {
        return function;
    }

    public LogicalTVFRelation withCachedOutputs(List<Slot> replaceSlots) {
        return new LogicalTVFRelation(relationId, function, Utils.fastToImmutableList(operativeSlots),
                Optional.of(replaceSlots), Optional.empty(), Optional.empty());
    }
}

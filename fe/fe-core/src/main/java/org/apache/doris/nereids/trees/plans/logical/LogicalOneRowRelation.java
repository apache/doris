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
import org.apache.doris.nereids.properties.ExprFdItem;
import org.apache.doris.nereids.properties.FdFactory;
import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A relation that contains only one row consist of some constant expressions.
 * e.g. select 100, 'value'
 */
public class LogicalOneRowRelation extends LogicalRelation implements OneRowRelation, OutputPrunable {

    private final List<NamedExpression> projects;

    public LogicalOneRowRelation(RelationId relationId, List<NamedExpression> projects) {
        this(relationId, projects, Optional.empty(), Optional.empty());
    }

    private LogicalOneRowRelation(RelationId relationId, List<NamedExpression> projects,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(relationId, PlanType.LOGICAL_ONE_ROW_RELATION, groupExpression, logicalProperties);
        this.projects = ImmutableList.copyOf(Objects.requireNonNull(projects, "projects can not be null"));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOneRowRelation(this, context);
    }

    @Override
    public List<NamedExpression> getProjects() {
        return projects;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return projects;
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOneRowRelation(relationId, projects, groupExpression, Optional.of(getLogicalProperties()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalOneRowRelation(relationId, projects, groupExpression, logicalProperties);
    }

    @Override
    public LogicalOneRowRelation withRelationId(RelationId relationId) {
        throw new RuntimeException("should not call LogicalOneRowRelation's withRelationId method");
    }

    @Override
    public List<Slot> computeOutput() {
        return projects.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
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
        LogicalOneRowRelation that = (LogicalOneRowRelation) o;
        return Objects.equals(projects, that.projects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), projects);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalOneRowRelation",
                "projects", projects
        );
    }

    public LogicalOneRowRelation withProjects(List<NamedExpression> namedExpressions) {
        return new LogicalOneRowRelation(relationId, namedExpressions, Optional.empty(), Optional.empty());
    }

    @Override
    public List<NamedExpression> getOutputs() {
        return projects;
    }

    @Override
    public Plan pruneOutputs(List<NamedExpression> prunedOutputs) {
        return withProjects(prunedOutputs);
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        getOutput().forEach(builder::addUniqueSlot);
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        getOutput().forEach(builder::addUniformSlot);
    }

    @Override
    public ImmutableSet<FdItem> computeFdItems() {
        Set<NamedExpression> output = ImmutableSet.copyOf(getOutput());
        ImmutableSet.Builder<FdItem> builder = ImmutableSet.builder();
        ImmutableSet<SlotReference> slotSet = output.stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .collect(ImmutableSet.toImmutableSet());
        ExprFdItem fdItem = FdFactory.INSTANCE.createExprFdItem(slotSet, true, slotSet);
        builder.add(fdItem);

        return builder.build();
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        Map<Expression, NamedExpression> aliasMap = new HashMap<>();
        for (NamedExpression namedExpr : getOutputs()) {
            if (namedExpr instanceof Alias) {
                if (aliasMap.containsKey(namedExpr.child(0))) {
                    builder.addEqualPair(namedExpr.toSlot(), aliasMap.get(namedExpr.child(0)).toSlot());
                }
                aliasMap.put(namedExpr.child(0), namedExpr);
            }
        }
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        // don't generate
    }
}

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
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.EmptyRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A logical relation that contains empty row.
 * e.g.
 * select * from tbl limit 0
 */
public class LogicalEmptyRelation extends LogicalRelation
        implements EmptyRelation, OutputPrunable, BlockFuncDepsPropagation {

    private final List<NamedExpression> projects;

    public LogicalEmptyRelation(RelationId relationId, List<? extends NamedExpression> projects) {
        this(relationId, projects, Optional.empty(), Optional.empty());
    }

    public LogicalEmptyRelation(RelationId relationId, List<? extends NamedExpression> projects,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(relationId, PlanType.LOGICAL_EMPTY_RELATION, groupExpression, logicalProperties);
        this.projects = ImmutableList.copyOf(Objects.requireNonNull(projects, "projects can not be null"));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalEmptyRelation(this, context);
    }

    @Override
    public List<NamedExpression> getProjects() {
        return projects;
    }

    public LogicalEmptyRelation withProjects(List<? extends NamedExpression> projects) {
        return new LogicalEmptyRelation(relationId, projects);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalEmptyRelation(relationId, projects,
                groupExpression, Optional.of(logicalPropertiesSupplier.get()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalEmptyRelation(relationId, projects, groupExpression, logicalProperties);
    }

    @Override
    public LogicalEmptyRelation withRelationId(RelationId relationId) {
        throw new RuntimeException("should not call LogicalEmptyRelation's withRelationId method");
    }

    @Override
    public List<Slot> computeOutput() {
        return projects.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalEmptyRelation",
                "projects", projects
        );
    }

    @Override
    public List<NamedExpression> getOutputs() {
        return projects;
    }

    @Override
    public Plan pruneOutputs(List<NamedExpression> prunedOutputs) {
        return withProjects(prunedOutputs);
    }
}

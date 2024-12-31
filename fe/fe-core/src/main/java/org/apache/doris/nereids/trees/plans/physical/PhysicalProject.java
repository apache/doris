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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Physical project plan.
 */
public class PhysicalProject<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> implements Project {

    private final List<NamedExpression> projects;
    private final Supplier<Set<NamedExpression>> projectsSet;
    //multiLayerProjects is used to extract common expressions
    // projects: (A+B) * 2, (A+B) * 3
    // multiLayerProjects:
    //            L1: A+B as x
    //            L2: x*2, x*3
    private List<List<NamedExpression>> multiLayerProjects = Lists.newArrayList();

    public PhysicalProject(List<NamedExpression> projects, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(projects, Optional.empty(), logicalProperties, child);
    }

    public PhysicalProject(List<NamedExpression> projects, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_PROJECT, groupExpression, logicalProperties, child);
        this.projects = ImmutableList.copyOf(Objects.requireNonNull(projects, "projects can not be null"));
        this.projectsSet = Suppliers.memoize(() -> ImmutableSet.copyOf(this.projects));
    }

    public PhysicalProject(List<NamedExpression> projects, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_PROJECT, groupExpression, logicalProperties, physicalProperties, statistics,
                child);
        this.projects = ImmutableList.copyOf(Objects.requireNonNull(projects, "projects can not be null"));
        this.projectsSet = Suppliers.memoize(() -> ImmutableSet.copyOf(this.projects));
    }

    public List<NamedExpression> getProjects() {
        return projects;
    }

    @Override
    public String toString() {
        StringBuilder cse = new StringBuilder();
        for (int i = 0; i < multiLayerProjects.size(); i++) {
            List<NamedExpression> layer = multiLayerProjects.get(i);
            cse.append("l").append(i).append("(").append(layer).append(")");
        }
        return Utils.toSqlString("PhysicalProject[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "stats", statistics, "projects", projects, "multi_proj", cse.toString()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalProject<?> that = (PhysicalProject<?>) o;
        return projectsSet.get().equals(that.projectsSet.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectsSet.get());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalProject(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return projects;
    }

    @Override
    public PhysicalProject<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalProject<>(projects,
                groupExpression,
                getLogicalProperties(),
                physicalProperties,
                statistics,
                children.get(0)
        );
    }

    @Override
    public PhysicalProject<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalProject<>(projects, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalProject<>(projects, groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalProject<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalProject<>(projects, groupExpression, getLogicalProperties(), physicalProperties,
                statistics, child());
    }

    /**
     * replace projections and child, it is used for merge consecutive projections.
     * @param projections new projections
     * @param child new child
     * @return new project
     */
    public PhysicalProject<Plan> withProjectionsAndChild(List<NamedExpression> projections, Plan child) {
        return new PhysicalProject<>(ImmutableList.copyOf(projections),
                groupExpression,
                getLogicalProperties(),
                physicalProperties,
                statistics,
                child
        );
    }

    @Override
    public List<Slot> computeOutput() {
        List<NamedExpression> output = projects;
        if (! multiLayerProjects.isEmpty()) {
            int layers = multiLayerProjects.size();
            output = multiLayerProjects.get(layers - 1);
        }
        return output.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public PhysicalProject<CHILD_TYPE> reComputeOutput() {
        DataTrait dataTrait = getLogicalProperties().getTrait();
        LogicalProperties newLogicalProperties = new LogicalProperties(this::computeOutput, () -> dataTrait);
        return new PhysicalProject<>(projects, groupExpression, newLogicalProperties, physicalProperties,
                statistics, child());
    }

    public boolean hasMultiLayerProjection() {
        return !multiLayerProjects.isEmpty();
    }

    public List<List<NamedExpression>> getMultiLayerProjects() {
        return multiLayerProjects;
    }

    public void setMultiLayerProjects(List<List<NamedExpression>> multiLayers) {
        this.multiLayerProjects = multiLayers;
    }
}

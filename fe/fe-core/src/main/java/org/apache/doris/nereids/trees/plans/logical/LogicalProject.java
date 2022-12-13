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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical project plan.
 */
public class LogicalProject<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Project {

    private final ImmutableList<NamedExpression> projects;
    private final ImmutableList<NamedExpression> excepts;

    public LogicalProject(List<NamedExpression> projects, List<NamedExpression> excepts, CHILD_TYPE child) {
        this(projects, excepts, Optional.empty(), Optional.empty(), child);
    }

    public LogicalProject(List<NamedExpression> projects, CHILD_TYPE child) {
        this(projects, Collections.emptyList(), child);
    }

    /**
     * Constructor for LogicalProject.
     *
     * @param projects project list
     */
    public LogicalProject(List<NamedExpression> projects, List<NamedExpression> excepts,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.LOGICAL_PROJECT, groupExpression, logicalProperties, child);
        this.projects = ImmutableList.copyOf(Objects.requireNonNull(projects, "projects can not be null"));
        this.excepts = ImmutableList.copyOf(excepts);
    }

    /**
     * Get project list.
     *
     * @return all project of this node.
     */
    @Override
    public List<NamedExpression> getProjects() {
        return projects;
    }

    public List<NamedExpression> getExcepts() {
        return excepts;
    }

    @Override
    public List<Slot> computeOutput() {
        return projects.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalProject",
                "projects", projects,
                "excepts", excepts
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalProject(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return projects;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalProject that = (LogicalProject) o;
        return projects.equals(that.projects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projects);
    }

    @Override
    public LogicalUnary<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalProject<>(projects, excepts, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalProject<>(projects, excepts, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalProject<>(projects, excepts, Optional.empty(), logicalProperties, child());
    }
}

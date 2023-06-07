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
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * LogicalCTEProducer
 */
public class LogicalCTEProducer<CHILD_TYPE extends Plan>
        extends LogicalUnary<CHILD_TYPE> {

    private final CTEId cteId;

    private final List<Slot> projects;

    private final boolean rewritten;

    public LogicalCTEProducer(CHILD_TYPE child, CTEId cteId) {
        super(PlanType.LOGICAL_CTE_PRODUCER, child);
        this.cteId = cteId;
        this.projects = ImmutableList.of();
        this.rewritten = false;
    }

    public LogicalCTEProducer(Optional<GroupExpression> groupExpression,
                              Optional<LogicalProperties> logicalProperties, CHILD_TYPE child, CTEId cteId,
                              List<Slot> projects, boolean rewritten) {
        super(PlanType.LOGICAL_CTE_PRODUCER, groupExpression, logicalProperties, child);
        this.cteId = cteId;
        this.projects = ImmutableList.copyOf(Objects.requireNonNull(projects,
                "projects should not null"));
        this.rewritten = rewritten;
    }

    public CTEId getCteId() {
        return cteId;
    }

    public List<Slot> getProjects() {
        return projects;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalCTEProducer<>(groupExpression, Optional.of(getLogicalProperties()), children.get(0),
                cteId, projects, rewritten);
    }

    public Plan withChildrenAndProjects(List<Plan> children, List<Slot> projects, boolean rewritten) {
        return new LogicalCTEProducer<>(groupExpression, Optional.of(getLogicalProperties()), children.get(0),
                cteId, projects, rewritten);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCTEProducer(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return child().getExpressions();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCTEProducer<>(groupExpression, Optional.of(getLogicalProperties()), child(), cteId,
            projects, rewritten);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalCTEProducer<>(groupExpression, logicalProperties, child(), cteId,
            projects, rewritten);
    }

    @Override
    public List<Slot> computeOutput() {
        return child().computeOutput();
    }

    @Override
    public String toString() {
        return String.format("LOGICAL_CTE_PRODUCER#%d", cteId.asInt());
    }

    public boolean isRewritten() {
        return rewritten;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cteId, projects, rewritten);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalCTEProducer p = (LogicalCTEProducer) o;
        if (cteId != p.cteId) {
            return false;
        }
        if (rewritten != p.rewritten) {
            return false;
        }
        return projects.equals(p.projects);
    }
}

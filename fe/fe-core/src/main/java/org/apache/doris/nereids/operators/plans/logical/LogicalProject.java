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

package org.apache.doris.nereids.operators.plans.logical;

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

/**
 * Logical project plan operator.
 */
public class LogicalProject extends LogicalUnaryOperator {

    private final List<NamedExpression> projects;

    /**
     * Constructor for LogicalProject.
     *
     * @param projects project list
     */
    public LogicalProject(List<NamedExpression> projects) {
        super(OperatorType.LOGICAL_PROJECT);
        this.projects = ImmutableList.copyOf(Objects.requireNonNull(projects, "projects can not be null"));
    }

    /**
     * Get project list.
     *
     * @return all project of this node.
     */
    public List<NamedExpression> getProjects() {
        return projects;
    }

    @Override
    public List<Slot> computeOutput(Plan input) {
        return projects.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public String toString() {
        return "LogicalProject (" + StringUtils.join(projects, ", ") + ")";
    }

    @Override
    public List<Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>().addAll(projects).build();
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
}

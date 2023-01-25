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
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A physical relation that contains only one row consist of some constant expressions.
 * e.g. select 100, 'value'
 */
public class PhysicalOneRowRelation extends PhysicalLeaf implements OneRowRelation {
    private final ImmutableList<NamedExpression> projects;
    private final boolean buildUnionNode;

    public PhysicalOneRowRelation(List<NamedExpression> projects, boolean buildUnionNode,
            LogicalProperties logicalProperties) {
        this(projects, buildUnionNode, Optional.empty(), logicalProperties, null, null);
    }

    private PhysicalOneRowRelation(List<NamedExpression> projects,
            boolean buildUnionNode,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult) {
        super(PlanType.PHYSICAL_ONE_ROW_RELATION, groupExpression, logicalProperties, physicalProperties,
                statsDeriveResult);
        this.projects = ImmutableList.copyOf(Objects.requireNonNull(projects, "projects can not be null"));
        this.buildUnionNode = buildUnionNode;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalOneRowRelation(this, context);
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
        return new PhysicalOneRowRelation(projects, buildUnionNode, groupExpression,
                logicalPropertiesSupplier.get(), physicalProperties, statsDeriveResult);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalOneRowRelation(projects, buildUnionNode, Optional.empty(),
                logicalProperties.get(), physicalProperties, statsDeriveResult);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalOneRowRelation",
                "expressions", projects,
                "buildUnionNode", buildUnionNode
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
        PhysicalOneRowRelation that = (PhysicalOneRowRelation) o;
        return Objects.equals(projects, that.projects)
                && buildUnionNode == that.buildUnionNode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(projects, buildUnionNode);
    }

    @Override
    public PhysicalOneRowRelation withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult) {
        return new PhysicalOneRowRelation(projects, buildUnionNode, Optional.empty(),
                logicalPropertiesSupplier.get(), physicalProperties, statsDeriveResult);
    }

    public boolean notBuildUnionNode() {
        return !buildUnionNode;
    }
}

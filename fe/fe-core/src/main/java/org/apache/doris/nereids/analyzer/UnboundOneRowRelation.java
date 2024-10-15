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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * A relation that contains only one row consist of some constant expressions.
 * e.g. select 100, 'value'
 */
public class UnboundOneRowRelation extends LogicalRelation implements Unbound, OneRowRelation,
        BlockFuncDepsPropagation {

    private final List<NamedExpression> projects;

    public UnboundOneRowRelation(RelationId relationId, List<NamedExpression> projects) {
        this(relationId, projects, Optional.empty(), Optional.empty());
    }

    private UnboundOneRowRelation(RelationId id,
                                  List<NamedExpression> projects,
                                  Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        super(id, PlanType.LOGICAL_UNBOUND_ONE_ROW_RELATION, groupExpression, logicalProperties);
        this.projects = ImmutableList.copyOf(projects);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundOneRowRelation(this, context);
    }

    @Override
    public List<NamedExpression> getProjects() {
        return projects;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " don't support getExpression()");
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new UnboundOneRowRelation(relationId, projects,
                groupExpression, Optional.of(logicalPropertiesSupplier.get()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new UnboundOneRowRelation(relationId, projects, groupExpression, logicalProperties);
    }

    @Override
    public UnboundOneRowRelation withRelationId(RelationId relationId) {
        throw new UnboundException("should not call UnboundOneRowRelation's withRelationId method");
    }

    @Override
    public List<Slot> computeOutput() {
        throw new UnboundException("output");
    }

    @Override
    public LogicalProperties computeLogicalProperties() {
        return UnboundLogicalProperties.INSTANCE;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("UnboundOneRowRelation",
                "relationId", relationId,
                "projects", projects
        );
    }
}

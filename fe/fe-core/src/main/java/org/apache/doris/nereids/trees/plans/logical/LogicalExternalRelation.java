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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * abstract class catalog relation for logical relation
 */
public abstract class LogicalExternalRelation extends LogicalCatalogRelation {

    // TODO remove this conjuncts when old planner is removed
    protected final Set<Expression> conjuncts;

    public LogicalExternalRelation(RelationId relationId, PlanType type, TableIf table, List<String> qualifier,
            Set<Expression> conjuncts,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(relationId, type, table, qualifier, groupExpression, logicalProperties);
        this.conjuncts = ImmutableSet.copyOf(Objects.requireNonNull(conjuncts, "conjuncts should not be null"));
    }

    public abstract LogicalExternalRelation withConjuncts(Set<Expression> conjuncts);

    @Override
    public abstract LogicalExternalRelation withRelationId(RelationId relationId);

    public Set<Expression> getConjuncts() {
        return conjuncts;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && Objects.equals(conjuncts, ((LogicalExternalRelation) o).conjuncts);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalExternalRelation(this, context);
    }
}

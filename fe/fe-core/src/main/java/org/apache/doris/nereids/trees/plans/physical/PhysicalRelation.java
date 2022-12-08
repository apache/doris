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
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Scan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Abstract class for all physical scan plan.
 */
public abstract class PhysicalRelation extends PhysicalLeaf implements Scan {

    protected final ImmutableList<String> qualifier;

    protected final RelationId id;

    /**
     * Constructor for PhysicalRelation.
     */
    public PhysicalRelation(RelationId id, PlanType type, List<String> qualifier,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties) {
        super(type, groupExpression, logicalProperties);
        this.qualifier = ImmutableList.copyOf(Objects.requireNonNull(qualifier, "qualifier can not be null"));
        this.id = id;
    }

    /**
     * Constructor for PhysicalRelation.
     */
    public PhysicalRelation(RelationId id, PlanType type, List<String> qualifier,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult) {
        super(type, groupExpression, logicalProperties, physicalProperties, statsDeriveResult);
        this.qualifier = ImmutableList.copyOf(Objects.requireNonNull(qualifier, "qualifier can not be null"));
        this.id = id;
    }

    public List<String> getQualifier() {
        return qualifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalRelation that = (PhysicalRelation) o;
        return this.id.equals(that.id) && Objects.equals(qualifier, that.qualifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qualifier);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalScan(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    public RelationId getId() {
        return id;
    }
}

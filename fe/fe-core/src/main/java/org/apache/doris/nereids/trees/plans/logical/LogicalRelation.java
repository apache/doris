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
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.collect.ImmutableList;
import org.json.JSONObject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical relation plan.
 */
public abstract class LogicalRelation extends LogicalLeaf implements Relation {

    protected final RelationId relationId;

    public LogicalRelation(RelationId relationId, PlanType type) {
        this(relationId, type, Optional.empty(), Optional.empty());
    }

    public LogicalRelation(RelationId relationId, PlanType type,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(type, groupExpression, logicalProperties);
        this.relationId = relationId;

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalRelation that = (LogicalRelation) o;
        return this.relationId.equals(that.getRelationId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(relationId);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalRelation(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    public RelationId getRelationId() {
        return relationId;
    }

    @Override
    public JSONObject toJson() {
        JSONObject logicalRelation = super.toJson();
        JSONObject properties = new JSONObject();
        properties.put("RelationId", relationId.toString());
        logicalRelation.put("Properties", properties);
        return logicalRelation;
    }
}

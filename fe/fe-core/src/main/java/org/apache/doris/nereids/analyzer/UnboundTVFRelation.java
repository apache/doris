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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.TVFProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeaf;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** UnboundTVFRelation */
public class UnboundTVFRelation extends LogicalLeaf implements Relation, Unbound {
    private final String functionName;
    private final TVFProperties properties;

    public UnboundTVFRelation(String functionName, TVFProperties properties) {
        this(functionName, properties, Optional.empty(), Optional.empty());
    }

    public UnboundTVFRelation(String functionName, TVFProperties properties,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(PlanType.LOGICAL_UNBOUND_TVF_RELATION, groupExpression, logicalProperties);
        this.functionName = Objects.requireNonNull(functionName, "functionName can not be null");
        this.properties = Objects.requireNonNull(properties, "properties can not be null");
    }

    public String getFunctionName() {
        return functionName;
    }

    public TVFProperties getProperties() {
        return properties;
    }

    @Override
    public LogicalProperties computeLogicalProperties() {
        return UnboundLogicalProperties.INSTANCE;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundTVFRelation(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " don't support getExpression()");
    }

    @Override
    public List<Slot> computeOutput() {
        throw new UnboundException("output");
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new UnboundTVFRelation(functionName, properties, groupExpression, Optional.of(getLogicalProperties()));
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new UnboundTVFRelation(functionName, properties, Optional.empty(), logicalProperties);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("UnboundTVFRelation",
                "functionName", functionName,
                "arguments", properties
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
        if (!super.equals(o)) {
            return false;
        }
        UnboundTVFRelation relation = (UnboundTVFRelation) o;
        return Objects.equals(functionName, relation.functionName) && Objects.equals(properties,
                relation.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), functionName, properties);
    }
}

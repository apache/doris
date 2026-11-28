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
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.TVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** UnboundTVFRelation */
public class UnboundTVFRelation extends LogicalRelation implements TVFRelation, Unbound,
        BlockFuncDepsPropagation {

    private final String functionName;
    private final Properties properties;
    private final Map<String, Placeholder> propertyParameters;

    public UnboundTVFRelation(RelationId id, String functionName, Properties properties) {
        this(id, functionName, properties, Optional.empty(), Optional.empty());
    }

    public UnboundTVFRelation(RelationId id, String functionName, Properties properties,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        this(id, functionName, properties, ImmutableMap.of(), groupExpression, logicalProperties);
    }

    public UnboundTVFRelation(RelationId id, String functionName, Properties properties,
            Map<String, Placeholder> propertyParameters) {
        this(id, functionName, properties, propertyParameters, Optional.empty(), Optional.empty());
    }

    private UnboundTVFRelation(RelationId id, String functionName, Properties properties,
            Map<String, Placeholder> propertyParameters, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        super(id, PlanType.LOGICAL_UNBOUND_TVF_RELATION, groupExpression, logicalProperties);
        this.propertyParameters = ImmutableMap.copyOf(propertyParameters);
        this.functionName = Objects.requireNonNull(functionName, "functionName can not be null");
        this.properties = Objects.requireNonNull(properties, "properties can not be null");
    }

    public String getFunctionName() {
        return functionName;
    }

    public Properties getProperties() {
        return properties;
    }

    public Map<String, Placeholder> getPropertyParameters() {
        return propertyParameters;
    }

    @Override
    public List<Expression> getExpressions() {
        return ImmutableList.copyOf(propertyParameters.values());
    }

    @Override
    public TableValuedFunction getFunction() {
        throw new UnboundException("getFunction");
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
    public List<Slot> computeOutput() {
        throw new UnboundException("output");
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new UnboundTVFRelation(relationId, functionName, properties, propertyParameters, groupExpression,
                Optional.of(getLogicalProperties()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new UnboundTVFRelation(relationId, functionName, properties, propertyParameters,
                groupExpression, logicalProperties);
    }

    @Override
    public UnboundTVFRelation withRelationId(RelationId relationId) {
        throw new UnboundException("should not call UnboundTVFRelation's withRelationId method");
    }

    @Override
    public String toString() {
        return Utils.toSqlString("UnboundTVFRelation",
                "functionName", functionName,
                "arguments", properties
        );
    }

    @Override
    public String toDigest() {
        StringBuilder sb = new StringBuilder(functionName);
        sb.append("(?)");
        return sb.toString();
    }
}

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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Union;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical Union.
 */
public class PhysicalUnion extends PhysicalSetOperation implements Union {

    // in doris, we use union node to present one row relation
    private final List<List<NamedExpression>> constantExprsList;

    public PhysicalUnion(Qualifier qualifier,
            List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs,
            List<List<NamedExpression>> constantExprsList,
            LogicalProperties logicalProperties,
            List<Plan> children) {
        super(PlanType.PHYSICAL_UNION, qualifier, outputs, childrenOutputs, logicalProperties, children);
        this.constantExprsList = ImmutableList.copyOf(
                Objects.requireNonNull(constantExprsList, "constantExprsList should not be null"));
    }

    public PhysicalUnion(Qualifier qualifier,
            List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs,
            List<List<NamedExpression>> constantExprsList,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            List<Plan> children) {
        super(PlanType.PHYSICAL_UNION, qualifier, outputs, childrenOutputs,
                groupExpression, logicalProperties, children);
        this.constantExprsList = ImmutableList.copyOf(
                Objects.requireNonNull(constantExprsList, "constantExprsList should not be null"));
    }

    public PhysicalUnion(Qualifier qualifier, List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs, List<List<NamedExpression>> constantExprsList,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, List<Plan> inputs) {
        super(PlanType.PHYSICAL_UNION, qualifier, outputs, childrenOutputs,
                groupExpression, logicalProperties, physicalProperties, statistics, inputs);
        this.constantExprsList = ImmutableList.copyOf(
                Objects.requireNonNull(constantExprsList, "constantExprsList should not be null"));
    }

    public List<List<NamedExpression>> getConstantExprsList() {
        return constantExprsList;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalUnion(this, context);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalUnion" + getGroupIdWithPrefix(),
                "qualifier", qualifier,
                "outputs", outputs,
                "regularChildrenOutputs", regularChildrenOutputs,
                "constantExprsList", constantExprsList,
                "stats", statistics);
    }

    @Override
    public PhysicalUnion withChildren(List<Plan> children) {
        return new PhysicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList,
                getLogicalProperties(), children);
    }

    @Override
    public PhysicalUnion withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList,
                groupExpression, getLogicalProperties(), children);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList,
                groupExpression, logicalProperties.get(), children);
    }

    @Override
    public PhysicalUnion withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList,
                Optional.empty(), getLogicalProperties(), physicalProperties, statistics, children);
    }

    @Override
    public PhysicalUnion resetLogicalProperties() {
        return new PhysicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList,
                Optional.empty(), null, physicalProperties, statistics, children);
    }
}

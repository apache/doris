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
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import java.util.List;
import java.util.Optional;

/**
 * Physical Except.
 */
public class PhysicalExcept extends PhysicalSetOperation {

    public PhysicalExcept(Qualifier qualifier,
            List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs,
            LogicalProperties logicalProperties,
            List<Plan> children) {
        super(PlanType.PHYSICAL_EXCEPT, qualifier, outputs, childrenOutputs, logicalProperties, children);
    }

    public PhysicalExcept(Qualifier qualifier,
            List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            List<Plan> children) {
        super(PlanType.PHYSICAL_EXCEPT, qualifier, outputs, childrenOutputs,
                groupExpression, logicalProperties, children);
    }

    public PhysicalExcept(Qualifier qualifier, List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics,
            List<Plan> children) {
        super(PlanType.PHYSICAL_EXCEPT, qualifier, outputs, childrenOutputs,
                groupExpression, logicalProperties, physicalProperties, statistics, children);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalExcept(this, context);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalExcept",
                "qualifier", qualifier,
                "outputs", outputs,
                "regularChildrenOutputs", regularChildrenOutputs,
                "stats", statistics);
    }

    @Override
    public PhysicalExcept withChildren(List<Plan> children) {
        return new PhysicalExcept(qualifier, outputs, regularChildrenOutputs, getLogicalProperties(), children);
    }

    @Override
    public PhysicalExcept withGroupExpression(
            Optional<GroupExpression> groupExpression) {
        return new PhysicalExcept(qualifier, outputs, regularChildrenOutputs,
                groupExpression, getLogicalProperties(), children);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalExcept(qualifier, outputs, regularChildrenOutputs,
                groupExpression, logicalProperties.get(), children);
    }

    @Override
    public PhysicalExcept withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalExcept(qualifier, outputs, regularChildrenOutputs, Optional.empty(),
                getLogicalProperties(), physicalProperties, statistics, children);
    }

    @Override
    public PhysicalExcept resetLogicalProperties() {
        return new PhysicalExcept(qualifier, outputs, regularChildrenOutputs, Optional.empty(),
                getLogicalProperties(), physicalProperties, statistics, children);
    }
}

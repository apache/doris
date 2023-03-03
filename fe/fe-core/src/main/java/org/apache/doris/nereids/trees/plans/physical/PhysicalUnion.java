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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import java.util.List;
import java.util.Optional;

/**
 * Physical Union.
 */
public class PhysicalUnion extends PhysicalSetOperation {

    public PhysicalUnion(Qualifier qualifier,
                         LogicalProperties logicalProperties,
                         List<Plan> inputs) {
        super(PlanType.PHYSICAL_UNION, qualifier, logicalProperties, inputs);
    }

    public PhysicalUnion(Qualifier qualifier,
                         Optional<GroupExpression> groupExpression,
                         LogicalProperties logicalProperties,
                         List<Plan> inputs) {
        super(PlanType.PHYSICAL_UNION, qualifier, groupExpression, logicalProperties, inputs);
    }

    public PhysicalUnion(Qualifier qualifier,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult, List<Plan> inputs) {
        super(PlanType.PHYSICAL_UNION, qualifier,
                groupExpression, logicalProperties, physicalProperties, statsDeriveResult, inputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalUnion(this, context);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalUnion",
                "qualifier", qualifier,
                "stats", statsDeriveResult);
    }

    @Override
    public PhysicalUnion withChildren(List<Plan> children) {
        return new PhysicalUnion(qualifier, getLogicalProperties(), children);
    }

    @Override
    public PhysicalUnion withGroupExpression(
            Optional<GroupExpression> groupExpression) {
        return new PhysicalUnion(qualifier, groupExpression, getLogicalProperties(), children);
    }

    @Override
    public PhysicalUnion withLogicalProperties(
            Optional<LogicalProperties> logicalProperties) {
        return new PhysicalUnion(qualifier, Optional.empty(), logicalProperties.get(), children);
    }

    @Override
    public PhysicalUnion withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult) {
        return new PhysicalUnion(qualifier, Optional.empty(),
                getLogicalProperties(), physicalProperties, statsDeriveResult, children);
    }
}

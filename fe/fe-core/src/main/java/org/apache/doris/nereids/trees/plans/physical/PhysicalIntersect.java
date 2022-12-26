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
 * Physical Intersect.
 */
public class PhysicalIntersect extends PhysicalSetOperation {

    public PhysicalIntersect(Qualifier qualifier,
                          LogicalProperties logicalProperties,
                          List<Plan> inputs) {
        super(PlanType.PHYSICAL_INTERSECT, qualifier, logicalProperties, inputs);
    }

    public PhysicalIntersect(Qualifier qualifier,
                             Optional<GroupExpression> groupExpression,
                             LogicalProperties logicalProperties,
                             List<Plan> inputs) {
        super(PlanType.PHYSICAL_INTERSECT, qualifier, groupExpression, logicalProperties, inputs);
    }

    public PhysicalIntersect(Qualifier qualifier,
                             Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
                             PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult,
                             List<Plan> inputs) {
        super(PlanType.PHYSICAL_INTERSECT, qualifier,
                groupExpression, logicalProperties, physicalProperties, statsDeriveResult, inputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalIntersect(this, context);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalIntersect",
                "qualifier", qualifier,
                "stats", statsDeriveResult);
    }

    @Override
    public PhysicalIntersect withChildren(List<Plan> children) {
        return new PhysicalIntersect(qualifier, getLogicalProperties(), children);
    }

    @Override
    public PhysicalIntersect withGroupExpression(
            Optional<GroupExpression> groupExpression) {
        return new PhysicalIntersect(qualifier, groupExpression, getLogicalProperties(), children);
    }

    @Override
    public PhysicalIntersect withLogicalProperties(
            Optional<LogicalProperties> logicalProperties) {
        return new PhysicalIntersect(qualifier, Optional.empty(), logicalProperties.get(), children);
    }

    @Override
    public PhysicalIntersect withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult) {
        return new PhysicalIntersect(qualifier,
                Optional.empty(), getLogicalProperties(), physicalProperties, statsDeriveResult, children);
    }
}

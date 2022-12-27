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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Optional;

/**
 * Logical Intersect.
 */
public class LogicalIntersect extends LogicalSetOperation {

    public LogicalIntersect(Qualifier qualifier, List<Plan> inputs) {
        super(PlanType.LOGICAL_INTERSECT, qualifier, inputs);
    }

    public LogicalIntersect(Qualifier qualifier, List<NamedExpression> outputs,
                            List<Plan> inputs) {
        super(PlanType.LOGICAL_INTERSECT, qualifier, outputs, inputs);
    }

    public LogicalIntersect(Qualifier qualifier, List<NamedExpression> outputs,
                         Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
                         List<Plan> inputs) {
        super(PlanType.LOGICAL_INTERSECT, qualifier, outputs, groupExpression, logicalProperties, inputs);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalIntersect",
                "qualifier", qualifier,
                "outputs", outputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalIntersect(this, context);
    }

    @Override
    public LogicalIntersect withChildren(List<Plan> children) {
        return new LogicalIntersect(qualifier, outputs, children);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalIntersect(qualifier, outputs, groupExpression,
                Optional.of(getLogicalProperties()), children);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalIntersect(qualifier, outputs,
                Optional.empty(), logicalProperties, children);
    }

    @Override
    public Plan withNewOutputs(List<NamedExpression> newOutputs) {
        return new LogicalIntersect(qualifier, newOutputs,
                Optional.empty(), Optional.empty(), children);
    }

    @Override
    public Plan withNewChildren(List<Plan> children) {
        return withChildren(children);
    }
}

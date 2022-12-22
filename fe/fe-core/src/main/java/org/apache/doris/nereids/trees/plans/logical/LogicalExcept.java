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
 * Logical Except.
 */
public class LogicalExcept extends LogicalSetOperation {

    public LogicalExcept(Qualifier qualifier, List<Plan> inputs) {
        super(PlanType.LOGICAL_EXCEPT, qualifier, inputs);
    }

    public LogicalExcept(Qualifier qualifier, List<NamedExpression> outputs, List<Plan> inputs) {
        super(PlanType.LOGICAL_EXCEPT, qualifier, outputs, inputs);
    }

    public LogicalExcept(Qualifier qualifier, List<NamedExpression> outputs,
                        Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
                        List<Plan> inputs) {
        super(PlanType.LOGICAL_EXCEPT, qualifier, outputs, groupExpression, logicalProperties, inputs);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalExcept",
                "qualifier", qualifier,
                "outputs", outputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalExcept(this, context);
    }

    @Override
    public LogicalExcept withChildren(List<Plan> children) {
        return new LogicalExcept(qualifier, outputs, children);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalExcept(qualifier, outputs, groupExpression,
                Optional.of(getLogicalProperties()), children);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalExcept(qualifier, outputs,
                Optional.empty(), logicalProperties, children);
    }

    @Override
    public Plan withNewOutputs(List<NamedExpression> newOutputs) {
        return new LogicalExcept(qualifier, newOutputs, Optional.empty(), Optional.empty(), children);
    }

    @Override
    public Plan withNewChildren(List<Plan> children) {
        return withChildren(children);
    }
}

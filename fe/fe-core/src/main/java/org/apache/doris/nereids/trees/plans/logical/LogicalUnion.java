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
 * Logical Union.
 */
public class LogicalUnion extends LogicalSetOperation implements OutputPrunable {
    public LogicalUnion(Qualifier qualifier, List<Plan> inputs) {
        super(PlanType.LOGICAL_UNION, qualifier, inputs);
    }

    public LogicalUnion(Qualifier qualifier, List<NamedExpression> outputs, List<Plan> inputs) {
        super(PlanType.LOGICAL_UNION, qualifier, outputs, inputs);
    }

    public LogicalUnion(Qualifier qualifier, List<NamedExpression> outputs, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> inputs) {
        super(PlanType.LOGICAL_UNION, qualifier, outputs, groupExpression, logicalProperties, inputs);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalUnion", "qualifier", qualifier, "outputs", outputs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalUnion that = (LogicalUnion) o;
        return super.equals(that);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalUnion(this, context);
    }

    @Override
    public LogicalUnion withChildren(List<Plan> children) {
        return new LogicalUnion(qualifier, outputs, children);
    }

    @Override
    public LogicalUnion withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalUnion(qualifier, outputs, groupExpression,
                Optional.of(getLogicalProperties()), children);
    }

    @Override
    public LogicalUnion withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalUnion(qualifier, outputs, Optional.empty(), logicalProperties, children);
    }

    @Override
    public LogicalUnion withNewOutputs(List<NamedExpression> newOutputs) {
        return new LogicalUnion(qualifier, newOutputs, Optional.empty(), Optional.empty(), children);
    }

    public LogicalUnion withAllQualifier() {
        return new LogicalUnion(Qualifier.ALL, outputs, Optional.empty(), Optional.empty(), children);
    }

    @Override
    public LogicalUnion withNewChildren(List<Plan> children) {
        return withChildren(children);
    }

    @Override
    public LogicalUnion pruneOutputs(List<NamedExpression> prunedOutputs) {
        return withNewOutputs(prunedOutputs);
    }
}

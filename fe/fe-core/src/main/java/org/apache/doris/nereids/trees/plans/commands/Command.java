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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import java.util.List;
import java.util.Optional;

/**
 * command.
 */
public interface Command extends LogicalPlan {

    void run(ConnectContext connectContext);

    @Override
    default Optional<GroupExpression> getGroupExpression() {
        return Optional.empty();
    }

    @Override
    default List<Plan> children() {
        return null;
    }

    @Override
    default Plan child(int index) {
        return null;
    }

    @Override
    default int arity() {
        return 0;
    }

    @Override
    default Plan withChildren(List<Plan> children) {
        return null;
    }

    @Override
    default PlanType getType() {
        return null;
    }

    @Override
    default List<Expression> getExpressions() {
        return null;
    }

    @Override
    default LogicalProperties getLogicalProperties() {
        return null;
    }

    @Override
    default List<Slot> getOutput() {
        return null;
    }

    @Override
    default String treeString() {
        return null;
    }

    @Override
    default Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return null;
    }

    @Override
    default Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return null;
    }

    @Override
    default long getLimit() {
        return 0;
    }
}

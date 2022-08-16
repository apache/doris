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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class for all plan node.
 */
public interface Plan extends TreeNode<Plan> {

    PlanType getType();

    // cache GroupExpression for fast exit from Memo.copyIn.
    Optional<GroupExpression> getGroupExpression();

    <R, C> R accept(PlanVisitor<R, C> visitor, C context);

    List<Expression> getExpressions();

    LogicalProperties getLogicalProperties();

    boolean canResolve();

    default boolean resolved() {
        return !(getLogicalProperties() instanceof UnboundLogicalProperties);
    }

    default boolean childrenResolved() {
        return children()
                .stream()
                .map(Plan::getLogicalProperties)
                .allMatch(p -> !(p instanceof UnboundLogicalProperties));
    }

    default LogicalProperties computeLogicalProperties() {
        throw new IllegalStateException("Not support compute logical properties for " + getClass().getName());
    }

    List<Slot> getOutput();

    default List<Slot> computeOutput() {
        throw new IllegalStateException("Not support compute output for " + getClass().getName());
    }

    String treeString();

    default Plan withOutput(List<Slot> output) {
        return withLogicalProperties(Optional.of(getLogicalProperties().withOutput(output)));
    }

    Plan withGroupExpression(Optional<GroupExpression> groupExpression);

    Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties);
}

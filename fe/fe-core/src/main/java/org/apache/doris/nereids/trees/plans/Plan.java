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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract class for all plan node.
 */
public interface Plan extends TreeNode<Plan> {

    PlanType getType();

    // cache GroupExpression for fast exit from Memo.copyIn.
    Optional<GroupExpression> getGroupExpression();

    <R, C> R accept(PlanVisitor<R, C> visitor, C context);

    List<? extends Expression> getExpressions();

    LogicalProperties getLogicalProperties();

    boolean canBind();

    default boolean bound() {
        return !(getLogicalProperties() instanceof UnboundLogicalProperties);
    }

    default boolean hasUnboundExpression() {
        return getExpressions().stream().anyMatch(Expression::hasUnbound);
    }

    default boolean childrenBound() {
        return children()
                .stream()
                .allMatch(Plan::bound);
    }

    default LogicalProperties computeLogicalProperties() {
        throw new IllegalStateException("Not support compute logical properties for " + getClass().getName());
    }

    /**
     * Get extra plans.
     */
    default List<Plan> extraPlans() {
        return ImmutableList.of();
    }

    default boolean displayExtraPlanFirst() {
        return false;
    }

    /**
     * Get output slot list of the plan.
     */
    List<Slot> getOutput();

    List<Slot> getNonUserVisibleOutput();

    /**
     * Get output slot set of the plan.
     */
    default Set<Slot> getOutputSet() {
        return ImmutableSet.copyOf(getOutput());
    }

    default Set<ExprId> getOutputExprIdSet() {
        return getOutput().stream().map(NamedExpression::getExprId).collect(Collectors.toSet());
    }

    /**
     * Get the input slot set of the plan.
     * The result is collected from all the expressions' input slots appearing in the plan node.
     * <p>
     * Note that the input slots of subquery's inner plan are not included.
     */
    default Set<Slot> getInputSlots() {
        return getExpressions().stream()
                .flatMap(expr -> expr.getInputSlots().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    default List<Slot> computeOutput() {
        throw new IllegalStateException("Not support compute output for " + getClass().getName());
    }

    default List<Slot> computeNonUserVisibleOutput() {
        return ImmutableList.of();
    }

    String treeString();

    default Plan withOutput(List<Slot> output) {
        return withLogicalProperties(Optional.of(getLogicalProperties().withOutput(output)));
    }

    Plan withGroupExpression(Optional<GroupExpression> groupExpression);

    Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties);
}

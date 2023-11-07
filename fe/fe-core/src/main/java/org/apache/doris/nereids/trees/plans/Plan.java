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
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
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
        // TODO: avoid to use getLogicalProperties()
        return !(getLogicalProperties() instanceof UnboundLogicalProperties);
    }

    default boolean hasUnboundExpression() {
        return getExpressions().stream().anyMatch(Expression::hasUnbound);
    }

    default LogicalProperties computeLogicalProperties() {
        throw new IllegalStateException("Not support compute logical properties for " + getClass().getName());
    }

    /**
     * Get extra plans.
     */
    default List<? extends Plan> extraPlans() {
        return ImmutableList.of();
    }

    default boolean displayExtraPlanFirst() {
        return false;
    }

    /**
     * Get output slot list of the plan.
     */
    List<Slot> getOutput();

    /**
     * Get output slot set of the plan.
     */
    default Set<Slot> getOutputSet() {
        return ImmutableSet.copyOf(getOutput());
    }

    default List<ExprId> getOutputExprIds() {
        return getOutput().stream().map(NamedExpression::getExprId).collect(Collectors.toList());
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

    String treeString();

    Plan withGroupExpression(Optional<GroupExpression> groupExpression);

    Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children);

    <T> Optional<T> getMutableState(String key);

    /** getOrInitMutableState */
    default <T> T getOrInitMutableState(String key, Supplier<T> initState) {
        Optional<T> mutableState = getMutableState(key);
        if (!mutableState.isPresent()) {
            T state = initState.get();
            setMutableState(key, state);
            return state;
        }
        return mutableState.get();
    }

    void setMutableState(String key, Object value);

    /**
     * a simple version of explain, used to verify plan shape
     * @param prefix "  "
     * @return string format of plan shape
     */
    default String shape(String prefix) {
        StringBuilder builder = new StringBuilder();
        String me = shapeInfo();
        String prefixTail = "";
        if (! ConnectContext.get().getSessionVariable().getIgnoreShapePlanNodes().contains(me)) {
            builder.append(prefix).append(shapeInfo()).append("\n");
            prefixTail += "--";
        }
        String childPrefix = prefix + prefixTail;
        children().forEach(
                child -> {
                    builder.append(child.shape(childPrefix));
                }
        );
        return builder.toString();
    }

    /**
     * used in shape()
     * @return default value is its class name
     */
    default String shapeInfo() {
        return this.getClass().getSimpleName();
    }
}

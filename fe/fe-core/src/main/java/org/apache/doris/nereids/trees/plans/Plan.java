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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;

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

    /** hasUnboundExpression */
    default boolean hasUnboundExpression() {
        for (Expression expression : getExpressions()) {
            if (expression.hasUnbound()) {
                return true;
            }
        }
        return false;
    }

    default boolean containsSlots(ImmutableSet<Slot> slots) {
        return getExpressions().stream().anyMatch(
                expression -> !Sets.intersection(slots, expression.getInputSlots()).isEmpty()
                        || children().stream().anyMatch(plan -> plan.containsSlots(slots)));
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
    Set<Slot> getOutputSet();

    /** getOutputExprIds */
    default List<ExprId> getOutputExprIds() {
        List<Slot> output = getOutput();
        ImmutableList.Builder<ExprId> exprIds = ImmutableList.builderWithExpectedSize(output.size());
        for (Slot slot : output) {
            exprIds.add(slot.getExprId());
        }
        return exprIds.build();
    }

    /** getOutputExprIdSet */
    default Set<ExprId> getOutputExprIdSet() {
        List<Slot> output = getOutput();
        ImmutableSet.Builder<ExprId> exprIds = ImmutableSet.builderWithExpectedSize(output.size());
        for (Slot slot : output) {
            exprIds.add(slot.getExprId());
        }
        return exprIds.build();
    }

    /** getChildrenOutputExprIdSet */
    default Set<ExprId> getChildrenOutputExprIdSet() {
        switch (arity()) {
            case 0: return ImmutableSet.of();
            case 1: return child(0).getOutputExprIdSet();
            default: {
                int exprIdSize = 0;
                for (Plan child : children()) {
                    exprIdSize += child.getOutput().size();
                }

                ImmutableSet.Builder<ExprId> exprIds = ImmutableSet.builderWithExpectedSize(exprIdSize);
                for (Plan child : children()) {
                    for (Slot slot : child.getOutput()) {
                        exprIds.add(slot.getExprId());
                    }
                }
                return exprIds.build();
            }
        }
    }

    /**
     * Get the input slot set of the plan.
     * The result is collected from all the expressions' input slots appearing in the plan node.
     * <p>
     * Note that the input slots of subquery's inner plan are not included.
     */
    default Set<Slot> getInputSlots() {
        return PlanUtils.fastGetInputSlots(this.getExpressions());
    }

    default List<Slot> computeOutput() {
        throw new IllegalStateException("Not support compute output for " + getClass().getName());
    }

    /**
     * Get the input relation ids set of the plan.
     * @return The result is collected from all inputs relations
     */
    default Set<RelationId> getInputRelations() {
        Set<RelationId> relationIdSet = Sets.newHashSet();
        children().forEach(
                plan -> relationIdSet.addAll(plan.getInputRelations())
        );
        return relationIdSet;
    }

    String treeString();

    Plan withGroupExpression(Optional<GroupExpression> groupExpression);

    Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children);

    /**
     * a simple version of explain, used to verify plan shape
     * @param prefix "  "
     * @return string format of plan shape
     */
    default String shape(String prefix) {
        StringBuilder builder = new StringBuilder();
        String me = this.getClass().getSimpleName();
        String prefixTail = "";
        if (!ConnectContext.get().getSessionVariable().getIgnoreShapePlanNodes().contains(me)) {
            builder.append(prefix).append(shapeInfo()).append("\n");
            prefixTail += "--";
        }
        String childPrefix = prefix + prefixTail;
        children().forEach(
                child -> {
                    if (this instanceof Join) {
                        if (child instanceof PhysicalDistribute) {
                            child = child.child(0);
                        }
                    }
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

    /**
     * used in treeString()
     *
     * @return "" if groupExpression is empty, o.w. string format of group id
     */
    default String getGroupIdAsString() {
        String groupId;
        if (getGroupExpression().isPresent()) {
            groupId = getGroupExpression().get().getOwnerGroup().getGroupId().asInt() + "";
        } else if (getMutableState(MutableState.KEY_GROUP).isPresent()) {
            groupId = getMutableState(MutableState.KEY_GROUP).get().toString();
        } else {
            groupId = "";
        }
        return groupId;
    }

    default String getGroupIdWithPrefix() {
        return "@" + getGroupIdAsString();
    }
}

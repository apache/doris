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
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequireProperties;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Window;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * physical node for window function
 */
public class PhysicalWindow<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> implements Window {

    private final WindowFrameGroup windowFrameGroup;
    private final RequireProperties requireProperties;

    private final List<NamedExpression> windowExpressions;
    private final boolean isSkew;

    public PhysicalWindow(WindowFrameGroup windowFrameGroup, RequireProperties requireProperties,
                          List<NamedExpression> windowExpressions, boolean isSkew,
                          LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(windowFrameGroup, requireProperties, windowExpressions, isSkew,
                Optional.empty(), logicalProperties, child);
    }

    /** constructor for PhysicalWindow */
    public PhysicalWindow(WindowFrameGroup windowFrameGroup, RequireProperties requireProperties,
                          List<NamedExpression> windowExpressions, boolean isSkew,
                          Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
                          CHILD_TYPE child) {
        super(PlanType.PHYSICAL_WINDOW, groupExpression, logicalProperties, child);
        this.windowFrameGroup = Objects.requireNonNull(windowFrameGroup, "windowFrameGroup in PhysicalWindow"
                + "cannot be null");
        this.requireProperties = requireProperties;
        this.windowExpressions = ImmutableList.copyOf(windowExpressions);
        this.isSkew = isSkew;
    }

    /** constructor for PhysicalWindow */
    public PhysicalWindow(WindowFrameGroup windowFrameGroup, RequireProperties requireProperties,
                          List<NamedExpression> windowExpressions, boolean isSkew,
                          Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
                          PhysicalProperties physicalProperties, Statistics statistics,
                          CHILD_TYPE child) {
        super(PlanType.PHYSICAL_WINDOW, groupExpression, logicalProperties, physicalProperties,
                statistics, child);
        this.windowFrameGroup = Objects.requireNonNull(windowFrameGroup, "windowFrameGroup in PhysicalWindow"
            + "cannot be null");
        this.requireProperties = requireProperties;
        this.windowExpressions = ImmutableList.copyOf(windowExpressions);
        this.isSkew = isSkew;
    }

    @Override
    public List<NamedExpression> getWindowExpressions() {
        return windowFrameGroup.getGroups();
    }

    public WindowFrameGroup getWindowFrameGroup() {
        return windowFrameGroup;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalWindow(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return windowFrameGroup.getGroups();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalWindow[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "stats", statistics,
            "windowFrameGroup", windowFrameGroup,
            "requiredProperties", requireProperties
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalWindow<?> that = (PhysicalWindow<?>) o;
        return Objects.equals(windowFrameGroup, that.windowFrameGroup)
            && Objects.equals(requireProperties, that.requireProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowFrameGroup, requireProperties);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkState(children.size() == 1);
        return new PhysicalWindow<>(windowFrameGroup, requireProperties, windowExpressions, isSkew, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalWindow<>(windowFrameGroup, requireProperties, windowExpressions, isSkew, groupExpression,
                getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkState(children.size() == 1);
        return new PhysicalWindow<>(windowFrameGroup, requireProperties, windowExpressions, isSkew, groupExpression,
                logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                       Statistics statistics) {
        return new PhysicalWindow<>(windowFrameGroup, requireProperties, windowExpressions, isSkew, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, child());
    }

    public <C extends Plan> PhysicalWindow<C> withRequirePropertiesAndChild(RequireProperties requireProperties,
                                                                            C newChild) {
        return new PhysicalWindow<>(windowFrameGroup, requireProperties, windowExpressions, isSkew, Optional.empty(),
                getLogicalProperties(), physicalProperties, statistics, newChild);
    }

    @Override
    public List<Slot> computeOutput() {
        return new ImmutableList.Builder<Slot>()
                .addAll(child().getOutput())
                .addAll(windowExpressions.stream()
                        .map(NamedExpression::toSlot)
                        .collect(ImmutableList.toImmutableList()))
                .build();
    }

    @Override
    public PhysicalWindow<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalWindow<>(windowFrameGroup, requireProperties, windowExpressions, isSkew, groupExpression,
                null, physicalProperties, statistics, child());
    }

    private boolean isUnique(NamedExpression namedExpression) {
        if (namedExpression.children().size() != 1 || !(namedExpression.child(0) instanceof WindowExpression)) {
            return false;
        }
        WindowExpression windowExpr = (WindowExpression) namedExpression.child(0);
        List<Expression> partitionKeys = windowExpr.getPartitionKeys();
        // Now we only support slot type keys
        if (!partitionKeys.stream().allMatch(Slot.class::isInstance)) {
            return false;
        }
        ImmutableSet<Slot> slotSet = partitionKeys.stream()
                .map(s -> (Slot) s)
                .collect(ImmutableSet.toImmutableSet());
        // if partition by keys are uniform or empty, output is unique
        if (child(0).getLogicalProperties().getTrait().isUniformAndNotNull(slotSet)
                || slotSet.isEmpty()) {
            if (windowExpr.getFunction() instanceof RowNumber) {
                return true;
            }
        }
        return false;
    }

    private boolean isUniform(NamedExpression namedExpression) {
        if (namedExpression.children().size() != 1 || !(namedExpression.child(0) instanceof WindowExpression)) {
            return false;
        }
        WindowExpression windowExpr = (WindowExpression) namedExpression.child(0);
        List<Expression> partitionKeys = windowExpr.getPartitionKeys();
        // Now we only support slot type keys
        if (!partitionKeys.stream().allMatch(Slot.class::isInstance)) {
            return false;
        }
        ImmutableSet<Slot> slotSet = partitionKeys.stream()
                .map(s -> (Slot) s)
                .collect(ImmutableSet.toImmutableSet());
        // if partition by keys are unique, output is uniform
        if (child(0).getLogicalProperties().getTrait().isUniqueAndNotNull(slotSet)) {
            if (windowExpr.getFunction() instanceof RowNumber
                    || windowExpr.getFunction() instanceof Rank
                    || windowExpr.getFunction() instanceof DenseRank) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        builder.addUniqueSlot(child(0).getLogicalProperties().getTrait());
        for (NamedExpression namedExpression : windowExpressions) {
            if (isUnique(namedExpression)) {
                builder.addUniqueSlot(namedExpression.toSlot());
            }
        }
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        builder.addUniformSlot(child(0).getLogicalProperties().getTrait());
        for (NamedExpression namedExpression : windowExpressions) {
            if (isUniform(namedExpression)) {
                builder.addUniformSlot(namedExpression.toSlot());
            }
        }
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        builder.addEqualSet(child(0).getLogicalProperties().getTrait());
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        builder.addFuncDepsDG(child().getLogicalProperties().getTrait());
    }

    public boolean isSkew() {
        return isSkew;
    }
}

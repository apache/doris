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
import org.apache.doris.nereids.properties.RequireProperties;
import org.apache.doris.nereids.properties.RequirePropertiesSupplier;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Window;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * physical node for window function
 */
public class PhysicalWindow<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> implements Window,
        RequirePropertiesSupplier<PhysicalWindow<CHILD_TYPE>> {

    private final WindowFrameGroup windowFrameGroup;
    private final RequireProperties requireProperties;

    public PhysicalWindow(WindowFrameGroup windowFrameGroup, RequireProperties requireProperties,
                          LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(windowFrameGroup, requireProperties, Optional.empty(), logicalProperties, child);
    }

    /** constructor for PhysicalWindow */
    public PhysicalWindow(WindowFrameGroup windowFrameGroup, RequireProperties requireProperties,
                          Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
                          CHILD_TYPE child) {
        super(PlanType.PHYSICAL_WINDOW, groupExpression, logicalProperties, child);
        this.windowFrameGroup = Objects.requireNonNull(windowFrameGroup, "windowFrameGroup in PhysicalWindow"
                + "cannot be null");
        this.requireProperties = requireProperties;
    }

    /** constructor for PhysicalWindow */
    public PhysicalWindow(WindowFrameGroup windowFrameGroup, RequireProperties requireProperties,
                          Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
                          PhysicalProperties physicalProperties, Statistics statistics,
                          CHILD_TYPE child) {
        super(PlanType.PHYSICAL_WINDOW, groupExpression, logicalProperties, physicalProperties,
                statistics, child);
        this.windowFrameGroup = Objects.requireNonNull(windowFrameGroup, "windowFrameGroup in PhysicalWindow"
            + "cannot be null");
        this.requireProperties = requireProperties;
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
        return Utils.toSqlString("PhysicalWindow[" + id.asInt() + "]" + getGroupIdAsString(),
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
    public RequireProperties getRequireProperties() {
        return requireProperties;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkState(children.size() == 1);
        return new PhysicalWindow<>(windowFrameGroup, requireProperties, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalWindow<>(windowFrameGroup, requireProperties, groupExpression,
                getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkState(children.size() == 1);
        return new PhysicalWindow<>(windowFrameGroup, requireProperties, groupExpression,
                logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                       Statistics statistics) {
        return new PhysicalWindow<>(windowFrameGroup, requireProperties, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public PhysicalWindow<Plan> withRequireAndChildren(RequireProperties requireProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return withRequirePropertiesAndChild(requireProperties, children.get(0));
    }

    public <C extends Plan> PhysicalWindow<C> withRequirePropertiesAndChild(RequireProperties requireProperties,
                                                                            C newChild) {
        return new PhysicalWindow<>(windowFrameGroup, requireProperties, Optional.empty(),
                getLogicalProperties(), physicalProperties, statistics, newChild);
    }
}

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

package org.apache.doris.nereids.memo;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.operators.plans.logical.LogicalOperator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;

import com.clearspring.analytics.util.Lists;
import com.clearspring.analytics.util.Preconditions;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Representation for group in cascades optimizer.
 */
public class Group {
    private final GroupId groupId = GroupId.newPlanSetId();

    private final List<GroupExpression> logicalExpressions = Lists.newArrayList();
    private final List<GroupExpression> physicalExpressions = Lists.newArrayList();
    private LogicalProperties logicalProperties;

    private Map<PhysicalProperties, Pair<Double, GroupExpression>> lowestCostPlans;
    private double costLowerBound = -1;
    private boolean isExplored = false;

    /**
     * Constructor for Group.
     *
     * @param groupExpression first {@link GroupExpression} in this Group
     */
    public Group(GroupExpression groupExpression, LogicalProperties logicalProperties) {
        if (groupExpression.getOperator() instanceof LogicalOperator) {
            this.logicalExpressions.add(groupExpression);
        } else {
            this.physicalExpressions.add(groupExpression);
        }
        this.logicalProperties = logicalProperties;
        groupExpression.setParent(this);
    }

    public GroupId getGroupId() {
        return groupId;
    }

    /**
     * Add new {@link GroupExpression} into this group.
     *
     * @param groupExpression {@link GroupExpression} to be added
     * @return added {@link GroupExpression}
     */
    public GroupExpression addGroupExpression(GroupExpression groupExpression) {
        if (groupExpression.getOperator() instanceof LogicalOperator) {
            logicalExpressions.add(groupExpression);
        } else {
            physicalExpressions.add(groupExpression);
        }
        groupExpression.setParent(this);
        return groupExpression;
    }

    /**
     * Remove groupExpression from this group.
     *
     * @param groupExpression to be removed
     * @return removed {@link GroupExpression}
     */
    public GroupExpression removeGroupExpression(GroupExpression groupExpression) {
        if (groupExpression.getOperator() instanceof LogicalOperator) {
            logicalExpressions.remove(groupExpression);
        } else {
            physicalExpressions.remove(groupExpression);
        }
        groupExpression.setParent(null);
        return groupExpression;
    }

    /**
     * Rewrite the logical group expression to the new logical group expression.
     *
     * @param newExpression new logical group expression
     * @return old logical group expression
     */
    public GroupExpression rewriteLogicalExpression(GroupExpression newExpression,
                                                    LogicalProperties logicalProperties) {
        logicalExpressions.clear();
        logicalExpressions.add(newExpression);
        this.logicalProperties = logicalProperties;
        return getLogicalExpression();
    }

    public double getCostLowerBound() {
        return costLowerBound;
    }

    public void setCostLowerBound(double costLowerBound) {
        this.costLowerBound = costLowerBound;
    }

    public List<GroupExpression> getLogicalExpressions() {
        return logicalExpressions;
    }

    /**
     * Get the first logical group expression in this group.
     * If there is no logical group expression or more than one, throw an exception.
     *
     * @return the first logical group expression in this group
     */
    public GroupExpression getLogicalExpression() {
        Preconditions.checkArgument(logicalExpressions.size() == 1,
                "There should be only one Logical Expression in Group");
        Preconditions.checkArgument(physicalExpressions.isEmpty(),
                "The Physical Expression list in Group should be empty");
        return logicalExpressions.get(0);
    }

    public List<GroupExpression> getPhysicalExpressions() {
        return physicalExpressions;
    }

    public LogicalProperties getLogicalProperties() {
        return logicalProperties;
    }

    public void setLogicalProperties(LogicalProperties logicalProperties) {
        this.logicalProperties = logicalProperties;
    }

    public boolean isExplored() {
        return isExplored;
    }

    public void setExplored(boolean explored) {
        isExplored = explored;
    }

    /**
     * Get the lowest cost {@link org.apache.doris.nereids.trees.plans.physical.PhysicalPlan}
     * which meeting the physical property constraints in this Group.
     *
     * @param physicalProperties the physical property constraints
     * @return {@link Optional} of cost and {@link GroupExpression} of physical plan pair.
     */
    public Optional<Pair<Double, GroupExpression>> getLowestCostPlan(PhysicalProperties physicalProperties) {
        if (physicalProperties == null || CollectionUtils.isEmpty(lowestCostPlans)) {
            return Optional.empty();
        }
        return Optional.ofNullable(lowestCostPlans.get(physicalProperties));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Group group = (Group) o;
        return groupId.equals(group.groupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId);
    }
}

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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Representation for group in cascades optimizer.
 */
public class Group {
    private final GroupId groupId;

    private final List<GroupExpression> logicalExpressions = Lists.newArrayList();
    private final List<GroupExpression> physicalExpressions = Lists.newArrayList();
    private LogicalProperties logicalProperties;

    // Map of cost lower bounds
    // Map required plan props to cost lower bound of corresponding plan
    private final Map<PhysicalProperties, Pair<Double, GroupExpression>> lowestCostPlans = Maps.newHashMap();
    private double costLowerBound = -1;
    private boolean isExplored = false;
    private boolean hasCost = false;
    private StatsDeriveResult statistics;

    /**
     * Constructor for Group.
     *
     * @param groupExpression first {@link GroupExpression} in this Group
     */
    public Group(GroupId groupId, GroupExpression groupExpression, LogicalProperties logicalProperties) {
        this.groupId = groupId;
        if (groupExpression.getPlan() instanceof LogicalPlan) {
            this.logicalExpressions.add(groupExpression);
        } else {
            this.physicalExpressions.add(groupExpression);
        }
        this.logicalProperties = logicalProperties;
        groupExpression.setOwnerGroup(this);
    }

    /**
     * For unit test only.
     */
    public Group() {
        groupId = null;
    }

    public GroupId getGroupId() {
        return groupId;
    }

    public boolean isHasCost() {
        return hasCost;
    }

    public void setHasCost(boolean hasCost) {
        this.hasCost = hasCost;
    }

    /**
     * Add new {@link GroupExpression} into this group.
     *
     * @param groupExpression {@link GroupExpression} to be added
     * @return added {@link GroupExpression}
     */
    public GroupExpression addGroupExpression(GroupExpression groupExpression) {
        if (groupExpression.getPlan() instanceof LogicalPlan) {
            logicalExpressions.add(groupExpression);
        } else {
            physicalExpressions.add(groupExpression);
        }
        groupExpression.setOwnerGroup(this);
        return groupExpression;
    }

    /**
     * Remove groupExpression from this group.
     *
     * @param groupExpression to be removed
     * @return removed {@link GroupExpression}
     */
    public GroupExpression removeGroupExpression(GroupExpression groupExpression) {
        if (groupExpression.getPlan() instanceof LogicalPlan) {
            logicalExpressions.remove(groupExpression);
        } else {
            physicalExpressions.remove(groupExpression);
        }
        groupExpression.setOwnerGroup(null);
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
        newExpression.setOwnerGroup(this);
        this.logicalProperties = logicalProperties;
        GroupExpression oldExpression = getLogicalExpression();
        logicalExpressions.clear();
        logicalExpressions.add(newExpression);
        return oldExpression;
    }

    public double getCostLowerBound() {
        return costLowerBound;
    }

    public void setCostLowerBound(double costLowerBound) {
        this.costLowerBound = costLowerBound;
    }

    /**
     * Set or update lowestCostPlans: properties --> Pair.of(cost, expression)
     */
    public void setBestPlan(GroupExpression expression, double cost, PhysicalProperties properties) {
        if (lowestCostPlans.containsKey(properties)) {
            if (lowestCostPlans.get(properties).first > cost) {
                lowestCostPlans.put(properties, Pair.of(cost, expression));
            }
        } else {
            lowestCostPlans.put(properties, Pair.of(cost, expression));
        }
    }

    public GroupExpression getBestPlan(PhysicalProperties properties) {
        if (lowestCostPlans.containsKey(properties)) {
            return lowestCostPlans.get(properties).second;
        }
        return null;
    }

    public void replaceBestPlan(PhysicalProperties oldProperty, PhysicalProperties newProperty, double cost) {
        Pair<Double, GroupExpression> pair = lowestCostPlans.get(oldProperty);
        GroupExpression lowestGroupExpr = pair.second;
        lowestGroupExpr.updateLowestCostTable(newProperty, lowestGroupExpr.getInputPropertiesList(oldProperty), cost);
        lowestCostPlans.remove(oldProperty);
        lowestCostPlans.put(newProperty, pair);
    }

    public StatsDeriveResult getStatistics() {
        return statistics;
    }

    public void setStatistics(StatsDeriveResult statistics) {
        this.statistics = statistics;
    }


    public List<GroupExpression> getLogicalExpressions() {
        return logicalExpressions;
    }

    public GroupExpression logicalExpressionsAt(int index) {
        return logicalExpressions.get(index);
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
        if (physicalProperties == null || lowestCostPlans.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(lowestCostPlans.get(physicalProperties));
    }

    /**
     * Get the first Plan from Memo.
     */
    public PhysicalPlan extractPlan() throws AnalysisException {
        GroupExpression groupExpression = this.physicalExpressions.get(0);

        List<Plan> planChildren = com.google.common.collect.Lists.newArrayList();
        for (int i = 0; i < groupExpression.arity(); i++) {
            planChildren.add(groupExpression.child(i).extractPlan());
        }

        Plan plan = groupExpression.getPlan()
                .withChildren(planChildren)
                .withGroupExpression(Optional.of(groupExpression));
        if (!(plan instanceof PhysicalPlan)) {
            throw new AnalysisException("generate logical plan");
        }
        PhysicalPlan physicalPlan = (PhysicalPlan) plan;

        return physicalPlan;
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

    @Override
    public String toString() {
        return "Group[" + groupId + "]";
    }

    /**
     * move the ownerGroup of all logical expressions to target group
     * if this.equals(target), do nothing.
     * @param target the new owner group of expressions
     */
    public void moveLogicalExpressionOwnership(Group target) {
        if (equals(target)) {
            return;
        }
        for (GroupExpression expression : logicalExpressions) {
            target.addGroupExpression(expression);
        }
        logicalExpressions.clear();
    }

    /**
     * move the ownerGroup of all physical expressions to target group
     * if this.equals(target), do nothing.
     * @param target the new owner group of expressions
     */
    public void movePhysicalExpressionOwnership(Group target) {
        if (equals(target)) {
            return;
        }
        for (GroupExpression expression : physicalExpressions) {
            target.addGroupExpression(expression);
        }
        physicalExpressions.clear();
    }

}

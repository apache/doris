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
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.util.TreeStringUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Representation for group in cascades optimizer.
 */
public class Group {
    private final GroupId groupId;
    // Save all parent GroupExpression to avoid traversing whole Memo.
    private final IdentityHashMap<GroupExpression, Void> parentExpressions = new IdentityHashMap<>();

    private final List<GroupExpression> logicalExpressions = Lists.newArrayList();
    private final List<GroupExpression> physicalExpressions = Lists.newArrayList();
    private LogicalProperties logicalProperties;

    // Map of cost lower bounds
    // Map required plan props to cost lower bound of corresponding plan
    private final Map<PhysicalProperties, Pair<Double, GroupExpression>> lowestCostPlans = Maps.newHashMap();

    private boolean isExplored = false;

    private StatsDeriveResult statistics;

    /**
     * Constructor for Group.
     *
     * @param groupExpression first {@link GroupExpression} in this Group
     */
    public Group(GroupId groupId, GroupExpression groupExpression, LogicalProperties logicalProperties) {
        this.groupId = groupId;
        addGroupExpression(groupExpression);
        this.logicalProperties = logicalProperties;
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
        if (groupExpression.getPlan() instanceof LogicalPlan) {
            logicalExpressions.add(groupExpression);
        } else {
            physicalExpressions.add(groupExpression);
        }
        groupExpression.setOwnerGroup(this);
        return groupExpression;
    }

    public void addLogicalExpression(GroupExpression groupExpression) {
        groupExpression.setOwnerGroup(this);
        logicalExpressions.add(groupExpression);
    }

    public void addPhysicalExpression(GroupExpression groupExpression) {
        groupExpression.setOwnerGroup(this);
        physicalExpressions.add(groupExpression);
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

    /**
     * Remove groupExpression from this group.
     *
     * @param groupExpression to be removed
     * @return removed {@link GroupExpression}
     */
    public GroupExpression removeGroupExpression(GroupExpression groupExpression) {
        // use identityRemove to avoid equals() method
        if (groupExpression.getPlan() instanceof LogicalPlan) {
            Utils.identityRemove(logicalExpressions, groupExpression);
        } else {
            Utils.identityRemove(physicalExpressions, groupExpression);
        }
        groupExpression.setOwnerGroup(null);
        return groupExpression;
    }

    public List<GroupExpression> clearLogicalExpressions() {
        List<GroupExpression> move = logicalExpressions.stream()
                .peek(groupExpr -> groupExpr.setOwnerGroup(null))
                .collect(Collectors.toList());
        logicalExpressions.clear();
        return move;
    }

    public List<GroupExpression> clearPhysicalExpressions() {
        List<GroupExpression> move = physicalExpressions.stream()
                .peek(groupExpr -> groupExpr.setOwnerGroup(null))
                .collect(Collectors.toList());
        physicalExpressions.clear();
        return move;
    }

    public double getCostLowerBound() {
        return -1D;
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

    public GroupExpression getBestPlan(PhysicalProperties properties) {
        if (lowestCostPlans.containsKey(properties)) {
            return lowestCostPlans.get(properties).second;
        }
        return null;
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

    /**
     * replace best plan with new properties
     */
    public void replaceBestPlanProperty(PhysicalProperties oldProperty, PhysicalProperties newProperty, double cost) {
        Pair<Double, GroupExpression> pair = lowestCostPlans.get(oldProperty);
        GroupExpression lowestGroupExpr = pair.second;
        lowestGroupExpr.updateLowestCostTable(newProperty,
                lowestGroupExpr.getInputPropertiesList(oldProperty), cost);
        lowestCostPlans.remove(oldProperty);
        lowestCostPlans.put(newProperty, pair);
    }

    /**
     * replace oldGroupExpression with newGroupExpression in lowestCostPlans.
     */
    public void replaceBestPlanGroupExpr(GroupExpression oldGroupExpression, GroupExpression newGroupExpression) {
        Map<PhysicalProperties, Pair<Double, GroupExpression>> needReplaceBestExpressions = Maps.newHashMap();
        for (Iterator<Entry<PhysicalProperties, Pair<Double, GroupExpression>>> iterator =
                lowestCostPlans.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<PhysicalProperties, Pair<Double, GroupExpression>> entry = iterator.next();
            Pair<Double, GroupExpression> pair = entry.getValue();
            if (pair.second.equals(oldGroupExpression)) {
                needReplaceBestExpressions.put(entry.getKey(), Pair.of(pair.first, newGroupExpression));
                iterator.remove();
            }
        }
        lowestCostPlans.putAll(needReplaceBestExpressions);
    }

    public StatsDeriveResult getStatistics() {
        return statistics;
    }

    public void setStatistics(StatsDeriveResult statistics) {
        this.statistics = statistics;
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

    public List<GroupExpression> getParentGroupExpressions() {
        return ImmutableList.copyOf(parentExpressions.keySet());
    }

    public void addParentExpression(GroupExpression parent) {
        parentExpressions.put(parent, null);
    }

    /**
     * remove the reference to parent groupExpression
     *
     * @param parent group expression
     * @return parentExpressions's num
     */
    public int removeParentExpression(GroupExpression parent) {
        parentExpressions.remove(parent);
        return parentExpressions.size();
    }

    /**
     * move the ownerGroup to target group.
     *
     * @param target the new owner group of expressions
     */
    public void mergeTo(Group target) {
        // move parentExpressions Ownership
        parentExpressions.keySet().forEach(target::addParentExpression);
        // PhysicalEnforcer isn't in groupExpressions, so mergeGroup() can't replace its children.
        // So we need to manually replace the children of PhysicalEnforcer in here.
        parentExpressions.keySet().stream().filter(ge -> ge.getPlan() instanceof PhysicalDistribute)
                .forEach(ge -> ge.children().set(0, target));
        parentExpressions.clear();

        // move LogicalExpression PhysicalExpression Ownership
        Map<GroupExpression, GroupExpression> logicalSet = target.getLogicalExpressions().stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
        for (GroupExpression logicalExpression : logicalExpressions) {
            GroupExpression existGroupExpr = logicalSet.get(logicalExpression);
            if (existGroupExpr != null) {
                Preconditions.checkState(logicalExpression != existGroupExpr, "must not equals");
                // lowCostPlans must be physical GroupExpression, don't need to replaceBestPlanGroupExpr
                logicalExpression.mergeToNotOwnerRemove(existGroupExpr);
            } else {
                target.addLogicalExpression(logicalExpression);
            }
        }
        logicalExpressions.clear();
        // movePhysicalExpressionOwnership
        Map<GroupExpression, GroupExpression> physicalSet = target.getPhysicalExpressions().stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
        for (GroupExpression physicalExpression : physicalExpressions) {
            GroupExpression existGroupExpr = physicalSet.get(physicalExpression);
            if (existGroupExpr != null) {
                Preconditions.checkState(physicalExpression != existGroupExpr, "must not equals");
                physicalExpression.getOwnerGroup().replaceBestPlanGroupExpr(physicalExpression, existGroupExpr);
                physicalExpression.mergeToNotOwnerRemove(existGroupExpr);
            } else {
                target.addPhysicalExpression(physicalExpression);
            }
        }
        physicalExpressions.clear();

        // Above we already replaceBestPlanGroupExpr, but we still need to moveLowestCostPlansOwnership.
        // Because PhysicalEnforcer don't exist in physicalExpressions, so above `replaceBestPlanGroupExpr` can't
        // move PhysicalEnforcer in lowestCostPlans. Following code can move PhysicalEnforcer in lowestCostPlans.
        lowestCostPlans.forEach((physicalProperties, costAndGroupExpr) -> {
            GroupExpression bestGroupExpression = costAndGroupExpr.second;
            if (bestGroupExpression.getOwnerGroup() == this || bestGroupExpression.getOwnerGroup() == null) {
                // move PhysicalEnforcer into target
                Preconditions.checkState(bestGroupExpression.getPlan() instanceof PhysicalDistribute);
                bestGroupExpression.setOwnerGroup(target);
            }
            // move lowestCostPlans Ownership
            if (!target.lowestCostPlans.containsKey(physicalProperties)) {
                target.lowestCostPlans.put(physicalProperties, costAndGroupExpr);
            } else {
                if (costAndGroupExpr.first < target.lowestCostPlans.get(physicalProperties).first) {
                    target.lowestCostPlans.put(physicalProperties, costAndGroupExpr);
                }
            }
        });
        lowestCostPlans.clear();

        // If statistics is null, use other statistics
        if (target.statistics == null) {
            target.statistics = this.statistics;
        }
    }

    public boolean isJoinGroup() {
        return getLogicalExpression().getPlan() instanceof LogicalJoin;
    }

    public boolean isProjectGroup() {
        return getLogicalExpression().getPlan() instanceof LogicalProject;
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
     * Get tree like string describing group.
     *
     * @return tree like string describing group
     */
    public String treeString() {
        Function<Object, String> toString = obj -> {
            if (obj instanceof Group) {
                return obj.toString();
            } else if (obj instanceof GroupExpression) {
                return ((GroupExpression) obj).getPlan().toString();
            } else if (obj instanceof Pair) {
                // print logicalExpressions or physicalExpressions
                // first is name, second is group expressions
                return ((Pair<?, ?>) obj).first.toString();
            } else {
                return obj.toString();
            }
        };

        Function<Object, List<Object>> getChildren = obj -> {
            if (obj instanceof Group) {
                Group group = (Group) obj;
                List children = new ArrayList<>();

                // to <name, children> pair
                if (!group.getLogicalExpressions().isEmpty()) {
                    children.add(Pair.of("logicalExpressions", group.getLogicalExpressions()));
                }
                if (!group.getPhysicalExpressions().isEmpty()) {
                    children.add(Pair.of("physicalExpressions", group.getPhysicalExpressions()));
                }
                return children;
            } else if (obj instanceof GroupExpression) {
                return (List) ((GroupExpression) obj).children();
            } else if (obj instanceof Pair) {
                return (List) ((Pair<String, List<GroupExpression>>) obj).second;
            } else {
                return ImmutableList.of();
            }
        };

        Function<Object, List<Object>> getExtraPlans = obj -> {
            if (obj instanceof Plan) {
                return (List) ((Plan) obj).extraPlans();
            } else {
                return ImmutableList.of();
            }
        };

        Function<Object, Boolean> displayExtraPlan = obj -> {
            if (obj instanceof Plan) {
                return ((Plan) obj).displayExtraPlanFirst();
            } else {
                return false;
            }
        };

        return TreeStringUtils.treeString(this, toString, getChildren, getExtraPlans, displayExtraPlan);
    }
}

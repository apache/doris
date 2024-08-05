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
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.TreeStringUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
    private final Map<GroupExpression, GroupExpression> enforcers = Maps.newHashMap();
    private boolean isStatsReliable = true;
    private LogicalProperties logicalProperties;

    // Map of cost lower bounds
    // Map required plan props to cost lower bound of corresponding plan
    private final Map<PhysicalProperties, Pair<Cost, GroupExpression>> lowestCostPlans = Maps.newLinkedHashMap();

    private boolean isExplored = false;

    private Statistics statistics;

    private PhysicalProperties chosenProperties;

    private int chosenGroupExpressionId = -1;

    private List<PhysicalProperties> chosenEnforcerPropertiesList = new ArrayList<>();
    private List<Integer> chosenEnforcerIdList = new ArrayList<>();

    private StructInfoMap structInfoMap = new StructInfoMap();

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

    /**
     * Construct a Group without any group expression
     *
     * @param groupId the groupId in memo
     */
    public Group(GroupId groupId, LogicalProperties logicalProperties) {
        this.groupId = groupId;
        this.logicalProperties = logicalProperties;
    }

    public GroupId getGroupId() {
        return groupId;
    }

    public List<PhysicalProperties> getAllProperties() {
        return new ArrayList<>(lowestCostPlans.keySet());
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

    public void setStatsReliable(boolean statsReliable) {
        this.isStatsReliable = statsReliable;
    }

    public boolean isStatsReliable() {
        return isStatsReliable;
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

    public void clearLowestCostPlans() {
        lowestCostPlans.clear();
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
    public Optional<Pair<Cost, GroupExpression>> getLowestCostPlan(PhysicalProperties physicalProperties) {
        if (physicalProperties == null || lowestCostPlans.isEmpty()) {
            return Optional.empty();
        }
        Optional<Pair<Cost, GroupExpression>> costAndGroupExpression =
                Optional.ofNullable(lowestCostPlans.get(physicalProperties));
        return costAndGroupExpression;
    }

    public Map<PhysicalProperties, Cost> getLowestCosts() {
        return lowestCostPlans.entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(Entry::getKey, kv -> kv.getValue().first));
    }

    public GroupExpression getBestPlan(PhysicalProperties properties) {
        if (lowestCostPlans.containsKey(properties)) {
            return lowestCostPlans.get(properties).second;
        }
        return null;
    }

    public void addEnforcer(GroupExpression enforcer) {
        enforcer.setOwnerGroup(this);
        enforcers.put(enforcer, enforcer);
    }

    public Map<GroupExpression, GroupExpression> getEnforcers() {
        return enforcers;
    }

    /**
     * Set or update lowestCostPlans: properties --> Pair.of(cost, expression)
     */
    public void setBestPlan(GroupExpression expression, Cost cost, PhysicalProperties properties) {
        if (lowestCostPlans.containsKey(properties)) {
            if (lowestCostPlans.get(properties).first.getValue() > cost.getValue()) {
                lowestCostPlans.put(properties, Pair.of(cost, expression));
            }
        } else {
            lowestCostPlans.put(properties, Pair.of(cost, expression));
        }
    }

    /**
     * replace best plan with new properties
     */
    public void replaceBestPlanProperty(PhysicalProperties oldProperty,
            PhysicalProperties newProperty, Cost cost) {
        Pair<Cost, GroupExpression> pair = lowestCostPlans.get(oldProperty);
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
        Map<PhysicalProperties, Pair<Cost, GroupExpression>> needReplaceBestExpressions = Maps.newHashMap();
        for (Iterator<Entry<PhysicalProperties, Pair<Cost, GroupExpression>>> iterator =
                lowestCostPlans.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<PhysicalProperties, Pair<Cost, GroupExpression>> entry = iterator.next();
            Pair<Cost, GroupExpression> pair = entry.getValue();
            if (pair.second.equals(oldGroupExpression)) {
                needReplaceBestExpressions.put(entry.getKey(), Pair.of(pair.first, newGroupExpression));
                iterator.remove();
            }
        }
        lowestCostPlans.putAll(needReplaceBestExpressions);
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public void setStatistics(Statistics statistics) {
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

    public void removeParentPhysicalExpressions() {
        parentExpressions.entrySet().removeIf(entry -> entry.getKey().getPlan() instanceof PhysicalPlan);
    }

    /**
     * move the ownerGroup to target group.
     *
     * @param target the new owner group of expressions
     */
    public void mergeTo(Group target) {
        // move parentExpressions Ownership
        parentExpressions.keySet().forEach(parent -> target.addParentExpression(parent));

        // move enforcers Ownership
        enforcers.forEach((k, v) -> k.children().set(0, target));
        // TODO: dedup?
        enforcers.forEach((k, v) -> target.addEnforcer(k));
        enforcers.clear();

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
        lowestCostPlans.forEach((physicalProperties, costAndGroupExpr) -> {
            // move lowestCostPlans Ownership
            if (!target.lowestCostPlans.containsKey(physicalProperties)) {
                target.lowestCostPlans.put(physicalProperties, costAndGroupExpr);
            } else {
                if (costAndGroupExpr.first.getValue()
                        < target.lowestCostPlans.get(physicalProperties).first.getValue()) {
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

    /**
     * This function used to check whether the group is an end node in DPHyp
     */
    public boolean isValidJoinGroup() {
        Plan plan = getLogicalExpression().getPlan();
        if (plan instanceof LogicalJoin
                && ((LogicalJoin) plan).getJoinType() == JoinType.INNER_JOIN
                && !((LogicalJoin) plan).isMarkJoin()) {
            Preconditions.checkArgument(!((LogicalJoin) plan).getExpressions().isEmpty(),
                    "inner join must have join conjuncts");
            if (((LogicalJoin) plan).getHashJoinConjuncts().isEmpty()
                    && ((LogicalJoin) plan).getOtherJoinConjuncts().get(0) instanceof Literal) {
                return false;
            } else {
                // Right now, we only support inner join with some conjuncts referencing any side of the child's output
                return true;
            }
        }
        return false;
    }

    public StructInfoMap getstructInfoMap() {
        return structInfoMap;
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
        StringBuilder str = new StringBuilder("Group[" + groupId + "]\n");
        str.append("  logical expressions:\n");
        for (GroupExpression logicalExpression : logicalExpressions) {
            str.append("    ").append(logicalExpression).append("\n");
        }
        str.append("  physical expressions:\n");
        for (GroupExpression physicalExpression : physicalExpressions) {
            str.append("    ").append(physicalExpression).append("\n");
        }
        str.append("  enforcers:\n");
        for (GroupExpression enforcer : enforcers.keySet()) {
            str.append("    ").append(enforcer).append("\n");
        }
        if (!chosenEnforcerIdList.isEmpty()) {
            str.append("  chosen enforcer(id, requiredProperties):\n");
            for (int i = 0; i < chosenEnforcerIdList.size(); i++) {
                str.append("      (").append(i).append(")").append(chosenEnforcerIdList.get(i)).append(",  ")
                        .append(chosenEnforcerPropertiesList.get(i)).append("\n");
            }
        }
        if (chosenGroupExpressionId != -1) {
            str.append("  chosen expression id: ").append(chosenGroupExpressionId).append("\n");
            str.append("  chosen properties: ").append(chosenProperties).append("\n");
        }
        str.append("  stats").append("\n");
        str.append(getStatistics() == null ? "" : getStatistics().detail("    "));

        str.append("  lowest Plan(cost, properties, plan, childrenRequires)");
        for (Map.Entry<PhysicalProperties, Pair<Cost, GroupExpression>> entry : lowestCostPlans.entrySet()) {
            PhysicalProperties prop = entry.getKey();
            Pair<Cost, GroupExpression> costGroupExpressionPair = entry.getValue();
            Cost cost = costGroupExpressionPair.first;
            GroupExpression child = costGroupExpressionPair.second;
            str.append("\n\n    ").append(cost.getValue()).append(" ").append(prop)
                .append("\n     ").append(child).append("\n     ")
                .append(child.getInputPropertiesListOrEmpty(prop));
        }
        str.append("\n").append("  struct info map").append("\n");
        str.append(structInfoMap);

        return str.toString();
    }

    /**
     * Get tree like string describing group.
     *
     * @return tree like string describing group
     */
    public String treeString() {
        Function<Object, String> toString = obj -> {
            if (obj instanceof Group) {
                Group group = (Group) obj;
                Map<PhysicalProperties, Cost> lowestCosts = group.getLowestCosts();
                return "Group[" + group.groupId + ", lowestCosts: " + lowestCosts + "]";
            } else if (obj instanceof GroupExpression) {
                GroupExpression groupExpression = (GroupExpression) obj;
                Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> lowestCostTable
                        = groupExpression.getLowestCostTable();
                Map<PhysicalProperties, PhysicalProperties> requestPropertiesMap
                        = groupExpression.getRequestPropertiesMap();
                Cost cost = groupExpression.getCost();
                return groupExpression.getPlan().toString() + " [cost: " + cost + ", lowestCostTable: "
                        + lowestCostTable + ", requestPropertiesMap: " + requestPropertiesMap + "]";
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

    public PhysicalProperties getChosenProperties() {
        return chosenProperties;
    }

    public void setChosenProperties(PhysicalProperties chosenProperties) {
        this.chosenProperties = chosenProperties;
    }

    public void setChosenGroupExpressionId(int chosenGroupExpressionId) {
        Preconditions.checkArgument(this.chosenGroupExpressionId == -1,
                "chosenGroupExpressionId is already set");
        this.chosenGroupExpressionId = chosenGroupExpressionId;
    }

    public void addChosenEnforcerProperties(PhysicalProperties chosenEnforcerProperties) {
        this.chosenEnforcerPropertiesList.add(chosenEnforcerProperties);
    }

    public void addChosenEnforcerId(int chosenEnforcerId) {
        this.chosenEnforcerIdList.add(chosenEnforcerId);
    }
}

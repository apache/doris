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
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.cost.CostEstimate;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Representation for group expression in cascades optimizer.
 */
public class GroupExpression {
    private double cost = 0.0;
    private CostEstimate costEstimate = null;
    private Group ownerGroup;
    private ImmutableList<Group> children;
    private final Plan plan;
    private final BitSet ruleMasks;
    private boolean statDerived;

    // Mapping from output properties to the corresponding best cost, statistics, and child properties.
    private final Map<PhysicalProperties, Pair<Double, List<PhysicalProperties>>> lowestCostTable;
    // Each physical group expression maintains mapping incoming requests to the corresponding child requests.
    private final Map<PhysicalProperties, PhysicalProperties> requestPropertiesMap;

    public GroupExpression(Plan plan) {
        this(plan, Lists.newArrayList());
    }

    /**
     * Constructor for GroupExpression.
     *
     * @param plan {@link Plan} to reference
     * @param children children groups in memo
     */
    public GroupExpression(Plan plan, List<Group> children) {
        this.plan = Objects.requireNonNull(plan, "plan can not be null")
                .withGroupExpression(Optional.of(this));
        this.children = ImmutableList.copyOf(Objects.requireNonNull(children, "children can not be null"));
        this.ruleMasks = new BitSet(RuleType.SENTINEL.ordinal());
        this.statDerived = false;
        this.lowestCostTable = Maps.newHashMap();
        this.requestPropertiesMap = Maps.newHashMap();
        this.children.forEach(childGroup -> childGroup.addParentExpression(this));
    }

    public PhysicalProperties getOutputProperties(PhysicalProperties requestProperties) {
        PhysicalProperties outputProperties = requestPropertiesMap.get(requestProperties);
        Preconditions.checkNotNull(outputProperties);
        return outputProperties;
    }

    public int arity() {
        return children.size();
    }

    public Group getOwnerGroup() {
        return ownerGroup;
    }

    public void setOwnerGroup(Group ownerGroup) {
        this.ownerGroup = ownerGroup;
    }

    public Plan getPlan() {
        return plan;
    }

    public Group child(int i) {
        return children.get(i);
    }

    public List<Group> children() {
        return children;
    }

    public void setChildren(ImmutableList<Group> children) {
        this.children.forEach(g -> g.removeParentExpression(this));
        this.children = children;
        this.children.forEach(g -> g.addParentExpression(this));
    }

    /**
     * replaceChild.
     *
     * @param originChild origin child group
     * @param newChild new child group
     */
    public void replaceChild(Group originChild, Group newChild) {
        originChild.removeParentExpression(this);
        ImmutableList.Builder<Group> groupBuilder = ImmutableList.builderWithExpectedSize(arity());
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i) == originChild) {
                groupBuilder.add(newChild);
                newChild.addParentExpression(this);
            } else {
                groupBuilder.add(child(i));
            }
        }
        this.children = groupBuilder.build();
    }

    public void setChild(int index, Group group) {
        this.children.get(index).removeParentExpression(this);
        setChildByIndex(index, group);
    }

    public boolean hasApplied(Rule rule) {
        return ruleMasks.get(rule.getRuleType().ordinal());
    }

    private void setChildByIndex(int index, Group group) {
        ImmutableList.Builder<Group> builder = new Builder<>();
        builder.addAll(children.subList(0, index));
        builder.add(group);
        builder.addAll(children.subList(index + 1, children.size()));
        children = builder.build();
        group.addParentExpression(this);
    }

    public boolean notApplied(Rule rule) {
        return !hasApplied(rule);
    }

    public void setApplied(Rule rule) {
        ruleMasks.set(rule.getRuleType().ordinal());
    }

    public void setApplied(RuleType ruleType) {
        ruleMasks.set(ruleType.ordinal());
    }

    public void propagateApplied(GroupExpression toGroupExpression) {
        toGroupExpression.ruleMasks.or(ruleMasks);
    }

    public boolean isStatDerived() {
        return statDerived;
    }

    public void setStatDerived(boolean statDerived) {
        this.statDerived = statDerived;
    }

    public Map<PhysicalProperties, Pair<Double, List<PhysicalProperties>>> getLowestCostTable() {
        return lowestCostTable;
    }

    public List<PhysicalProperties> getInputPropertiesList(PhysicalProperties require) {
        Preconditions.checkState(lowestCostTable.containsKey(require));
        return lowestCostTable.get(require).second;
    }

    /**
     * Add a (outputProperties) -> (cost, childrenInputProperties) in lowestCostTable.
     */
    public boolean updateLowestCostTable(PhysicalProperties outputProperties,
            List<PhysicalProperties> childrenInputProperties, double cost) {
        if (lowestCostTable.containsKey(outputProperties)) {
            if (lowestCostTable.get(outputProperties).first > cost) {
                lowestCostTable.put(outputProperties, Pair.of(cost, childrenInputProperties));
                return true;
            } else {
                return false;
            }
        } else {
            lowestCostTable.put(outputProperties, Pair.of(cost, childrenInputProperties));
            return true;
        }
    }

    /**
     * get the lowest cost when satisfy property
     *
     * @param property property that needs to be satisfied
     * @return Lowest cost to satisfy that property
     */
    public double getCostByProperties(PhysicalProperties property) {
        Preconditions.checkState(lowestCostTable.containsKey(property));
        return lowestCostTable.get(property).first;
    }

    public void putOutputPropertiesMap(PhysicalProperties outputPropertySet,
            PhysicalProperties requiredPropertySet) {
        this.requestPropertiesMap.put(requiredPropertySet, outputPropertySet);
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public CostEstimate getCostEstimate() {
        return costEstimate;
    }

    public void setCostEstimate(CostEstimate costEstimate) {
        this.costEstimate = costEstimate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupExpression that = (GroupExpression) o;
        // TODO: add relation id to UnboundRelation
        if (plan instanceof UnboundRelation) {
            return false;
        }
        return children.equals(that.children) && plan.equals(that.plan)
                && plan.getLogicalProperties().equals(that.plan.getLogicalProperties());
    }

    @Override
    public int hashCode() {
        return Objects.hash(children, plan);
    }

    public StatsDeriveResult getCopyOfChildStats(int idx) {
        return child(idx).getStatistics().copy();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        if (ownerGroup == null) {
            builder.append("OWNER GROUP IS NULL[]");
        } else {
            builder.append(ownerGroup.getGroupId()).append(" cost=").append((long) cost);
        }

        if (costEstimate != null) {
            builder.append(" est=").append(costEstimate);
        }
        builder.append(" (plan=" + plan.toString() + ") children=[");
        for (Group group : children) {
            builder.append(group.getGroupId()).append(" ");
        }
        builder.append("]");
        if (ownerGroup != null) {
            builder.append(" stats=").append(ownerGroup.getStatistics());
        }
        return builder.toString();
    }
}

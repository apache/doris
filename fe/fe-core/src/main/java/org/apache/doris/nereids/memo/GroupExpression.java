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
import org.apache.doris.nereids.metrics.EventChannel;
import org.apache.doris.nereids.metrics.EventProducer;
import org.apache.doris.nereids.metrics.consumer.LogConsumer;
import org.apache.doris.nereids.metrics.event.CostStateUpdateEvent;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.text.DecimalFormat;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Representation for group expression in cascades optimizer.
 */
public class GroupExpression {
    private static final EventProducer COST_STATE_TRACER = new EventProducer(CostStateUpdateEvent.class,
            EventChannel.getDefaultChannel().addConsumers(new LogConsumer(CostStateUpdateEvent.class,
                    EventChannel.LOG)));
    private Cost cost;
    private Group ownerGroup;
    private final List<Group> children;
    private final Plan plan;
    private final BitSet ruleMasks;
    private boolean statDerived;

    private double estOutputRowCount = -1;

    // Record the rule that generate this plan. It's used for debugging
    private Rule fromRule;

    // Mapping from output properties to the corresponding best cost, statistics, and child properties.
    // key is the physical properties the group expression support for its parent
    // and value is cost and request physical properties to its children.
    private final Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> lowestCostTable;
    // Each physical group expression maintains mapping incoming requests to the corresponding child requests.
    // key is the output physical properties satisfying the incoming request properties
    // value is the request physical properties
    private final Map<PhysicalProperties, PhysicalProperties> requestPropertiesMap;

    // After mergeGroup(), source Group was cleaned up, but it may be in the Job Stack. So use this to mark and skip it.
    private boolean isUnused = false;

    private final ObjectId id = StatementScopeIdGenerator.newObjectId();

    /**
     * Just for UT.
     */
    public GroupExpression(Plan plan) {
        this(plan, Lists.newArrayList());
    }

    /**
     * Notice!!!: children will use param `children` directly, So don't modify it after this constructor outside.
     * Constructor for GroupExpression.
     *
     * @param plan {@link Plan} to reference
     * @param children children groups in memo
     */
    public GroupExpression(Plan plan, List<Group> children) {
        this.plan = Objects.requireNonNull(plan, "plan can not be null")
                .withGroupExpression(Optional.of(this));
        this.children = Objects.requireNonNull(children, "children can not be null");
        this.children.forEach(childGroup -> childGroup.addParentExpression(this));
        this.ruleMasks = new BitSet(RuleType.SENTINEL.ordinal());
        this.statDerived = false;
        this.lowestCostTable = Maps.newHashMap();
        this.requestPropertiesMap = Maps.newHashMap();
    }

    public PhysicalProperties getOutputProperties(PhysicalProperties requestProperties) {
        PhysicalProperties outputProperties = requestPropertiesMap.get(requestProperties);
        Preconditions.checkNotNull(outputProperties);
        return outputProperties;
    }

    public int arity() {
        return children.size();
    }

    public void setFromRule(Rule rule) {
        this.fromRule = rule;
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

    public void setChild(int i, Group group) {
        child(i).removeParentExpression(this);
        children.set(i, group);
        group.addParentExpression(this);
    }

    public List<Group> children() {
        return children;
    }

    /**
     * replaceChild.
     *
     * @param oldChild origin child group
     * @param newChild new child group
     */
    public void replaceChild(Group oldChild, Group newChild) {
        oldChild.removeParentExpression(this);
        newChild.addParentExpression(this);
        Utils.replaceList(children, oldChild, newChild);
    }

    public boolean hasApplied(Rule rule) {
        return ruleMasks.get(rule.getRuleType().ordinal());
    }

    public boolean notApplied(Rule rule) {
        return !hasApplied(rule);
    }

    public void setApplied(Rule rule) {
        ruleMasks.set(rule.getRuleType().ordinal());
    }

    public void propagateApplied(GroupExpression toGroupExpression) {
        toGroupExpression.ruleMasks.or(ruleMasks);
    }

    public void clearApplied() {
        ruleMasks.clear();
    }

    public boolean isStatDerived() {
        return statDerived;
    }

    public void setStatDerived(boolean statDerived) {
        this.statDerived = statDerived;
    }

    /**
     * Check this GroupExpression isUnused. See detail of `isUnused` in its comment.
     */
    public boolean isUnused() {
        if (isUnused) {
            Preconditions.checkState(children.isEmpty() && ownerGroup == null);
            return true;
        }
        Preconditions.checkState(ownerGroup != null);
        return false;
    }

    public void setUnused(boolean isUnused) {
        this.isUnused = isUnused;
    }

    public Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> getLowestCostTable() {
        return lowestCostTable;
    }

    public List<PhysicalProperties> getInputPropertiesList(PhysicalProperties require) {
        Preconditions.checkState(lowestCostTable.containsKey(require));
        return lowestCostTable.get(require).second;
    }

    public List<PhysicalProperties> getInputPropertiesListOrEmpty(PhysicalProperties require) {
        Pair<Cost, List<PhysicalProperties>> costAndChildRequire = lowestCostTable.get(require);
        return costAndChildRequire == null ? ImmutableList.of() : costAndChildRequire.second;
    }

    /**
     * Add a (outputProperties) -> (cost, childrenInputProperties) in lowestCostTable.
     * if the outputProperties exists, will be covered.
     *
     * @return true if lowest cost table change.
     */
    public boolean updateLowestCostTable(PhysicalProperties outputProperties,
            List<PhysicalProperties> childrenInputProperties, Cost cost) {
        COST_STATE_TRACER.log(CostStateUpdateEvent.of(this, cost.getValue(), outputProperties));
        if (lowestCostTable.containsKey(outputProperties)) {
            if (lowestCostTable.get(outputProperties).first.getValue() > cost.getValue()) {
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
        return lowestCostTable.get(property).first.getValue();
    }

    public Cost getCostValueByProperties(PhysicalProperties property) {
        Preconditions.checkState(lowestCostTable.containsKey(property));
        return lowestCostTable.get(property).first;
    }

    public void putOutputPropertiesMap(PhysicalProperties outputProperties,
            PhysicalProperties requiredProperties) {
        this.requestPropertiesMap.put(requiredProperties, outputProperties);
    }

    /**
     * Merge GroupExpression.
     */
    public void mergeTo(GroupExpression target) {
        this.ownerGroup.removeGroupExpression(this);
        this.mergeToNotOwnerRemove(target);
    }

    /**
     * Merge GroupExpression, but owner don't remove this GroupExpression.
     */
    public void mergeToNotOwnerRemove(GroupExpression target) {
        // LowestCostTable
        this.getLowestCostTable()
                .forEach((properties, pair) -> target.updateLowestCostTable(properties, pair.second, pair.first));
        // requestPropertiesMap
        target.requestPropertiesMap.putAll(this.requestPropertiesMap);
        // ruleMasks
        target.ruleMasks.or(this.ruleMasks);

        // clear
        this.children.forEach(child -> child.removeParentExpression(this));
        this.children.clear();
        this.ownerGroup = null;
    }

    public Cost getCost() {
        return cost;
    }

    public void setCost(Cost cost) {
        this.cost = cost;
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
        return children.equals(that.children) && plan.equals(that.plan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(children, plan);
    }

    public Statistics childStatistics(int idx) {
        return child(idx).getStatistics();
    }

    public void setEstOutputRowCount(double estOutputRowCount) {
        this.estOutputRowCount = estOutputRowCount;
    }

    @Override
    public String toString() {
        DecimalFormat format = new DecimalFormat("#,###.##");
        StringBuilder builder = new StringBuilder("id:");
        builder.append(id.asInt());
        if (ownerGroup == null) {
            builder.append("OWNER GROUP IS NULL[]");
        } else {
            builder.append("#").append(ownerGroup.getGroupId().asInt());
        }
        if (cost != null) {
            builder.append(" cost=").append(format.format((long) cost.getValue()) + " " + cost);
        } else {
            builder.append(" cost=null");
        }
        builder.append(" estRows=").append(format.format(estOutputRowCount));
        builder.append(" children=[").append(Joiner.on(", ").join(
                        children.stream().map(Group::getGroupId).collect(Collectors.toList())))
                .append(" ]");
        builder.append(" (plan=").append(plan.toString()).append(")");
        return builder.toString();
    }

    public ObjectId getId() {
        return id;
    }
}

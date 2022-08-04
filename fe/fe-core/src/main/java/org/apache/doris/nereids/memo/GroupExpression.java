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
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Representation for group expression in cascades optimizer.
 */
public class GroupExpression {
    private Group ownerGroup;
    private List<Group> children;
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
        this.plan = Objects.requireNonNull(plan, "plan can not be null");
        this.children = Objects.requireNonNull(children);
        this.ruleMasks = new BitSet(RuleType.SENTINEL.ordinal());
        this.statDerived = false;
        this.lowestCostTable = Maps.newHashMap();
        this.requestPropertiesMap = Maps.newHashMap();
    }

    // TODO: rename
    public PhysicalProperties getPropertyFromMap(PhysicalProperties requiredPropertySet) {
        PhysicalProperties outputProperty = requestPropertiesMap.get(requiredPropertySet);
        Preconditions.checkState(outputProperty != null);
        return outputProperty;
    }

    public int arity() {
        return children.size();
    }

    public void addChild(Group child) {
        children.add(child);
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

    public void setChildren(List<Group> children) {
        this.children = children;
    }

    public BitSet getRuleMasks() {
        return ruleMasks;
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
    public boolean updateLowestCostTable(
            PhysicalProperties outputProperties,
            List<PhysicalProperties> childrenInputProperties,
            double cost) {
        if (lowestCostTable.containsKey(outputProperties)) {
            if (lowestCostTable.get(outputProperties).first > cost) {
                lowestCostTable.put(outputProperties, new Pair<>(cost, childrenInputProperties));
                return true;
            }
        } else {
            lowestCostTable.put(outputProperties, new Pair<>(cost, childrenInputProperties));
            return true;
        }
        return false;
    }

    public void putOutputPropertiesMap(PhysicalProperties outputPropertySet,
            PhysicalProperties requiredPropertySet) {
        this.requestPropertiesMap.put(requiredPropertySet, outputPropertySet);
    }

    private enum JoinReorderMark {
        // left deep
        HAS_COMMUTE,
        HAS_LASSCOM,

        // zig-zag
        HAS_COMMUTE_ZIG_ZAG,

        // bushy
        HAS_EXCHANGE,
        HAS_RIGHT_ASSOCIATE,
        HAS_LEFT_ASSOCIATE;

        public static int getCommuteOrdinal() {
            return RuleType.SENTINEL.ordinal() + JoinReorderMark.HAS_COMMUTE.ordinal();
        }

        public static int getLAsscomOrdinal() {
            return RuleType.LOGICAL_JOIN_LASSCOM.ordinal() + JoinReorderMark.HAS_LASSCOM.ordinal();
        }

        public static int getCommuteZigZagOrdinal() {
            return RuleType.LOGICAL_JOIN_LASSCOM.ordinal() + JoinReorderMark.HAS_COMMUTE_ZIG_ZAG.ordinal();
        }

        public static int getExchangeOrdinal() {
            return RuleType.LOGICAL_JOIN_LASSCOM.ordinal() + JoinReorderMark.HAS_EXCHANGE.ordinal();
        }

        public static int getRightAssociateOrdinal() {
            return RuleType.LOGICAL_JOIN_LASSCOM.ordinal() + JoinReorderMark.HAS_RIGHT_ASSOCIATE.ordinal();
        }

        public static int getLeftAssociateOrdinal() {
            return RuleType.LOGICAL_JOIN_LASSCOM.ordinal() + JoinReorderMark.HAS_LEFT_ASSOCIATE.ordinal();
        }
    }

    /**
     * Copy duplicate-free mark.
     * Use mark to avoid duplicate of join reorder.
     * Paper:
     * - Optimizing Join Enumeration in Transformation-based Query Optimizers
     * - Improving Join Reorderability with Compensation Operators
     */
    public void copyMark(BitSet copyFrom) {
        if (hasCommute(copyFrom)) {
            ruleMasks.set(RuleType.LOGICAL_JOIN_COMMUTE.ordinal());
        }
        if (hasLAsscom(copyFrom)) {
            ruleMasks.set(RuleType.LOGICAL_JOIN_LASSCOM.ordinal());
        }
    }

    public static boolean hasCommute(BitSet marks) {
        return marks.get(JoinReorderMark.getCommuteOrdinal());
    }

    public static void setHasCommute(BitSet marks) {
        marks.set(JoinReorderMark.getCommuteOrdinal());
    }

    public static boolean hasLAsscom(BitSet marks) {
        return marks.get(JoinReorderMark.getLAsscomOrdinal());
    }

    public static void sethasLAsscom(BitSet marks) {
        marks.set(JoinReorderMark.getLAsscomOrdinal());
    }

    public static boolean hasCommuteZigZag(BitSet marks) {
        return marks.get(JoinReorderMark.getCommuteZigZagOrdinal());
    }

    public static void setCommuteZigZag(BitSet marks) {
        marks.set(JoinReorderMark.getCommuteZigZagOrdinal());
    }

    public static boolean hasExchange(BitSet marks) {
        return marks.get(JoinReorderMark.getExchangeOrdinal());
    }

    public static void setHasExchange(BitSet marks) {
        marks.set(JoinReorderMark.getExchangeOrdinal());
    }

    public static boolean hasRightAssociate(BitSet marks) {
        return marks.get(JoinReorderMark.getRightAssociateOrdinal());
    }

    public static void setHasRightAssociate(BitSet marks) {
        marks.set(JoinReorderMark.getRightAssociateOrdinal());
    }

    public static boolean isHasLeftAssociate(BitSet marks) {
        return marks.get(JoinReorderMark.getLeftAssociateOrdinal());
    }

    public static void setHasLeftAssociate(BitSet marks) {
        marks.set(JoinReorderMark.getLeftAssociateOrdinal());
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

    public StatsDeriveResult getCopyOfChildStats(int idx) {
        return child(idx).getStatistics().copy();
    }
}

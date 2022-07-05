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
import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;

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
    private Group parent;
    private List<Group> children;
    private final Operator op;
    private final BitSet ruleMasks;
    private boolean statDerived;

    // Mapping from output properties to the corresponding best cost, statistics, and child properties.
    private final Map<PhysicalProperties, Pair<Double, List<PhysicalProperties>>> lowestCostTable;

    public GroupExpression(Operator op) {
        this(op, Lists.newArrayList());
    }

    /**
     * Constructor for GroupExpression.
     *
     * @param op {@link Operator} to reference
     * @param children children groups in memo
     */
    public GroupExpression(Operator op, List<Group> children) {
        this.op = Objects.requireNonNull(op);
        this.children = Objects.requireNonNull(children);
        this.ruleMasks = new BitSet(RuleType.SENTINEL.ordinal());
        this.statDerived = false;
        this.lowestCostTable = Maps.newHashMap();
    }

    public int arity() {
        return children.size();
    }

    public void addChild(Group child) {
        children.add(child);
    }

    public Group getParent() {
        return parent;
    }

    public void setParent(Group parent) {
        this.parent = parent;
    }

    public Operator getOperator() {
        return op;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupExpression that = (GroupExpression) o;
        return children.equals(that.children) && op.equals(that.op);
    }

    @Override
    public int hashCode() {
        return Objects.hash(children, op);
    }
}

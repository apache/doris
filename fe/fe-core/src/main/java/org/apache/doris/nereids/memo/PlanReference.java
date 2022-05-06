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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;

import com.clearspring.analytics.util.Lists;

import java.util.BitSet;
import java.util.List;

/**
 * Representation for group expression in cascades optimizer.
 */
public class PlanReference {
    private Group parent;
    private List<Group> children;
    private final Plan<?> plan;
    private final BitSet ruleMasks;
    private boolean statDerived;

    public PlanReference(Plan<?> plan) {
        this(plan, Lists.newArrayList());
    }

    /**
     * Constructor for PlanReference.
     *
     * @param plan {@link Plan} to reference
     * @param children children groups in memo
     */
    public PlanReference(Plan<?> plan, List<Group> children) {
        this.plan = plan;
        this.children = children;
        this.ruleMasks = new BitSet(RuleType.SENTINEL.ordinal());
        this.statDerived = false;
        plan.setPlanReference(this);
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

    public Plan<?> getPlan() {
        return plan;
    }

    public List<Group> getChildren() {
        return children;
    }

    public void setChildren(List<Group> children) {
        this.children = children;
    }

    public boolean hasExplored(Rule rule) {
        return ruleMasks.get(rule.getRuleType().ordinal());
    }

    public void setExplored(Rule rule) {
        ruleMasks.set(rule.getRuleType().ordinal());
    }

    public boolean isStatDerived() {
        return statDerived;
    }

    public void setStatDerived(boolean statDerived) {
        this.statDerived = statDerived;
    }
}

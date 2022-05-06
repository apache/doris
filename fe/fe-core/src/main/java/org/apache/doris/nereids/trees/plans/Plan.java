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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.memo.PlanReference;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.alibaba.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class for all plan node.
 *
 * @param <PlanType> either {@link org.apache.doris.nereids.trees.plans.logical.LogicalPlan}
 *                  or {@link org.apache.doris.nereids.trees.plans.physical.PhysicalPlan}
 */
public abstract class Plan<PlanType extends Plan<PlanType>> extends TreeNode<PlanType> {

    protected final boolean isPhysical;
    protected PlanReference planReference;
    protected List<Slot> output = Lists.newArrayList();

    public Plan(NodeType type, boolean isPhysical) {
        super(type);
        this.isPhysical = isPhysical;
    }

    public org.apache.doris.nereids.trees.NodeType getType() {
        return type;
    }

    public boolean isPhysical() {
        return isPhysical;
    }

    public boolean isLogical() {
        return !isPhysical;
    }

    public abstract List<Slot> getOutput() throws UnboundException;

    public PlanReference getPlanReference() {
        return planReference;
    }

    public void setPlanReference(PlanReference planReference) {
        this.planReference = planReference;
    }

    /**
     * Get tree like string describing query plan.
     *
     * @return tree like string describing query plan
     */
    public String treeString() {
        List<String> lines = new ArrayList<>();
        treeString(lines, 0, new ArrayList<>(), this);
        return StringUtils.join(lines, "\n");
    }

    private void treeString(List<String> lines, int depth, List<Boolean> lastChildren, Plan<PlanType> plan) {
        StringBuilder sb = new StringBuilder();
        if (depth > 0) {
            if (lastChildren.size() > 1) {
                for (int i = 0; i < lastChildren.size() - 1; i++) {
                    sb.append(lastChildren.get(i) ? "   " : "|  ");
                }
            }
            if (lastChildren.size() > 0) {
                Boolean last = lastChildren.get(lastChildren.size() - 1);
                sb.append(last ? "+--" : "|--");
            }
        }
        sb.append(plan.toString());
        lines.add(sb.toString());

        List<PlanType> children = plan.getChildren();
        for (int i = 0; i < children.size(); i++) {
            List<Boolean> newLasts = new ArrayList<>(lastChildren);
            newLasts.add(i + 1 == children.size());
            treeString(lines, depth + 1, newLasts, children.get(i));
        }
    }
}

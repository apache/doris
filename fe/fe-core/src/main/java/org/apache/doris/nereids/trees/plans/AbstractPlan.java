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

import org.apache.doris.nereids.operators.plans.PlanOperator;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.NodeType;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Abstract class for all concrete plan node.
 *
 * @param <PLAN_TYPE> either {@link org.apache.doris.nereids.trees.plans.logical.LogicalPlan}
 *                  or {@link org.apache.doris.nereids.trees.plans.physical.PhysicalPlan}
 */
public abstract class AbstractPlan<
        PLAN_TYPE extends AbstractPlan<PLAN_TYPE, OP_TYPE>,
        OP_TYPE extends PlanOperator>
        extends AbstractTreeNode<PLAN_TYPE> implements Plan<PLAN_TYPE, OP_TYPE> {

    public final OP_TYPE op;

    public AbstractPlan(NodeType type, OP_TYPE operator, Plan... children) {
        super(type, children);
        this.op = Objects.requireNonNull(operator, "operator can not be null");
    }

    @Override
    public OP_TYPE getOperator() {
        return op;
    }

    @Override
    public List<Plan> children() {
        return (List) children;
    }

    @Override
    public Plan child(int index) {
        return (Plan) children.get(index);
    }

    /**
     * Get tree like string describing query plan.
     *
     * @return tree like string describing query plan
     */
    @Override
    public String treeString() {
        List<String> lines = new ArrayList<>();
        treeString(lines, 0, new ArrayList<>(), this);
        return StringUtils.join(lines, "\n");
    }

    private void treeString(List<String> lines, int depth, List<Boolean> lastChildren, Plan plan) {
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

        List<Plan> children = plan.children();
        for (int i = 0; i < children.size(); i++) {
            List<Boolean> newLasts = new ArrayList<>(lastChildren);
            newLasts.add(i + 1 == children.size());
            treeString(lines, depth + 1, newLasts, children.get(i));
        }
    }
}

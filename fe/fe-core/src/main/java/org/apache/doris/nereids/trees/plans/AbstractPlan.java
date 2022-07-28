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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.statistics.StatsDeriveResult;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Abstract class for all concrete plan node.
 */
public abstract class AbstractPlan extends AbstractTreeNode<Plan> implements Plan {

    protected StatsDeriveResult statsDeriveResult;
    protected long limit = -1;

    protected final PlanType type;
    protected final LogicalProperties logicalProperties;

    public AbstractPlan(PlanType type, Plan... children) {
        this(type, Optional.empty(), Optional.empty(), children);
    }

    public AbstractPlan(PlanType type, Optional<LogicalProperties> optLogicalProperties, Plan... children) {
        this(type, Optional.empty(), optLogicalProperties, children);
    }

    /** all parameter constructor. */
    public AbstractPlan(PlanType type, Optional<GroupExpression> groupExpression,
                        Optional<LogicalProperties> optLogicalProperties, Plan... children) {
        super(groupExpression, children);
        this.type = Objects.requireNonNull(type, "type can not be null");
        LogicalProperties logicalProperties = optLogicalProperties.orElseGet(() -> computeLogicalProperties(children));
        this.logicalProperties = Objects.requireNonNull(logicalProperties, "logicalProperties can not be null");
    }

    @Override
    public PlanType getType() {
        return type;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractPlan that = (AbstractPlan) o;
        return limit == that.limit
                && Objects.equals(statsDeriveResult, that.statsDeriveResult)
                && Objects.equals(logicalProperties, that.logicalProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statsDeriveResult, limit, logicalProperties);
    }

    public long getLimit() {
        return limit;
    }
}

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
import org.apache.doris.nereids.operators.plans.PlanOperator;
import org.apache.doris.nereids.operators.plans.logical.LogicalOperator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalOperator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.NodeType;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Abstract class for all concrete plan node.
 */
public abstract class AbstractPlan<OP_TYPE extends PlanOperator>
        extends AbstractTreeNode<Plan> implements Plan {

    public final OP_TYPE operator;

    protected final Supplier<LogicalProperties> logicalProperties;

    public AbstractPlan(NodeType type, OP_TYPE operator,
                        Optional<LogicalProperties> logicalProperties, Plan... children) {
        this(type, operator, Optional.empty(), logicalProperties, children);
    }

    /** all parameter constructor. */
    public AbstractPlan(NodeType type, OP_TYPE operator, Optional<GroupExpression> groupExpression,
                        Optional<LogicalProperties> logicalProperties, Plan... children) {
        super(type, groupExpression, children);
        this.operator = Objects.requireNonNull(operator, "operator can not be null");
        this.logicalProperties = toLogicalPropsSupplier(
            Objects.requireNonNull(logicalProperties, "logicalProperties can not be null"));
    }

    @Override
    public OP_TYPE getOperator() {
        return operator;
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

    private Supplier<LogicalProperties> toLogicalPropsSupplier(Optional<LogicalProperties> logicalProperties) {
        if (logicalProperties.isPresent()) {
            return Suppliers.ofInstance(logicalProperties.get());
        } else if (operator instanceof LogicalOperator) {
            // lazy compute logical properties. supplier can prevent throw exception by UnboundRelation.
            return Suppliers.memoize(() ->
                new LogicalProperties(((LogicalOperator) operator).computeOutput(children))
            );
        } else if (operator instanceof PhysicalOperator) {
            throw new IllegalStateException("Missing logical properties for physical operator");
        } else {
            throw new IllegalStateException("Unsupported compute logical properties for operator: "
                + operator.getClass());
        }
    }
}

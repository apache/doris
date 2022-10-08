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

import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.TreeStringUtils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Abstract class for all concrete plan node.
 */
public abstract class AbstractPlan extends AbstractTreeNode<Plan> implements Plan {

    protected final StatsDeriveResult statsDeriveResult;
    protected final PlanType type;
    protected final Optional<GroupExpression> groupExpression;
    protected final Supplier<LogicalProperties> logicalPropertiesSupplier;

    public AbstractPlan(PlanType type, Plan... children) {
        this(type, Optional.empty(), Optional.empty(), null, children);
    }

    public AbstractPlan(PlanType type, Optional<LogicalProperties> optLogicalProperties, Plan... children) {
        this(type, Optional.empty(), optLogicalProperties, null, children);
    }

    /**
     * all parameter constructor.
     */
    public AbstractPlan(PlanType type, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> optLogicalProperties, @Nullable StatsDeriveResult statsDeriveResult,
            Plan... children) {
        super(groupExpression, children);
        this.type = Objects.requireNonNull(type, "type can not be null");
        this.groupExpression = Objects.requireNonNull(groupExpression, "groupExpression can not be null");
        Objects.requireNonNull(optLogicalProperties, "logicalProperties can not be null");
        this.logicalPropertiesSupplier = Suppliers.memoize(() -> optLogicalProperties.orElseGet(
                this::computeLogicalProperties));
        this.statsDeriveResult = statsDeriveResult;
    }

    @Override
    public PlanType getType() {
        return type;
    }

    public Optional<GroupExpression> getGroupExpression() {
        return groupExpression;
    }

    public StatsDeriveResult getStats() {
        return statsDeriveResult;
    }

    @Override
    public boolean canBind() {
        return !bound()
                && !(this instanceof Unbound)
                && childrenBound();
    }

    /**
     * Get tree like string describing query plan.
     *
     * @return tree like string describing query plan
     */
    @Override
    public String treeString() {
        return TreeStringUtils.treeString(this,
                plan -> plan.toString(),
                plan -> (List) ((Plan) plan).children());
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
        // stats should don't need.
        return Objects.equals(getLogicalProperties(), that.getLogicalProperties());
    }

    @Override
    public int hashCode() {
        // stats should don't need.
        return Objects.hash(getLogicalProperties());
    }

    @Override
    public List<Slot> getOutput() {
        return getLogicalProperties().getOutput();
    }

    @Override
    public Set<ExprId> getOutputExprIdSet() {
        return getLogicalProperties().getOutputExprIdSet();
    }

    @Override
    public Plan child(int index) {
        return super.child(index);
    }

    @Override
    public LogicalProperties getLogicalProperties() {
        return logicalPropertiesSupplier.get();
    }
}

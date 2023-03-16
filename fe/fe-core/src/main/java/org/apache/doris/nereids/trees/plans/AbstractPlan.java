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
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.metrics.CounterType;
import org.apache.doris.nereids.metrics.EventChannel;
import org.apache.doris.nereids.metrics.EventProducer;
import org.apache.doris.nereids.metrics.consumer.LogConsumer;
import org.apache.doris.nereids.metrics.enhancer.AddCounterEventEnhancer;
import org.apache.doris.nereids.metrics.event.CounterEvent;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.nereids.util.MutableState.EmptyMutableState;
import org.apache.doris.nereids.util.TreeStringUtils;
import org.apache.doris.statistics.Statistics;

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
    private static final EventProducer PLAN_CONSTRUCT_TRACER = new EventProducer(CounterEvent.class,
            EventChannel.getDefaultChannel()
                    .addEnhancers(new AddCounterEventEnhancer())
                    .addConsumers(new LogConsumer(CounterEvent.class, EventChannel.LOG)));

    protected final Statistics statistics;
    protected final PlanType type;
    protected final Optional<GroupExpression> groupExpression;
    protected final Supplier<LogicalProperties> logicalPropertiesSupplier;

    // this field is special, because other fields in tree node is immutable, but in some scenes, mutable
    // state is necessary. e.g. the rewrite framework need distinguish whether the plan is created by
    // rules, the framework can set this field to a state variable to quickly judge without new big plan.
    // we should avoid using it as much as possible, because mutable state is easy to cause bugs and
    // difficult to locate.
    private MutableState mutableState = EmptyMutableState.INSTANCE;

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
            Optional<LogicalProperties> optLogicalProperties, @Nullable Statistics statistics,
            Plan... children) {
        super(groupExpression, children);
        this.type = Objects.requireNonNull(type, "type can not be null");
        this.groupExpression = Objects.requireNonNull(groupExpression, "groupExpression can not be null");
        Objects.requireNonNull(optLogicalProperties, "logicalProperties can not be null");
        this.logicalPropertiesSupplier = Suppliers.memoize(() -> optLogicalProperties.orElseGet(
                this::computeLogicalProperties));
        this.statistics = statistics;
        PLAN_CONSTRUCT_TRACER.log(CounterEvent.of(Memo.getStateId(), CounterType.PLAN_CONSTRUCTOR, null, null, null));
    }

    @Override
    public PlanType getType() {
        return type;
    }

    public Optional<GroupExpression> getGroupExpression() {
        return groupExpression;
    }

    public Statistics getStats() {
        return statistics;
    }

    @Override
    public boolean canBind() {
        return !bound() && childrenBound();
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
                plan -> (List) ((Plan) plan).children(),
                plan -> (List) ((Plan) plan).extraPlans(),
                plan -> ((Plan) plan).displayExtraPlanFirst());
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
    public List<Slot> getNonUserVisibleOutput() {
        return getLogicalProperties().getNonUserVisibleOutput();
    }

    @Override
    public Set<Slot> getOutputSet() {
        return getLogicalProperties().getOutputSet();
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
        if (this instanceof Unbound) {
            return UnboundLogicalProperties.INSTANCE;
        }
        return logicalPropertiesSupplier.get();
    }

    @Override
    public Optional<Object> getMutableState(String key) {
        return mutableState.get(key);
    }

    @Override
    public void setMutableState(String key, Object state) {
        this.mutableState = this.mutableState.set(key, state);
    }

    /**
     * used in treeString()
     * @return "" if groupExpression is empty, o.w. string format of group id
     */
    public String getGroupIdAsString() {
        String groupId = getGroupExpression().isPresent()
                ? "#" + getGroupExpression().get().getOwnerGroup().getGroupId().asInt()
                : "";
        return groupId;
    }
}

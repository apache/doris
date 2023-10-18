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

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.nereids.util.MutableState.EmptyMutableState;
import org.apache.doris.nereids.util.TreeStringUtils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Abstract class for all concrete plan node.
 */
public abstract class AbstractPlan extends AbstractTreeNode<Plan> implements Plan {
    public static final String FRAGMENT_ID = "fragment";

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

    protected AbstractPlan(PlanType type, List<Plan> children) {
        this(type, Optional.empty(), Optional.empty(), null, children);
    }

    /**
     * all parameter constructor.
     */
    protected AbstractPlan(PlanType type, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> optLogicalProperties, @Nullable Statistics statistics,
            List<Plan> children) {
        super(children);
        this.type = Objects.requireNonNull(type, "type can not be null");
        this.groupExpression = Objects.requireNonNull(groupExpression, "groupExpression can not be null");
        Objects.requireNonNull(optLogicalProperties, "logicalProperties can not be null");
        this.logicalPropertiesSupplier = Suppliers.memoize(() -> optLogicalProperties.orElseGet(
                this::computeLogicalProperties));
        this.statistics = statistics;
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
        return !bound() && children().stream().allMatch(Plan::bound);
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

    /** top toJson method, can be override by specific operator */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("PlanType", getType().toString());
        if (this.children().isEmpty()) {
            return json;
        }
        JSONArray childrenJson = new JSONArray();
        for (Plan child : children) {
            childrenJson.put(((AbstractPlan) child).toJson());
        }
        json.put("children", childrenJson);
        return json;
    }

    @Override
    public List<Slot> getOutput() {
        return getLogicalProperties().getOutput();
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
    public LogicalProperties getLogicalProperties() {
        return logicalPropertiesSupplier.get();
    }

    @Override
    public LogicalProperties computeLogicalProperties() {
        // After analyzer, we should skip to check bound.
        if (NereidsPlanner.isAnalyzerPhase) {
            if (children.stream().anyMatch(child -> !child.bound())
                    || (this instanceof Unbound || this.getExpressions().stream().anyMatch(Expression::hasUnbound))) {
                return UnboundLogicalProperties.INSTANCE;
            }
        }
        return new LogicalProperties(this::computeOutput);
    }

    @Override
    public Optional<Object> getMutableState(String key) {
        return mutableState.get(key);
    }

    @Override
    public void setMutableState(String key, Object state) {
        this.mutableState = this.mutableState.set(key, state);
    }
}

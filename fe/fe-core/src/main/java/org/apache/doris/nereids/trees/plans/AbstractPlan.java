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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.stats.HboUtils;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.TreeStringPlan.TreeStringNode;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.util.LazyCompute;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.nereids.util.TreeStringUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Abstract class for all concrete plan node.
 */
public abstract class AbstractPlan extends AbstractTreeNode<Plan> implements Plan {
    public static final String FRAGMENT_ID = "fragment";

    private static final ObjectId zeroId = new ObjectId(0);

    public final int depth;

    protected final ObjectId id;
    protected final PlanType type;
    protected final Optional<GroupExpression> groupExpression;
    protected final Supplier<LogicalProperties> logicalPropertiesSupplier;
    protected final Supplier<Boolean> hasUnboundChild;
    protected Statistics statistics;

    /**
     * all parameter constructor.
     */
    protected AbstractPlan(PlanType type, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> optLogicalProperties, @Nullable Statistics statistics,
            List<Plan> children) {
        super(children);
        this.type = Objects.requireNonNull(type, "type can not be null");
        this.groupExpression = Objects.requireNonNull(groupExpression, "groupExpression can not be null");
        this.logicalPropertiesSupplier
                = Objects.requireNonNull(optLogicalProperties, "logicalProperties can not be null").isPresent()
                    ? optLogicalProperties::get
                    : LazyCompute.of(this::computeLogicalProperties);
        this.statistics = statistics;
        this.id = StatementScopeIdGenerator.newObjectId();
        this.hasUnboundChild = buildHasUnboundChildCache();

        switch (children.size()) {
            case 0: {
                this.depth = 1;
                break;
            }
            case 1: {
                this.depth = 1 + children.get(0).depth();
                break;
            }
            case 2: {
                this.depth = 1 + Math.max(children.get(0).depth(), children.get(1).depth());
                break;
            }
            default: {
                this.depth = computeDepth();
            }
        }
    }

    protected AbstractPlan(PlanType type, Optional<GroupExpression> groupExpression,
            Supplier<LogicalProperties> logicalPropertiesSupplier, @Nullable Statistics statistics,
            List<Plan> children, boolean useZeroId) {
        super(children);
        this.type = Objects.requireNonNull(type, "type can not be null");
        this.groupExpression = Objects.requireNonNull(groupExpression, "groupExpression can not be null");
        this.logicalPropertiesSupplier = logicalPropertiesSupplier;
        this.statistics = statistics;
        Preconditions.checkArgument(useZeroId);
        this.id = zeroId;
        this.hasUnboundChild = buildHasUnboundChildCache();

        switch (children.size()) {
            case 0: {
                this.depth = 1;
                break;
            }
            case 1: {
                this.depth = 1 + children.get(0).depth();
                break;
            }
            case 2: {
                this.depth = 1 + Math.max(children.get(0).depth(), children.get(1).depth());
                break;
            }
            default: {
                this.depth = computeDepth();
            }
        }
    }

    private int computeDepth() {
        int depth = 0;
        for (Plan child : children) {
            int childDepth = child.depth();
            if (childDepth > depth) {
                depth = childDepth;
            }
        }
        return depth + 1;
    }

    @Override
    public int depth() {
        return depth;
    }

    @Override
    public PlanType getType() {
        return type;
    }

    public Optional<GroupExpression> getGroupExpression() {
        return groupExpression;
    }

    @Override
    public Statistics getStats() {
        return statistics;
    }

    public void setStatistics(Statistics statistics) {
        this.statistics = statistics;
    }

    @Override
    public boolean canBind() {
        if (bound()) {
            return false;
        }
        for (Plan child : children()) {
            if (!child.bound()) {
                return false;
            }
        }
        return true;
    }

    public String getFingerprint() {
        return "";
    }

    /**
     * Get fingerprint of plan.
     */
    public String getPlanTreeFingerprint() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getFingerprint());
        if (this.children().isEmpty()) {
            return builder.toString();
        } else {
            List<Plan> mutableChildren = new ArrayList<>(children);
            // the following sorting by scans depended on will increase the plan matching possibility
            // during hbo plan matching stage, e.g, the final physical plan is encoded as 'a join b',
            // but the group plan is 'b join a' which can be matched also.
            // NOTE: it will increase the risk brought from rf, as above, the physical plan 'a join b'
            // is with rf's potential influence which will not exactly the same as 'b join a',
            // but when we ignore the join sides as above and want to increase the hbo plan stats.'s adaptability,
            // it may bring the unsuitable matching and increase the dependence for the rf-safe checking.
            Collections.sort(mutableChildren, new Comparator<Plan>() {
                @Override
                public int compare(Plan plan1, Plan plan2) {
                    List<String> scanQualifierList1 = new ArrayList<>();
                    List<String> scanQualifierList2 = new ArrayList<>();
                    HboUtils.collectScanQualifierList((AbstractPlan) plan1, scanQualifierList1);
                    HboUtils.collectScanQualifierList((AbstractPlan) plan2, scanQualifierList2);
                    Collections.sort(scanQualifierList1);
                    Collections.sort(scanQualifierList2);
                    String qualifiedName1 = Utils.qualifiedName(scanQualifierList1, "");
                    String qualifiedName2 = Utils.qualifiedName(scanQualifierList2, "");
                    return qualifiedName1.compareTo(qualifiedName2);
                }
            });
            for (Plan plan : mutableChildren) {
                if (plan instanceof GroupPlan) {
                    builder.append(((GroupPlan) plan).getFingerprint());
                } else if (plan instanceof AbstractLogicalPlan) {
                    builder.append(((AbstractPlan) plan).getPlanTreeFingerprint());
                } else if (plan instanceof AbstractPhysicalPlan) {
                    if (!isLocalAggPhysicalNode((AbstractPhysicalPlan) plan)) {
                        builder.append(((AbstractPlan) plan).getPlanTreeFingerprint());
                    } else {
                        builder.append(((AbstractPlan) plan.child(0)).getPlanTreeFingerprint());
                    }
                } else {
                    throw new AnalysisException("illegal plan type getPlanTreeFingerprint");
                }
            }
            return builder.toString();
        }
    }

    public static boolean isLocalAggPhysicalNode(AbstractPhysicalPlan plan) {
        if (plan instanceof PhysicalHashAggregate && ((PhysicalHashAggregate<?>) plan).getAggPhase().isLocal()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean bound() {
        return !hasUnboundChild.get();
    }

    /**
     * Get tree like string describing query plan.
     *
     * @return tree like string describing query plan
     */
    @Override
    public String treeString(boolean printStates) {
        return TreeStringUtils.treeString(this,
                plan -> {
                    if (printStates && plan instanceof Plan) {
                        return plan + ", states: " + ((Plan) plan).getMutableStates();
                    }
                    return plan.toString();
                },
                plan -> {
                    if (plan instanceof TreeStringPlan) {
                        Optional<TreeStringNode> treeStringNode = ((TreeStringPlan) plan).parseTreeStringNode();
                        return treeStringNode.isPresent() ? ImmutableList.of(treeStringNode.get()) : ImmutableList.of();
                    }
                    if (plan instanceof TreeStringNode) {
                        return (List) ((TreeStringNode) plan).children;
                    }
                    if (!(plan instanceof Plan)) {
                        return ImmutableList.of();
                    }
                    return (List) ((Plan) plan).children();
                },
                plan -> {
                    if (!(plan instanceof Plan)) {
                        return ImmutableList.of();
                    }
                    return (List) ((Plan) plan).extraPlans();
                },
                plan -> {
                    if (!(plan instanceof Plan)) {
                        return false;
                    }
                    return ((Plan) plan).displayExtraPlanFirst();
                });
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
        // TODO: use bound()?
        if (this instanceof Unbound) {
            return UnboundLogicalProperties.INSTANCE;
        }
        return logicalPropertiesSupplier.get();
    }

    @Override
    public LogicalProperties computeLogicalProperties() {
        if (this instanceof DiffOutputInAsterisk) {
            return new LogicalProperties(this::computeOutput, this::computeAsteriskOutput, this::computeDataTrait);
        } else {
            return new LogicalProperties(this::computeOutput, this::computeDataTrait);
        }
    }

    public int getId() {
        return id.asInt();
    }

    /**
     * ancestors in the tree
     */
    public List<Plan> getAncestors() {
        List<Plan> ancestors = Lists.newArrayList();
        ancestors.add(this);
        Optional<Object> parent = this.getMutableState(MutableState.KEY_PARENT);
        while (parent.isPresent()) {
            ancestors.add((Plan) parent.get());
            parent = ((Plan) parent.get()).getMutableState(MutableState.KEY_PARENT);
        }
        return ancestors;
    }

    public void updateActualRowCount(long actualRowCount) {
        if (statistics != null) {
            statistics.setActualRowCount(actualRowCount);
        }
    }

    private Supplier<Boolean> buildHasUnboundChildCache() {
        return LazyCompute.of(() -> {
            if (hasUnboundExpression()) {
                return true;
            }
            for (Plan child : children) {
                if (!child.bound()) {
                    return true;
                }
            }
            return false;
        });
    }
}

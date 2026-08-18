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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Abstract class for all concrete physical plan.
 */
public abstract class AbstractPhysicalPlan extends AbstractPlan implements PhysicalPlan, Explainable {
    protected final PhysicalProperties physicalProperties;
    protected final List<RuntimeFilter> runtimeFilters = Lists.newArrayList();
    private final List<RuntimeFilter> appliedRuntimeFilters = Lists.newArrayList();

    public AbstractPhysicalPlan(PlanType type, LogicalProperties logicalProperties, Plan... children) {
        this(type, Optional.empty(), logicalProperties, children);
    }

    public AbstractPhysicalPlan(PlanType type, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, Plan... children) {
        this(type, groupExpression, logicalProperties, PhysicalProperties.ANY, null, children);
    }

    public AbstractPhysicalPlan(PlanType type, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, @Nullable PhysicalProperties physicalProperties,
            Statistics statistics, Plan... children) {
        super(type, groupExpression,
                logicalProperties == null ? Optional.empty() : Optional.of(logicalProperties),
                statistics, ImmutableList.copyOf(children));
        this.physicalProperties =
                physicalProperties == null ? PhysicalProperties.ANY : physicalProperties;
    }

    public PhysicalProperties getPhysicalProperties() {
        return physicalProperties;
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return this;
    }

    public <T extends AbstractPhysicalPlan> AbstractPhysicalPlan copyStatsAndGroupIdFrom(T from) {
        T newPlan = (T) withPhysicalPropertiesAndStats(
                from.getPhysicalProperties(), from.getStats());
        newPlan.setMutableState(MutableState.KEY_GROUP, from.getGroupIdAsString());
        return newPlan;
    }

    public List<org.apache.doris.nereids.trees.plans.physical.RuntimeFilter> getAppliedRuntimeFilters() {
        return appliedRuntimeFilters;
    }

    /**
     * Deep copy the physical plan tree and detach every node from the memo.
     *
     * <p>Each node of the plan produced by the optimizer keeps a reference to its
     * {@link GroupExpression}, which in turn references the whole memo. If such a plan is
     * held by a long-lived object (e.g. the query {@code Profile}), the memo and all its
     * groups and group expressions can never be garbage collected even after
     * {@link org.apache.doris.nereids.CascadesContext#releaseMemo()} is called.
     *
     * <p>This method returns a copy of the plan tree in which every node's group expression
     * is cleared, so the copy shares no object with the memo and the memo can be released as
     * early as possible. The copy preserves the plan structure, node ids, statistics, physical
     * properties, group ids (stored as mutable state, so that {@link Plan#treeString()} output
     * stays the same) and the runtime filters (re-created on the copied builder nodes and
     * target scans, so the printed plan keeps the RF information without referencing the
     * original plan nodes).
     */
    public static PhysicalPlan copyPlanDetachedFromMemo(PhysicalPlan plan) {
        if (plan == null) {
            return null;
        }
        Map<Plan, Plan> originalToClone = new IdentityHashMap<>();
        Map<RuntimeFilter, RuntimeFilter> rfCloneMap = new IdentityHashMap<>();
        PhysicalPlan detached = copyPlanDetachedFromMemo(plan, originalToClone, rfCloneMap);
        copyRuntimeFilters(originalToClone, rfCloneMap);
        return detached;
    }

    static PhysicalPlan copyPlanDetachedFromMemo(PhysicalPlan plan,
            Map<Plan, Plan> originalToClone, Map<RuntimeFilter, RuntimeFilter> rfCloneMap) {
        if (plan instanceof PhysicalLazyMaterialize) {
            PhysicalLazyMaterialize<?> lazy = (PhysicalLazyMaterialize<?>) plan;
            Plan clonedChild = lazy.children().isEmpty()
                    ? null : copyPlanDetachedFromMemo((PhysicalPlan) lazy.child(0), originalToClone, rfCloneMap);
            PhysicalLazyMaterialize<?> copy = lazy.copyDetachedFromMemo(clonedChild, originalToClone, rfCloneMap);
            originalToClone.put(plan, copy);
            return copy;
        }
        if (plan instanceof PhysicalStorageLayerAggregate) {
            PhysicalStorageLayerAggregate storageAgg = (PhysicalStorageLayerAggregate) plan;
            PhysicalCatalogRelation detachedRelation = (PhysicalCatalogRelation) copyPlanDetachedFromMemo(
                    storageAgg.getRelation(), originalToClone, rfCloneMap);
            PhysicalStorageLayerAggregate copy = new PhysicalStorageLayerAggregate(detachedRelation,
                    storageAgg.getAggOp(), storageAgg.getCountArgumentExprIds(), Optional.empty(),
                    storageAgg.getLogicalProperties(), storageAgg.getPhysicalProperties(), storageAgg.getStats());
            copyGroupId(storageAgg, copy);
            originalToClone.put(plan, copy);
            return copy;
        }
        List<Plan> clonedChildren = new ArrayList<>(plan.arity());
        boolean childChanged = false;
        for (Plan child : plan.children()) {
            if (child instanceof PhysicalPlan) {
                PhysicalPlan clonedChild = copyPlanDetachedFromMemo((PhysicalPlan) child, originalToClone, rfCloneMap);
                clonedChildren.add(clonedChild);
                childChanged |= clonedChild != child;
            } else {
                clonedChildren.add(child);
            }
        }
        // The whole subtree carries no memo reference, reuse the node directly.
        if (plan.getGroupExpression().isEmpty() && !childChanged) {
            return plan;
        }
        PhysicalProperties physicalProperties = plan.getPhysicalProperties();
        Statistics statistics = plan.getStats();
        Plan copied = plan.withChildren(clonedChildren);
        if (copied.getGroupExpression().isPresent()) {
            copied = copied.withGroupExpression(Optional.empty());
        }
        PhysicalPlan detached;
        if (copied.getStats() == null && statistics != null) {
            detached = ((PhysicalPlan) copied).withPhysicalPropertiesAndStats(physicalProperties, statistics);
        } else {
            detached = (PhysicalPlan) copied;
        }
        copyGroupId(plan, detached);
        plan.getMutableState(MutableState.KEY_PUSH_TOPN_TO_AGG)
                .ifPresent(value -> detached.setMutableState(MutableState.KEY_PUSH_TOPN_TO_AGG, value));
        originalToClone.put(plan, detached);
        return detached;
    }

    /**
     * Re-create the runtime filters on the copied nodes. Each runtime filter is cloned once and
     * re-pointed to the copied builder node and target scan, so the copy keeps the runtime filter
     * information of the printed plan without referencing the original plan nodes (and thus the memo).
     */
    private static void copyRuntimeFilters(Map<Plan, Plan> originalToClone,
            Map<RuntimeFilter, RuntimeFilter> rfCloneMap) {
        // Clone every runtime filter built on the copied nodes; the constructor attaches the clone
        // to the copied builder node.
        for (Map.Entry<Plan, Plan> entry : originalToClone.entrySet()) {
            Plan original = entry.getKey();
            Plan clone = entry.getValue();
            if (original instanceof AbstractPhysicalPlan && clone instanceof AbstractPhysicalPlan) {
                for (RuntimeFilter rf : ((AbstractPhysicalPlan) original).getRuntimeFilters()) {
                    // Only clone when both the builder node and the target scan are copied, otherwise
                    // the clone would reference (and mutate) the original plan nodes.
                    if (originalToClone.containsKey(rf.getBuilderNode())
                            && originalToClone.containsKey(rf.getTargetScan())) {
                        rfCloneMap.computeIfAbsent(rf, r -> cloneRuntimeFilter(r, originalToClone));
                    }
                }
            }
        }
        // Attach the cloned runtime filters to the copied target scans.
        for (Map.Entry<Plan, Plan> entry : originalToClone.entrySet()) {
            Plan original = entry.getKey();
            Plan clone = entry.getValue();
            if (original instanceof AbstractPhysicalPlan && clone instanceof AbstractPhysicalPlan) {
                for (RuntimeFilter rf : ((AbstractPhysicalPlan) original).getAppliedRuntimeFilters()) {
                    RuntimeFilter cloned = rfCloneMap.get(rf);
                    if (cloned != null) {
                        ((AbstractPhysicalPlan) clone).addAppliedRuntimeFilter(cloned);
                    }
                }
            }
        }
    }

    private static RuntimeFilter cloneRuntimeFilter(RuntimeFilter rf, Map<Plan, Plan> originalToClone) {
        RuntimeFilter cloned = new RuntimeFilter(rf.getId(), rf.getSrcExpr(), rf.getTargetSlot(),
                rf.getTargetExpression(), rf.getType(), rf.getExprOrder(),
                (AbstractPhysicalPlan) originalToClone.get(rf.getBuilderNode()),
                rf.getBuildSideNdv(), rf.isBloomFilterSizeCalculatedByNdv(), rf.gettMinMaxType(),
                (PhysicalRelation) originalToClone.get(rf.getTargetScan()));
        cloned.setNonBlocking(rf.isNonBlocking());
        return cloned;
    }

    private static void copyGroupId(Plan source, Plan target) {
        String groupId = source.getGroupIdAsString();
        if (!groupId.isEmpty()) {
            target.setMutableState(MutableState.KEY_GROUP, groupId);
        }
    }

    public void addAppliedRuntimeFilter(org.apache.doris.nereids.trees.plans.physical.RuntimeFilter filter) {
        appliedRuntimeFilters.add(filter);
    }

    public void addRuntimeFilter(RuntimeFilter filter) {
        runtimeFilters.add(filter);
    }

    public List<RuntimeFilter> getRuntimeFilters() {
        return runtimeFilters;
    }

    public void removeAppliedRuntimeFilter(org.apache.doris.nereids.trees.plans.physical.RuntimeFilter filter) {
        appliedRuntimeFilters.remove(filter);
    }
}

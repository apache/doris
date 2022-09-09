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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.RuntimeFilterGenerator.FilterSizeLimits;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.SessionVariable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

/**
 * runtime filter context used at post process and translation.
 */
public class RuntimeFilterContext {

    private final IdGenerator<RuntimeFilterId> generator = RuntimeFilterId.createGenerator();

    // exprId of target to runtime filter.
    private final Map<ExprId, List<RuntimeFilter>> targetExprIdToFilter = Maps.newHashMap();

    // olap scan node that contains target of a runtime filter.
    private final Map<RelationId, List<Slot>> targetOnOlapScanNodeMap = Maps.newHashMap();

    private final List<org.apache.doris.planner.RuntimeFilter> legacyFilters = Lists.newArrayList();

    // exprId to olap scan node slotRef because the slotRef will be changed when translating.
    private final Map<ExprId, SlotRef> exprIdToOlapScanNodeSlotRef = Maps.newHashMap();

    private final Map<ExprId, Slot> hashJoinExprToOlapScanSlot = Maps.newHashMap();

    // Alias's child to itself.
    private final Map<Slot, NamedExpression> aliasChildToSelf = Maps.newHashMap();

    private final Map<Slot, OlapScanNode> scanNodeOfLegacyRuntimeFilterTarget = Maps.newHashMap();

    private final SessionVariable sessionVariable;

    private final FilterSizeLimits limits;

    public RuntimeFilterContext(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
        this.limits = new FilterSizeLimits(sessionVariable);
    }

    public SessionVariable getSessionVariable() {
        return sessionVariable;
    }

    public FilterSizeLimits getLimits() {
        return limits;
    }

    public void setTargetExprIdToFilters(ExprId id, RuntimeFilter filter) {
        this.targetExprIdToFilter.computeIfAbsent(id, k -> Lists.newArrayList()).add(filter);
    }

    public List<RuntimeFilter> getFiltersByTargetExprId(ExprId id) {
        return targetExprIdToFilter.get(id);
    }

    public void removeFilters(ExprId id) {
        targetExprIdToFilter.remove(id);
    }

    public void setTargetsOnScanNode(RelationId id, Slot slot) {
        this.targetOnOlapScanNodeMap.computeIfAbsent(id, k -> Lists.newArrayList()).add(slot);
    }

    public List<Slot> getTargetsOfScanNode(RelationId id) {
        return targetOnOlapScanNodeMap.get(id);
    }

    public <K, V> void setKVInNormalMap(@NotNull Map<K, V> map, K key, V value) {
        map.put(key, value);
    }

    public Map<ExprId, Slot> getHashJoinExprToOlapScanSlot() {
        return hashJoinExprToOlapScanSlot;
    }

    public Map<ExprId, SlotRef> getExprIdToOlapScanNodeSlotRef() {
        return exprIdToOlapScanNodeSlotRef;
    }

    public Map<Slot, NamedExpression> getAliasChildToSelf() {
        return aliasChildToSelf;
    }

    public Map<Slot, OlapScanNode> getScanNodeOfLegacyRuntimeFilterTarget() {
        return scanNodeOfLegacyRuntimeFilterTarget;
    }

    public Map<ExprId, List<RuntimeFilter>> getTargetExprIdToFilter() {
        return targetExprIdToFilter;
    }

    public Map<RelationId, List<Slot>> getTargetOnOlapScanNodeMap() {
        return targetOnOlapScanNodeMap;
    }

    public List<org.apache.doris.planner.RuntimeFilter> getLegacyFilters() {
        return legacyFilters;
    }

    public void setLegacyFilter(org.apache.doris.planner.RuntimeFilter filter) {
        this.legacyFilters.add(filter);
    }

    public <K, V> boolean checkExistValue(@NotNull Map<K, V> map, K key) {
        return map.containsKey(key);
    }

    /**
     * get nereids runtime filters
     * @return nereids runtime filters
     */
    @VisibleForTesting
    public List<RuntimeFilter> getNereidsRuntimeFilter() {
        List<RuntimeFilter> filters = getTargetExprIdToFilter().values().stream()
                .reduce(Lists.newArrayList(), (l, r) -> {
                    l.addAll(r);
                    return l;
                });
        filters.sort((a, b) -> a.getId().compareTo(b.getId()));
        return filters;
    }

    public List<NamedExpression> getSlotListOfTheSameSlotAtOlapScanNode(Slot slot) {
        ImmutableList.Builder<NamedExpression> builder = ImmutableList.builder();
        NamedExpression expr = slot;
        while (aliasChildToSelf.containsKey(expr.toSlot())) {
            builder.add(expr);
            expr = aliasChildToSelf.get(expr.toSlot());
        }
        return builder.build();
    }
}

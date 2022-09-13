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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.SlotRef;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * translate runtime filter
 */
public class RuntimeFilterTranslator {

    private final RuntimeFilterContext context;

    public RuntimeFilterTranslator(RuntimeFilterContext context) {
        this.context = context;
    }

    /**
     * Translate nereids runtime filter on hash join node
     * translate the runtime filter whose target expression id is one of the output slot reference of the left child of
     * the physical hash join.
     * @param node hash join node
     * @param ctx plan translator context
     */
    public void translateRuntimeFilter(Slot slot, HashJoinNode node, PlanTranslatorContext ctx) {
        ExprId id = slot.getExprId();
        context.getLegacyFilters().addAll(context.getFiltersByTargetExprId(id).stream()
                .filter(RuntimeFilter::isUninitialized)
                .map(filter -> createLegacyRuntimeFilter(filter, node, ctx))
                .collect(Collectors.toList()));
    }

    public List<Slot> getTargetOnScanNode(RelationId id) {
        return context.getTargetOnOlapScanNodeMap().getOrDefault(id, Collections.emptyList());
    }

    public Slot getHashJoinNodeExistRuntimeFilter(Slot slot) {
        return context.getHashJoinExprToOlapScanSlot().get(slot.getExprId());
    }

    /**
     * translate runtime filter target.
     * @param node olap scan node
     * @param ctx plan translator context
     */
    public void translateRuntimeFilterTarget(Slot slot, OlapScanNode node, PlanTranslatorContext ctx) {
        context.setKVInNormalMap(context.getExprIdToOlapScanNodeSlotRef(),
                slot.getExprId(), ctx.findSlotRef(slot.getExprId()));
        context.setKVInNormalMap(context.getScanNodeOfLegacyRuntimeFilterTarget(), slot, node);
    }

    private org.apache.doris.planner.RuntimeFilter createLegacyRuntimeFilter(RuntimeFilter filter,
            HashJoinNode node, PlanTranslatorContext ctx) {
        SlotRef src = ctx.findSlotRef(filter.getSrcExpr().getExprId());
        SlotRef target = context.getExprIdToOlapScanNodeSlotRef().get(filter.getTargetExpr().getExprId());
        org.apache.doris.planner.RuntimeFilter origFilter
                = org.apache.doris.planner.RuntimeFilter.fromNereidsRuntimeFilter(
                filter.getId(), node, src, filter.getExprOrder(), target,
                ImmutableMap.of(target.getDesc().getParent().getId(), ImmutableList.of(target.getSlotId())),
                filter.getType(), context.getLimits());
        origFilter.setIsBroadcast(node.getDistributionMode() == DistributionMode.BROADCAST);
        filter.setFinalized();
        OlapScanNode scanNode = context.getScanNodeOfLegacyRuntimeFilterTarget().get(filter.getTargetExpr());
        origFilter.addTarget(new RuntimeFilterTarget(
                scanNode,
                target,
                true,
                scanNode.getFragmentId().equals(node.getFragmentId())));
        return finalize(origFilter);
    }

    private org.apache.doris.planner.RuntimeFilter finalize(org.apache.doris.planner.RuntimeFilter origFilter) {
        origFilter.markFinalized();
        origFilter.assignToPlanNodes();
        origFilter.extractTargetsPosition();
        return origFilter;
    }
}

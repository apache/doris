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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.List;

/**
 * translate runtime filter
 */
public class RuntimeFilterTranslator {

    private final RuntimeFilterContext context;

    public RuntimeFilterTranslator(RuntimeFilterContext context) {
        this.context = context;
        context.generatePhysicalHashJoinToRuntimeFilter();
    }

    public List<RuntimeFilter> getRuntimeFilterOfHashJoinNode(PhysicalHashJoin join) {
        return context.getRuntimeFilterOnHashJoinNode(join);
    }

    public List<Slot> getTargetOnScanNode(RelationId id) {
        return context.getTargetOnOlapScanNodeMap().getOrDefault(id, Collections.emptyList());
    }

    /**
     * translate runtime filter target.
     * @param node olap scan node
     * @param ctx plan translator context
     */
    public void translateRuntimeFilterTarget(Slot slot, OlapScanNode node, PlanTranslatorContext ctx) {
        context.getExprIdToOlapScanNodeSlotRef().put(slot.getExprId(), ctx.findSlotRef(slot.getExprId()));
        context.getScanNodeOfLegacyRuntimeFilterTarget().put(slot, node);
    }

    /**
     * generate legacy runtime filter
     * @param filter nereids runtime filter
     * @param node hash join node
     * @param ctx plan translator context
     */
    public void createLegacyRuntimeFilter(RuntimeFilter filter, HashJoinNode node, PlanTranslatorContext ctx) {
        SlotRef src = ctx.findSlotRef(filter.getSrcExpr().getExprId());
        SlotRef target = context.getExprIdToOlapScanNodeSlotRef().get(filter.getTargetExpr().getExprId());
        if (target == null) {
            context.setTargetNullCount();
            return;
        }
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
        context.getLegacyFilters().add(finalize(origFilter));
    }

    private org.apache.doris.planner.RuntimeFilter finalize(org.apache.doris.planner.RuntimeFilter origFilter) {
        origFilter.markFinalized();
        origFilter.assignToPlanNodes();
        origFilter.extractTargetsPosition();
        return origFilter;
    }
}

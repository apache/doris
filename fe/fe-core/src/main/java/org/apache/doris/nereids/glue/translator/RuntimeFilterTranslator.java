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

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.CTEScanNode;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.JoinNodeBase;
import org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * translate runtime filter
 */
public class RuntimeFilterTranslator {

    private final RuntimeFilterContext context;

    public RuntimeFilterTranslator(RuntimeFilterContext context) {
        this.context = context;
    }

    public RuntimeFilterContext getContext() {
        return context;
    }

    /**
     * translate runtime filter target.
     * @param node olap scan node
     * @param ctx plan translator context
     */
    public void translateRuntimeFilterTarget(Slot slot, ScanNode node, PlanTranslatorContext ctx) {
        context.getExprIdToOlapScanNodeSlotRef().put(slot.getExprId(), ctx.findSlotRef(slot.getExprId()));
        context.getScanNodeOfLegacyRuntimeFilterTarget().put(slot, node);
    }

    private class RuntimeFilterExpressionTranslator extends ExpressionTranslator {
        SlotRef targetSlotRef;

        RuntimeFilterExpressionTranslator(SlotRef targetSlotRef) {
            this.targetSlotRef = targetSlotRef;
        }

        @Override
        public Expr visitSlotReference(SlotReference slotReference, PlanTranslatorContext context) {
            return targetSlotRef;
        }
    }

    /**
     * generate legacy runtime filter
     * @param filter nereids runtime filter
     * @param node hash join node
     * @param ctx plan translator context
     */
    public void createLegacyRuntimeFilter(RuntimeFilter filter, JoinNodeBase node, PlanTranslatorContext ctx) {
        if (ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable()
                .getIgnoredRuntimeFilterIds().contains(filter.getId().asInt())) {
            return;
        }
        Expr src = ExpressionTranslator.translate(filter.getSrcExpr(), ctx);
        List<Expr> targetExprList = new ArrayList<>();
        List<Map<TupleId, List<SlotId>>> targetTupleIdMapList = new ArrayList<>();
        List<ScanNode> scanNodeList = new ArrayList<>();
        boolean hasInvalidTarget = false;
        for (int i = 0; i < filter.getTargetExpressions().size(); i++) {
            Slot curTargetSlot = filter.getTargetSlots().get(i);
            Expression curTargetExpression = filter.getTargetExpressions().get(i);
            SlotRef targetSlotRef = context.getExprIdToOlapScanNodeSlotRef().get(curTargetSlot.getExprId());
            if (targetSlotRef == null) {
                context.setTargetNullCount();
                hasInvalidTarget = true;
                break;
            }
            ScanNode scanNode = context.getScanNodeOfLegacyRuntimeFilterTarget().get(curTargetSlot);
            Expr targetExpr;
            if (curTargetSlot.equals(curTargetExpression)) {
                targetExpr = targetSlotRef;
            } else {
                // map nereids target slot to original planner slot
                Preconditions.checkArgument(curTargetExpression.getInputSlots().size() == 1,
                        "target expression is invalid, input slot num > 1; filter :" + filter);
                Slot slotInTargetExpression = curTargetExpression.getInputSlots().iterator().next();
                Preconditions.checkArgument(slotInTargetExpression.equals(curTargetSlot)
                        || curTargetSlot.equals(context.getAliasTransferMap().get(slotInTargetExpression).second));
                RuntimeFilterExpressionTranslator translator = new RuntimeFilterExpressionTranslator(targetSlotRef);
                targetExpr = curTargetExpression.accept(translator, ctx);
            }

            // adjust data type
            if (!src.getType().equals(targetExpr.getType()) && filter.getType() != TRuntimeFilterType.BITMAP) {
                targetExpr = new CastExpr(src.getType(), targetExpr);
            }
            SlotRef targetSlot = targetSlotRef.getSrcSlotRef();
            TupleId targetTupleId = targetSlot.getDesc().getParent().getId();
            SlotId targetSlotId = targetSlot.getSlotId();
            scanNodeList.add(scanNode);
            targetExprList.add(targetExpr);
            targetTupleIdMapList.add(ImmutableMap.of(targetTupleId, ImmutableList.of(targetSlotId)));
        }
        if (!hasInvalidTarget) {
            org.apache.doris.planner.RuntimeFilter origFilter
                    = org.apache.doris.planner.RuntimeFilter.fromNereidsRuntimeFilter(
                    filter, node, src, targetExprList,
                    targetTupleIdMapList, context.getLimits());
            if (node instanceof HashJoinNode) {
                origFilter.setIsBroadcast(((HashJoinNode) node).getDistributionMode() == DistributionMode.BROADCAST);
            } else {
                // nest loop join
                origFilter.setIsBroadcast(true);
            }
            boolean isLocalTarget = scanNodeList.stream().allMatch(e ->
                    !(e instanceof CTEScanNode) && e.getFragmentId().equals(node.getFragmentId()));
            for (int i = 0; i < targetExprList.size(); i++) {
                ScanNode scanNode = scanNodeList.get(i);
                Expr targetExpr = targetExprList.get(i);
                origFilter.addTarget(new RuntimeFilterTarget(
                        scanNode, targetExpr, true, isLocalTarget));
            }
            origFilter.setBitmapFilterNotIn(filter.isBitmapFilterNotIn());
            origFilter.setBloomFilterSizeCalculatedByNdv(filter.isBloomFilterSizeCalculatedByNdv());
            org.apache.doris.planner.RuntimeFilter finalizedFilter = finalize(origFilter);
            scanNodeList.stream().filter(e -> e.getStatisticalType() == StatisticalType.CTE_SCAN_NODE)
                                 .forEach(f -> {
                                     DataStreamSink sink = context.getPlanNodeIdToCTEDataSinkMap().get(f.getId());
                                     if (sink != null) {
                                         sink.addRuntimeFilter(finalizedFilter);
                                     }
                                 });
            context.getLegacyFilters().add(finalizedFilter);
        }
    }

    private org.apache.doris.planner.RuntimeFilter finalize(org.apache.doris.planner.RuntimeFilter origFilter) {
        origFilter.markFinalized();
        origFilter.assignToPlanNodes();
        origFilter.extractTargetsPosition();
        return origFilter;
    }
}

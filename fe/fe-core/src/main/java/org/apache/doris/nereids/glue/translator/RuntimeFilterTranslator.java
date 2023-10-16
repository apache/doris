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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.CTEScanNode;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.JoinNodeBase;
import org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * translate runtime filter
 */
public class RuntimeFilterTranslator {

    private final RuntimeFilterContext context;

    public RuntimeFilterTranslator(RuntimeFilterContext context) {
        this.context = context;
        context.generatePhysicalHashJoinToRuntimeFilter();
    }

    public Set<RuntimeFilter> getRuntimeFilterOfHashJoinNode(AbstractPhysicalJoin join) {
        return context.getRuntimeFilterOnHashJoinNode(join);
    }

    public RuntimeFilterContext getContext() {
        return context;
    }

    public List<Slot> getTargetOnScanNode(RelationId id) {
        return context.getTargetOnOlapScanNodeMap().getOrDefault(id, Collections.emptyList());
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
        Map<ExprId, SlotRef> nereidsExprIdToSlotRef;

        RuntimeFilterExpressionTranslator(Map<ExprId, SlotRef> nereidsExprIdToSlotRef) {
            this.nereidsExprIdToSlotRef = nereidsExprIdToSlotRef;
        }

        @Override
        public Expr visitSlotReference(SlotReference slotReference, PlanTranslatorContext context) {
            slotReference = context.getRuntimeTranslator().get()
                    .context.getCorrespondingOlapSlotReference(slotReference);
            SlotRef slot = nereidsExprIdToSlotRef.get(slotReference.getExprId());
            if (slot == null) {
                throw new AnalysisException("cannot find SlotRef for " + slotReference);
            }
            return slot;
        }
    }

    /**
     * generate legacy runtime filter
     * @param filter nereids runtime filter
     * @param node hash join node
     * @param ctx plan translator context
     */
    public void createLegacyRuntimeFilter(RuntimeFilter filter, JoinNodeBase node, PlanTranslatorContext ctx) {
        Expr src = ExpressionTranslator.translate(filter.getSrcExpr(), ctx);
        List<Expr> targetExprList = new ArrayList<>();
        List<Map<TupleId, List<SlotId>>> targetTupleIdMapList = new ArrayList<>();
        List<ScanNode> scanNodeList = new ArrayList<>();
        boolean hasInvalidTarget = false;
        for (int i = 0; i < filter.getTargetExprs().size(); i++) {
            Slot curTargetExpr = filter.getTargetExprs().get(i);
            Expression curTargetExpression = filter.getTargetExpressions().get(i);
            Expr target = context.getExprIdToOlapScanNodeSlotRef().get(curTargetExpr.getExprId());
            if (target == null) {
                context.setTargetNullCount();
                hasInvalidTarget = true;
                break;
            }
            ScanNode scanNode = context.getScanNodeOfLegacyRuntimeFilterTarget().get(curTargetExpr);
            Expr targetExpr;
            if (filter.getType() == TRuntimeFilterType.BITMAP) {
                if (curTargetExpression.equals(curTargetExpr)) {
                    targetExpr = target;
                } else {
                    RuntimeFilterExpressionTranslator translator = new RuntimeFilterExpressionTranslator(
                            context.getExprIdToOlapScanNodeSlotRef());
                    targetExpr = curTargetExpression.accept(translator, ctx);
                }
            } else {
                targetExpr = target;
            }
            // adjust data type
            if (!src.getType().equals(target.getType()) && filter.getType() != TRuntimeFilterType.BITMAP) {
                targetExpr = new CastExpr(src.getType(), targetExpr);
            }
            SlotRef targetSlot = target.getSrcSlotRef();
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
                //bitmap rf requires isBroadCast=false, it always requires merge filter
                origFilter.setIsBroadcast(false);
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
        // Number of parallel instances are large for pipeline engine, so we prefer bloom filter.
        if (origFilter.hasRemoteTargets() && origFilter.getType() == TRuntimeFilterType.IN_OR_BLOOM
                && SessionVariable.enablePipelineEngine()) {
            origFilter.setType(TRuntimeFilterType.BLOOM);
        }
        return origFilter;
    }
}

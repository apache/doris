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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.JoinNodeBase;
import org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * translate runtime filter
 */
public class RuntimeFilterTranslator {

    private final RuntimeFilterContext context;

    public RuntimeFilterTranslator(RuntimeFilterContext context) {
        this.context = context;
        context.generatePhysicalHashJoinToRuntimeFilter();
    }

    public List<RuntimeFilter> getRuntimeFilterOfHashJoinNode(AbstractPhysicalJoin join) {
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
        Expr target = context.getExprIdToOlapScanNodeSlotRef().get(filter.getTargetExpr().getExprId());
        if (target == null) {
            context.setTargetNullCount();
            return;
        }
        Expr targetExpr = null;
        if (filter.getType() == TRuntimeFilterType.BITMAP) {
            if (filter.getTargetExpression().equals(filter.getTargetExpr())) {
                targetExpr = target;
            } else {
                RuntimeFilterExpressionTranslator translator = new RuntimeFilterExpressionTranslator(
                        context.getExprIdToOlapScanNodeSlotRef());
                try {
                    targetExpr = filter.getTargetExpression().accept(translator, ctx);
                    targetExpr.finalizeForNereids();
                } catch (org.apache.doris.common.AnalysisException e) {
                    throw new AnalysisException(
                            "Translate Nereids expression to stale expression failed. " + e.getMessage(), e);
                }

            }
        } else {
            targetExpr = target;
        }

        Expr src = ExpressionTranslator.translate(filter.getSrcExpr(), ctx);
        SlotRef targetSlot = target.getSrcSlotRef();
        TupleId targetTupleId = targetSlot.getDesc().getParent().getId();
        SlotId targetSlotId = targetSlot.getSlotId();
        // adjust data type
        if (!src.getType().equals(target.getType()) && filter.getType() != TRuntimeFilterType.BITMAP) {
            targetExpr = new CastExpr(src.getType(), targetExpr);
        }
        org.apache.doris.planner.RuntimeFilter origFilter
                = org.apache.doris.planner.RuntimeFilter.fromNereidsRuntimeFilter(
                filter.getId(), node, src, filter.getExprOrder(), targetExpr,
                ImmutableMap.of(targetTupleId, ImmutableList.of(targetSlotId)),
                filter.getType(), context.getLimits());
        if (node instanceof HashJoinNode) {
            origFilter.setIsBroadcast(((HashJoinNode) node).getDistributionMode() == DistributionMode.BROADCAST);
        } else {
            //bitmap rf requires isBroadCast=false, it always requires merge filter
            origFilter.setIsBroadcast(false);
        }
        ScanNode scanNode = context.getScanNodeOfLegacyRuntimeFilterTarget().get(filter.getTargetExpr());
        origFilter.addTarget(new RuntimeFilterTarget(
                scanNode,
                targetExpr,
                true,
                scanNode.getFragmentId().equals(node.getFragmentId())));
        origFilter.setBitmapFilterNotIn(filter.isBitmapFilterNotIn());
        context.getLegacyFilters().add(finalize(origFilter));
    }

    private org.apache.doris.planner.RuntimeFilter finalize(org.apache.doris.planner.RuntimeFilter origFilter) {
        origFilter.markFinalized();
        origFilter.assignToPlanNodes();
        origFilter.extractTargetsPosition();
        return origFilter;
    }
}

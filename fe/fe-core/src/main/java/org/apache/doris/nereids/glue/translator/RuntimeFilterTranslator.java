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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.planner.CTEScanNode;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.DistributionMode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * translate runtime filter
 */
public class RuntimeFilterTranslator {
    private static final Logger LOG = LogManager.getLogger(RuntimeFilterTranslator.class);

    private final RuntimeFilterContext context;

    private static final class RuntimeFilterGroupKey {
        private final Expression srcExpr;
        private final TRuntimeFilterType type;
        private final int exprOrder;
        private final TMinMaxRuntimeFilterType minMaxType;
        private final boolean bitmapFilterNotIn;
        private final boolean bloomFilterSizeCalculatedByNdv;
        private final long buildSideNdv;

        private RuntimeFilterGroupKey(RuntimeFilter filter) {
            this.srcExpr = filter.getSrcExpr();
            this.type = filter.getType();
            this.exprOrder = filter.getExprOrder();
            this.minMaxType = filter.gettMinMaxType();
            this.bitmapFilterNotIn = filter.isBitmapFilterNotIn();
            this.bloomFilterSizeCalculatedByNdv = filter.isBloomFilterSizeCalculatedByNdv();
            this.buildSideNdv = filter.getBuildSideNdv();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RuntimeFilterGroupKey)) {
                return false;
            }
            RuntimeFilterGroupKey that = (RuntimeFilterGroupKey) o;
            return exprOrder == that.exprOrder
                    && bitmapFilterNotIn == that.bitmapFilterNotIn
                    && bloomFilterSizeCalculatedByNdv == that.bloomFilterSizeCalculatedByNdv
                    && buildSideNdv == that.buildSideNdv
                    && Objects.equals(srcExpr, that.srcExpr)
                    && type == that.type
                    && minMaxType == that.minMaxType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(srcExpr, type, exprOrder, minMaxType,
                    bitmapFilterNotIn, bloomFilterSizeCalculatedByNdv, buildSideNdv);
        }
    }

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
     * Translate a batch of nereids runtime filters, grouping those with identical
     * translation semantics into a single legacy runtime filter with multiple targets.
     * This supports V2-style RF generation where each target produces a separate
     * nereids RF object.
     */
    public void createLegacyRuntimeFilters(List<RuntimeFilter> filters, PlanNode node, PlanTranslatorContext ctx) {
        Map<RuntimeFilterGroupKey, List<RuntimeFilter>> groupedFilters = new LinkedHashMap<>();
        for (RuntimeFilter filter : filters) {
            if (ConnectContext.get() != null
                    && ConnectContext.get().getSessionVariable()
                    .getIgnoredRuntimeFilterIds().contains(filter.getId().asInt())) {
                continue;
            }
            groupedFilters.computeIfAbsent(new RuntimeFilterGroupKey(filter), k -> new ArrayList<>()).add(filter);
        }
        for (List<RuntimeFilter> group : groupedFilters.values()) {
            if (group.size() == 1) {
                createLegacyRuntimeFilter(group.get(0), node, ctx);
            } else {
                createLegacyRuntimeFilterFromGroup(group, node, ctx);
            }
        }
    }

    private void createLegacyRuntimeFilterFromGroup(List<RuntimeFilter> group,
            PlanNode node, PlanTranslatorContext ctx) {
        try {
            RuntimeFilter head = group.get(0);
            Expr src = ExpressionTranslator.translate(head.getSrcExpr(), ctx);
            List<Expr> targetExprList = new ArrayList<>();
            List<Map<TupleId, List<SlotId>>> targetTupleIdMapList = new ArrayList<>();
            List<ScanNode> scanNodeList = new ArrayList<>();
            boolean hasInvalidTarget = false;
            for (RuntimeFilter filter : group) {
                Slot curTargetSlot = filter.getTargetSlot();
                Expression curTargetExpression = filter.getTargetExpression();
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
                    Preconditions.checkArgument(curTargetExpression.getInputSlots().size() == 1,
                            "target expression is invalid, input slot num > 1; filter :" + filter);
                    Slot slotInTargetExpression = curTargetExpression.getInputSlots().iterator().next();
                    Preconditions.checkArgument(slotInTargetExpression.equals(curTargetSlot),
                            "target expression input slot should equal target slot after rewrite; filter:"
                                    + filter);
                    RuntimeFilterExpressionTranslator translator =
                            new RuntimeFilterExpressionTranslator(targetSlotRef);
                    targetExpr = curTargetExpression.accept(translator, ctx);
                }
                if (!src.getType().equals(targetExpr.getType()) && head.getType() != TRuntimeFilterType.BITMAP) {
                    targetExpr = new CastExpr(src.getType(), targetExpr,
                            Cast.castNullable(src.isNullable(),
                                    DataType.fromCatalogType(src.getType()),
                                    DataType.fromCatalogType(targetExpr.getType())));
                }
                TupleId targetTupleId = targetSlotRef.getDesc().getParentId();
                SlotId targetSlotId = targetSlotRef.getSlotId();
                scanNodeList.add(scanNode);
                targetExprList.add(targetExpr);
                targetTupleIdMapList.add(ImmutableMap.of(targetTupleId, ImmutableList.of(targetSlotId)));
            }
            if (!hasInvalidTarget) {
                org.apache.doris.planner.RuntimeFilter origFilter
                        = org.apache.doris.planner.RuntimeFilter.fromNereidsRuntimeFilter(
                        head, node, src, targetExprList,
                        targetTupleIdMapList, context.getLimits());
                if (node instanceof HashJoinNode) {
                    origFilter.setIsBroadcast(
                            ((HashJoinNode) node).getDistributionMode() == DistributionMode.BROADCAST);
                    origFilter.setSingleEq(((HashJoinNode) node).getEqJoinConjuncts().size());
                } else if (node instanceof SetOperationNode) {
                    origFilter.setIsBroadcast(false);
                } else {
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
                origFilter.setBitmapFilterNotIn(head.isBitmapFilterNotIn());
                origFilter.setBloomFilterSizeCalculatedByNdv(head.isBloomFilterSizeCalculatedByNdv());
                org.apache.doris.planner.RuntimeFilter finalizedFilter = finalize(origFilter);
                scanNodeList.stream().filter(CTEScanNode.class::isInstance)
                        .forEach(f -> {
                            DataStreamSink sink = context.getPlanNodeIdToCTEDataSinkMap().get(f.getId());
                            if (sink != null) {
                                sink.addRuntimeFilter(finalizedFilter);
                            }
                        });
                context.getLegacyFilters().add(finalizedFilter);
            }
        } catch (Exception e) {
            LOG.info("failed to translate runtime filter group: " + e.getMessage());
            if (SessionVariable.isFeDebug()) {
                throw e;
            }
        }
    }

    /**
     * generate legacy runtime filter
     * @param filter nereids runtime filter
     * @param node hash join node
     * @param ctx plan translator context
     */
    public void createLegacyRuntimeFilter(RuntimeFilter filter, PlanNode node, PlanTranslatorContext ctx) {
        if (ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable()
                .getIgnoredRuntimeFilterIds().contains(filter.getId().asInt())) {
            return;
        }
        try {
            Expr src = ExpressionTranslator.translate(filter.getSrcExpr(), ctx);
            List<Expr> targetExprList = new ArrayList<>();
            List<Map<TupleId, List<SlotId>>> targetTupleIdMapList = new ArrayList<>();
            List<ScanNode> scanNodeList = new ArrayList<>();
            boolean hasInvalidTarget = false;
            Slot curTargetSlot = filter.getTargetSlot();
            Expression curTargetExpression = filter.getTargetExpression();
            SlotRef targetSlotRef = context.getExprIdToOlapScanNodeSlotRef().get(curTargetSlot.getExprId());
            if (targetSlotRef == null) {
                context.setTargetNullCount();
                hasInvalidTarget = true;
            } else {
                ScanNode scanNode = context.getScanNodeOfLegacyRuntimeFilterTarget().get(curTargetSlot);
                Expr targetExpr;
                if (curTargetSlot.equals(curTargetExpression)) {
                    targetExpr = targetSlotRef;
                } else {
                    // map nereids target slot to original planner slot
                    Preconditions.checkArgument(curTargetExpression.getInputSlots().size() == 1,
                            "target expression is invalid, input slot num > 1; filter :" + filter);
                    Slot slotInTargetExpression = curTargetExpression.getInputSlots().iterator().next();
                    Preconditions.checkArgument(slotInTargetExpression.equals(curTargetSlot),
                            "target expression input slot should equal target slot after rewrite; filter:" + filter);
                    RuntimeFilterExpressionTranslator translator = new RuntimeFilterExpressionTranslator(targetSlotRef);
                    targetExpr = curTargetExpression.accept(translator, ctx);
                }

                // adjust data type
                if (!src.getType().equals(targetExpr.getType()) && filter.getType() != TRuntimeFilterType.BITMAP) {
                    targetExpr = new CastExpr(src.getType(), targetExpr, Cast.castNullable(src.isNullable(),
                            DataType.fromCatalogType(src.getType()), DataType.fromCatalogType(targetExpr.getType())));
                }
                TupleId targetTupleId = targetSlotRef.getDesc().getParentId();
                SlotId targetSlotId = targetSlotRef.getSlotId();
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
                    origFilter.setIsBroadcast(
                            ((HashJoinNode) node).getDistributionMode() == DistributionMode.BROADCAST);
                    origFilter.setSingleEq(((HashJoinNode) node).getEqJoinConjuncts().size());
                } else if (node instanceof SetOperationNode) {
                    origFilter.setIsBroadcast(false);
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
                scanNodeList.stream().filter(CTEScanNode.class::isInstance)
                        .forEach(f -> {
                            DataStreamSink sink = context.getPlanNodeIdToCTEDataSinkMap().get(f.getId());
                            if (sink != null) {
                                sink.addRuntimeFilter(finalizedFilter);
                            }
                        });
                context.getLegacyFilters().add(finalizedFilter);
            }
        } catch (Exception e) {
            LOG.info("failed to translate runtime filter: " + e.getMessage());
            // throw exception in debug mode
            if (SessionVariable.isFeDebug()) {
                throw e;
            }
        }
    }

    private org.apache.doris.planner.RuntimeFilter finalize(org.apache.doris.planner.RuntimeFilter origFilter) {
        origFilter.markFinalized();
        origFilter.assignToPlanNodes();
        origFilter.extractTargetsPosition();
        return origFilter;
    }
}

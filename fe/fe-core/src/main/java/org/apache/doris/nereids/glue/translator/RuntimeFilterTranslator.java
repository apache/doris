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
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.planner.CTEScanNode;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.DistributionMode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.MultiCastPlanFragment;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.RecursiveCteNode;
import org.apache.doris.planner.RepeatNode;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
        private final boolean bloomFilterSizeCalculatedByNdv;
        private final long buildSideNdv;

        private RuntimeFilterGroupKey(RuntimeFilter filter) {
            this.srcExpr = filter.getSrcExpr();
            this.type = filter.getType();
            this.exprOrder = filter.getExprOrder();
            this.minMaxType = filter.gettMinMaxType();
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
                    && bloomFilterSizeCalculatedByNdv == that.bloomFilterSizeCalculatedByNdv
                    && buildSideNdv == that.buildSideNdv
                    && Objects.equals(srcExpr, that.srcExpr)
                    && type == that.type
                    && minMaxType == that.minMaxType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(srcExpr, type, exprOrder, minMaxType,
                    bloomFilterSizeCalculatedByNdv, buildSideNdv);
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
            // Kept for the exchange targets added below, which need the Nereids target slot and
            // expression to rebuild the filter against an exchange's output tuple.
            List<Slot> targetSlotList = new ArrayList<>();
            List<Expression> targetExpressionList = new ArrayList<>();
            Map<Integer, String> scanNodeTargetExprSql = new LinkedHashMap<>();
            Map<Integer, Boolean> scanNodeHasDifferentTargets = new LinkedHashMap<>();
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
                boolean isIdentityTarget = curTargetSlot.equals(curTargetExpression);
                if (isIdentityTarget) {
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
                targetExpr = castTargetToSourceTypeIfNeeded(src, targetExpr);
                TupleId targetTupleId = targetSlotRef.getDesc().getParentId();
                SlotId targetSlotId = targetSlotRef.getSlotId();
                scanNodeList.add(scanNode);
                targetSlotList.add(curTargetSlot);
                targetExpressionList.add(curTargetExpression);
                targetExprList.add(targetExpr);
                targetTupleIdMapList.add(ImmutableMap.of(targetTupleId, ImmutableList.of(targetSlotId)));
                int scanNodeId = scanNode.getId().asInt();
                String targetExprSql = targetExpr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE);
                String existingTargetExprSql = scanNodeTargetExprSql.putIfAbsent(scanNodeId, targetExprSql);
                if (existingTargetExprSql != null && !existingTargetExprSql.equals(targetExprSql)) {
                    scanNodeHasDifferentTargets.put(scanNodeId, true);
                }
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
                Set<PlanNodeId> addedExchangeNodes = new HashSet<>();
                for (int i = 0; i < targetExprList.size(); i++) {
                    ScanNode scanNode = scanNodeList.get(i);
                    Expr targetExpr = targetExprList.get(i);
                    origFilter.addTarget(new RuntimeFilterTarget(
                            scanNode, targetExpr, true, isLocalTarget));
                    addExchangeTargets(origFilter, node, scanNode, targetSlotList.get(i),
                            targetExpressionList.get(i), src, isLocalTarget, ctx, addedExchangeNodes);
                    updateConditionCacheEligibility(scanNode, group.get(i));
                    // TRuntimeFilterDesc keys target expressions and pruning metadata by scan node ID.
                    // If one grouped RF has different targets on the same scan, BE cannot match
                    // metadata back to a specific target expression, so skip pruning metadata.
                    if (scanNodeHasDifferentTargets.getOrDefault(scanNode.getId().asInt(), false)) {
                        continue;
                    }
                    setPruningMetadata(origFilter, scanNode, group.get(i));
                }
                origFilter.setBloomFilterSizeCalculatedByNdv(head.isBloomFilterSizeCalculatedByNdv());
                // The merged legacy filter applies on every target of the group, so it may not wait when any
                // of the filters it merges must not: the wait would put back the edge that the non-blocking
                // filter of the group removes, and a target of that filter can then wait for a builder which
                // itself waits for the target.
                setWaitTimeMs(origFilter, group.stream().anyMatch(RuntimeFilter::isNonBlocking));
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
                boolean isIdentityTarget = curTargetSlot.equals(curTargetExpression);
                if (isIdentityTarget) {
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
                targetExpr = castTargetToSourceTypeIfNeeded(src, targetExpr);
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
                Set<PlanNodeId> addedExchangeNodes = new HashSet<>();
                for (int i = 0; i < targetExprList.size(); i++) {
                    ScanNode scanNode = scanNodeList.get(i);
                    Expr targetExpr = targetExprList.get(i);
                    origFilter.addTarget(new RuntimeFilterTarget(
                            scanNode, targetExpr, true, isLocalTarget));
                    addExchangeTargets(origFilter, node, scanNode, curTargetSlot, curTargetExpression,
                            src, isLocalTarget, ctx, addedExchangeNodes);
                    updateConditionCacheEligibility(scanNode, filter);
                    setPruningMetadata(origFilter, scanNode, filter);
                }
                origFilter.setBloomFilterSizeCalculatedByNdv(filter.isBloomFilterSizeCalculatedByNdv());
                setWaitTimeMs(origFilter, filter.isNonBlocking());
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

    private void updateConditionCacheEligibility(ScanNode scanNode, RuntimeFilter filter) {
        Expression target = filter.getTargetExpression();
        DataType sourceType = filter.getSrcExpr().getDataType();
        // Include the implicit cast inserted by castTargetToSourceTypeIfNeeded.
        if (!target.getDataType().equals(sourceType)) {
            target = new Cast(target, sourceType);
        }
        // RF probes become scan predicates even when no PhysicalFilter is attached to the scan.
        if (ExpressionUtils.containsNonCacheableExpression(ImmutableList.of(target))) {
            scanNode.setEnableConditionCache(false);
        }
    }

    private void setPruningMetadata(org.apache.doris.planner.RuntimeFilter runtimeFilter,
            ScanNode scanNode, RuntimeFilter nereidsFilter) {
        if (nereidsFilter.canPrunePartitions()) {
            Preconditions.checkState(scanNode instanceof OlapScanNode,
                    "partition-pruning runtime filter target must be an OlapScanNode");
            runtimeFilter.markTargetCanPrunePartitions(scanNode.getId());
            ((OlapScanNode) scanNode).snapshotPartitionBoundariesForRuntimeFilter();
        }
        runtimeFilter.setTargetPartitionMonotonicity(
                scanNode.getId(), nereidsFilter.getPartitionMonotonicity());
        if (nereidsFilter.canPruneBuckets()) {
            runtimeFilter.markTargetCanPruneBuckets(scanNode.getId());
        }
    }

    private void setWaitTimeMs(org.apache.doris.planner.RuntimeFilter filter,
            boolean isNonBlocking) {
        if (isNonBlocking) {
            filter.setWaitTimeMs(0);
        } else if (ConnectContext.get() != null) {
            SessionVariable sessionVar = ConnectContext.get().getSessionVariable();
            // The wait budget is short (runtime_filter_wait_time_ms, 20ms by default) and starts
            // only after the pipeline's join build side is ready, so a local target does not need
            // a different (infinite) budget any more: the filter is published when the build sink
            // closes. See PipelineTask::_wait_to_start() in BE.
            filter.setWaitTimeMs(sessionVar.runtimeFilterWaitInfinitely
                    ? sessionVar.getQueryTimeoutS() * 1000
                    : sessionVar.getRuntimeFilterWaitTimeMs());
        } else {
            filter.setWaitTimeMs(20);
        }
    }

    /**
     * Apply the filter on the exchange nodes between the target scan and the builder join as well.
     * The target scan no longer blocks for long waiting for the filter, so it can already produce
     * blocks before the filter arrives. Filtering at the exchanges on the path lets those blocks be
     * pruned before they are shuffled and probed.
     *
     * <p>Only exchanges whose output still carries the target expression and whose input path is a
     * single chain back to the scan are used. An exchange that also merges other branches (a join
     * or a set operation below it) carries rows that do not belong to the target scan, and the
     * target expression is not meaningful for them, so filtering there could drop valid rows.
     */
    private void addExchangeTargets(org.apache.doris.planner.RuntimeFilter filter, PlanNode builderNode,
            ScanNode scanNode, Slot targetSlot, Expression targetExpression, Expr src,
            boolean isLocalTarget, PlanTranslatorContext ctx, Set<PlanNodeId> addedExchangeNodes) {
        if (scanNode instanceof CTEScanNode) {
            // A CTE target is applied at the CTE producer's sink; the consumer path is a multicast
            // DAG and is not handled here.
            return;
        }
        List<PlanNode> path = new ArrayList<>();
        if (!findPlanNodePath(builderNode, scanNode, new HashSet<>(), path)) {
            return;
        }
        for (PlanNode pathNode : path) {
            if (pathNode.getFragment() instanceof MultiCastPlanFragment) {
                // A CTE producer feeds several consumers; the target expression is not a property
                // of every row that crosses an exchange in that DAG.
                return;
            }
        }
        boolean isIdentityTarget = targetSlot.equals(targetExpression);
        for (int i = 0; i < path.size(); i++) {
            PlanNode node = path.get(i);
            if (!(node instanceof ExchangeNode)) {
                continue;
            }
            // An exchange that also merges other branches (a join or a set operation below it)
            // carries rows that do not belong to the target scan. The target expression is not
            // meaningful for those rows, so filtering there could drop valid rows.
            if (!isSafeSingleInputChain(path, i + 1)) {
                continue;
            }
            if (addedExchangeNodes.contains(node.getId())) {
                continue;
            }
            SlotRef exchangeSlotRef = findSlotRefInOutput(ctx, node, targetSlot.getExprId());
            if (exchangeSlotRef == null) {
                // The target slot is not part of this exchange's output tuple, e.g. an
                // intermediate operator consumed it or renamed it.
                continue;
            }
            addedExchangeNodes.add(node.getId());
            Expr exchangeTargetExpr;
            if (isIdentityTarget) {
                exchangeTargetExpr = exchangeSlotRef;
            } else {
                exchangeTargetExpr = targetExpression.accept(
                        new RuntimeFilterExpressionTranslator(exchangeSlotRef), ctx);
            }
            exchangeTargetExpr = castTargetToSourceTypeIfNeeded(src, exchangeTargetExpr);
            // Locality is a property of the whole filter (all scan targets local or not), and BE
            // requires has_local_targets != has_remote_targets. An exchange on the path of a
            // remote target must therefore stay remote.
            filter.addTarget(new RuntimeFilterTarget(node, exchangeTargetExpr, true, isLocalTarget));
        }
    }

    private static SlotRef findSlotRefInOutput(PlanTranslatorContext ctx, PlanNode node,
            ExprId exprId) {
        for (TupleId tupleId : node.getOutputTupleIds()) {
            TupleDescriptor tuple = ctx.getTupleDesc(tupleId);
            for (SlotDescriptor slot : tuple.getSlots()) {
                if (exprId.equals(ctx.findExprId(slot.getId()))) {
                    return new SlotRef(slot);
                }
            }
        }
        return null;
    }

    /**
     * Collects the plan nodes on the path from {@code current} down to {@code target}. Exchange
     * nodes keep their upstream fragment root as child, so the path crosses fragment boundaries.
     */
    private static boolean findPlanNodePath(PlanNode current, PlanNode target, Set<PlanNode> visited,
            List<PlanNode> path) {
        if (current == target) {
            return true;
        }
        if (current == null || !visited.add(current) || current.getChildren() == null) {
            return false;
        }
        for (PlanNode child : current.getChildren()) {
            path.add(child);
            if (findPlanNodePath(child, target, visited, path)) {
                return true;
            }
            path.remove(path.size() - 1);
        }
        return false;
    }

    /**
     * True when every node between the exchange and the scan produces exactly one output row per
     * input row and preserves the input columns. Joins, set operations and table functions have
     * several children and are rejected by the child count; grouping sets (repeat) and recursive
     * CTEs can produce rows whose target slot is not a value of the scan, so they are rejected
     * explicitly even though they have one child.
     */
    private static boolean isSafeSingleInputChain(List<PlanNode> path, int startIndex) {
        for (int i = startIndex; i < path.size(); i++) {
            PlanNode node = path.get(i);
            if (node instanceof RepeatNode || node instanceof RecursiveCteNode) {
                return false;
            }
            if (node.getChildren() != null && node.getChildren().size() > 1) {
                return false;
            }
        }
        return true;
    }

    private Expr castTargetToSourceTypeIfNeeded(Expr src, Expr targetExpr) {
        // The cast is: CAST(targetExpr AS src.getType()), so the child of the cast
        // is targetExpr and the destination type is src.getType().
        // castNullable(srcNullable, srcType, targetType) expects: child nullable, child type, dest type.
        if (!src.getType().equals(targetExpr.getType())) {
            return new CastExpr(src.getType(), targetExpr, Cast.castNullable(targetExpr.isNullable(),
                    DataType.fromCatalogType(targetExpr.getType()), DataType.fromCatalogType(src.getType())));
        }
        return targetExpr;
    }
}

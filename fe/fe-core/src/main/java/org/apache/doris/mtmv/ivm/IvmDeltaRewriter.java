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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.common.Pair;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.ivm.agg.IvmAggMeta;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Entry point for IVM delta rewriting.
 *
 * <h3>Multi-bundle generation</h3>
 * <p>The rewriter generates one bundle per OlapScan whose stream has pending data.
 * OlapScans belonging to excluded trigger tables are skipped entirely (assumed unchanged).
 * For the i-th delta scan Si:
 * <ul>
 *   <li>Si → {@link #replaceWithDelta} (LogicalOlapTableStreamScan as delta source)</li>
 *   <li>Sj where j &lt; i → post-refresh snapshot ({@code scan.withPostSnapshot()})</li>
 *   <li>Sj where j &gt; i → pre-refresh snapshot ({@code scan.withPreSnapshot(stream)})</li>
 * </ul>
 *
 * <p>Both the collection pass and the replacement pass use
 * {@link Plan#rewriteDownShortCircuit} to guarantee identical traversal order,
 * so the incrementing scanIndex correctly correlates each visit to the collected scan list.
 */
public class IvmDeltaRewriter {

    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;
    private final IvmAggDeltaHandler aggHandler = new IvmAggDeltaHandler();

    /**
     * Generates the rewritten sink child for the analyzer rule path.
     * The input sink child may already contain bind-sink adapter projects.
     */
    public Plan generateIncrementalRefreshPlan(Plan sinkChild, IvmRewriteResult rewriteResult,
            IvmRewriteContext rewriteContext, ConnectContext connectContext) {
        IvmRefreshContext refreshContext = new IvmRefreshContext(
                rewriteContext.getMtmv(), connectContext, rewriteResult);
        Set<TableNameInfo> excluded = rewriteContext.getMtmv().getExcludedTriggerTables();
        return generateMergedDeltaPlan(sinkChild, refreshContext,
                scan -> isExcludedTriggerTable(scan, excluded), rewriteContext.isIncludeUpToDateStreams());
    }

    /**
     * Generates the merged delta plan (without INSERT wrapper) for EXPLAIN or execution.
     *
     * @param includeUpToDate if true, includes delta plans for up-to-date streams (EXPLAIN).
     *                         if false, skips them (execution).
     * @return merged plan, or empty relation if no delta plans are available
     */
    Plan generateMergedDeltaPlan(Plan normalizedPlan, IvmRefreshContext ctx,
            Predicate<LogicalOlapScan> isExcluded, boolean includeUpToDate) {
        Pair<Plan, List<LogicalProject<?>>> prefixChain = helper.detachAdaptProjectChain(normalizedPlan);
        // --- Step 0: check AGG ---
        Plan rootPlan = prefixChain.first;
        IvmAggMeta aggMeta = ctx.getRewriteResult() != null
                ? ctx.getRewriteResult().getAggMeta() : null;
        boolean isAgg = aggMeta != null;

        // --- Step 1 (AGG only): detach entire chain above+including AGG ---
        LogicalAggregate<?> savedAgg = null;
        List<LogicalProject<?>> savedChain = new ArrayList<>();
        Plan workPlan = rootPlan;
        if (isAgg) {
            Plan current = rootPlan;
            while (current != null && !(current instanceof LogicalAggregate)) {
                Preconditions.checkState(current instanceof LogicalProject,
                        "IVM: unexpected node above AGG: " + current.getClass().getSimpleName());
                savedChain.add((LogicalProject<?>) current);
                current = current.child(0);
            }
            Preconditions.checkState(current instanceof LogicalAggregate,
                    "IVM: AGG MV missing aggregate node");
            savedAgg = (LogicalAggregate<?>) current;
            workPlan = savedAgg.child(0);
        }

        // --- Step 2: generate delta plans from workPlan ---
        List<Plan> deltaPlans = generateDeltaPlans(workPlan, ctx, isExcluded, includeUpToDate);
        if (deltaPlans.isEmpty()) {
            return new LogicalEmptyRelation(
                    ctx.getConnectContext().getStatementContext().getNextRelationId(), normalizedPlan.getOutput());
        }
        long refreshVersion = isAgg ? 0L : ctx.getMtmv().getNextRefreshVersion();

        // --- Step 3: per-table visitor rewrite ---
        IvmDeltaRewriteVisitor visitor = new IvmDeltaRewriteVisitor();
        List<Plan> rewrittenPlans = new ArrayList<>();
        for (int deltaPlanIndex = 0; deltaPlanIndex < deltaPlans.size(); deltaPlanIndex++) {
            Plan deltaPlan = deltaPlans.get(deltaPlanIndex);
            IvmDeltaRewriteResult result = visitor.rewritePlan(deltaPlan, ctx);
            rewrittenPlans.add(isAgg ? result.plan : addDeltaSequence(result.plan, deltaPlanIndex, refreshVersion));
        }

        // --- Step 4: UNION ALL ---
        Plan mergedPlan;
        if (rewrittenPlans.size() == 1) {
            mergedPlan = rewrittenPlans.get(0);
        } else {
            List<Plan> unionChildren = new ArrayList<>(rewrittenPlans.size());
            for (Plan rewrittenPlan : rewrittenPlans) {
                // Fresh copy each finalized child right before UNION ALL so different branches never
                // reuse ExprIds, while earlier rewrite stages still see the original slot mappings.
                unionChildren.add(helper.freshPlan(rewrittenPlan).first);
            }
            mergedPlan = helper.buildUnionAll(unionChildren);
            if (isAgg) {
                List<Slot> targetOutputs = new ArrayList<>(workPlan.getOutput());
                targetOutputs.addAll(mergedPlan.getOutput().subList(workPlan.getOutput().size(),
                        mergedPlan.getOutput().size()));
                mergedPlan = helper.projectUnionOutputs((LogicalUnion) mergedPlan, targetOutputs);
            }
        }

        if (isAgg) {
            // --- Step 5 (AGG only): re-attach AGG, call aggHandler directly ---
            mergedPlan = reattachAggAndProcess(savedAgg, workPlan, mergedPlan, aggMeta, ctx);
            // --- Step 6 (AGG only): rebuild above-AGG chain bottom-up ---
            mergedPlan = rebuildAboveAggChain(savedChain, mergedPlan);
        }

        // --- Final step: dml_factor → delete_sign (all paths converge here) ---
        Slot dmlSlot = helper.findSlotByName(mergedPlan.getOutput(), Column.IVM_DML_FACTOR_COL);
        mergedPlan = helper.finalizeQuery(prefixChain, new IvmDeltaRewriteResult(mergedPlan, dmlSlot, null), ctx);
        return mergedPlan;
    }

    private Plan addDeltaSequence(Plan plan, int deltaPlanIndex, long refreshVersion) {
        long prefix = getSequencePrefix(refreshVersion, deltaPlanIndex);
        Slot baseOpSlot = helper.findSlotByName(plan.getOutput(), Column.IVM_BASE_OP_COL);
        List<NamedExpression> outputs = new ArrayList<>(plan.getOutput());
        Expression phase = new If(new GreaterThan(baseOpSlot, new TinyIntLiteral((byte) 0)),
                new BigIntLiteral(1), new BigIntLiteral(0));
        outputs.add(new Alias(new Add(new BigIntLiteral(prefix), phase), Column.SEQUENCE_COL));
        return new LogicalProject<>(outputs, plan);
    }

    private long getSequencePrefix(long refreshVersion, int deltaPlanIndex) {
        Preconditions.checkState(refreshVersion >= 0 && refreshVersion <= (Long.MAX_VALUE >>> 11),
                "IVM refresh version exceeds the sequence encoding range: %s", refreshVersion);
        Preconditions.checkState(deltaPlanIndex >= 0 && deltaPlanIndex < 1024,
                "invalid IVM delta plan index: %s", deltaPlanIndex);
        return (refreshVersion << 11) | ((long) deltaPlanIndex << 1);
    }

    // ---------------------------------------------------------------------------
    // AGG chain detach / re-attach helpers
    // ---------------------------------------------------------------------------

    /**
     * Remaps the saved aggregate's group-by and output expressions from old child output slots
     * to merged plan output slots, then calls {@link IvmAggDeltaHandler#rewriteAggregate}.
     */
    private Plan reattachAggAndProcess(LogicalAggregate<?> savedAgg, Plan aggChild,
            Plan mergedPlan, IvmAggMeta aggMeta, IvmRefreshContext ctx) {
        // Build positional map: aggChild old output → mergedPlan new output
        Map<ExprId, ExprId> mapping = buildPositionalMap(aggChild.getOutput(), mergedPlan.getOutput());
        LogicalAggregate<?> remappedAgg = remapAggSlots(savedAgg, mapping);
        Slot dmlSlot = helper.findSlotByName(mergedPlan.getOutput(), Column.IVM_DML_FACTOR_COL);
        Slot baseOpSlot = helper.findSlotByNameOrNull(mergedPlan.getOutput(), Column.IVM_BASE_OP_COL);
        IvmDeltaRewriteResult childResult = new IvmDeltaRewriteResult(mergedPlan, dmlSlot, baseOpSlot);
        return aggHandler.rewriteAggregate(remappedAgg, childResult, ctx).plan;
    }

    /** Remaps group-by and output expressions of the aggregate node. */
    private LogicalAggregate<?> remapAggSlots(LogicalAggregate<?> agg, Map<ExprId, ExprId> mapping) {
        List<Expression> newGroupBy = new ArrayList<>();
        for (Expression expr : agg.getGroupByExpressions()) {
            newGroupBy.add(remapExprId(expr, mapping));
        }
        List<NamedExpression> newOutputs = new ArrayList<>();
        for (NamedExpression expr : agg.getOutputExpressions()) {
            newOutputs.add((NamedExpression) remapExprId(expr, mapping));
        }
        return agg.withGroupByAndOutput(newGroupBy, newOutputs);
    }

    /**
     * Rebuilds the above-AGG chain bottom-up, remapping expressions and passing dml_factor
     * through each project (matching what {@code IvmLinearDeltaHandler} does in the visitor).
     */
    private Plan rebuildAboveAggChain(List<LogicalProject<?>> savedChain, Plan applyPlan) {
        Plan currentPlan = applyPlan;
        Slot dmlSlot = helper.findSlotByName(currentPlan.getOutput(), Column.IVM_DML_FACTOR_COL);
        for (int i = savedChain.size() - 1; i >= 0; i--) {
            LogicalProject<?> savedProj = savedChain.get(i);
            List<Slot> oldChildOut = savedProj.child(0).getOutput();
            List<Slot> newChildOut = currentPlan.getOutput();
            Map<ExprId, ExprId> mapping = buildPositionalMap(oldChildOut, newChildOut);
            currentPlan = rebuildProjectNode(savedProj, currentPlan, mapping, dmlSlot);
            dmlSlot = helper.findSlotByName(currentPlan.getOutput(), Column.IVM_DML_FACTOR_COL);
        }
        return currentPlan;
    }

    /** Rebuilds a single saved project node remapping its expressions and adding dml_factor. */
    private LogicalProject<?> rebuildProjectNode(LogicalProject<?> savedProj, Plan newChild,
            Map<ExprId, ExprId> mapping, Slot dmlSlot) {
        List<NamedExpression> newExprs = new ArrayList<>();
        for (NamedExpression expr : savedProj.getProjects()) {
            newExprs.add((NamedExpression) remapExprId(expr, mapping));
        }
        newExprs.add(new Alias(dmlSlot, dmlSlot.getName()));
        return new LogicalProject<>(ImmutableList.copyOf(newExprs), newChild);
    }

    /** Builds a positional ExprId map from old output slots to new output slots. */
    private Map<ExprId, ExprId> buildPositionalMap(List<Slot> oldOutput, List<Slot> newOutput) {
        Map<ExprId, ExprId> map = new HashMap<>();
        for (int i = 0; i < oldOutput.size(); i++) {
            map.put(oldOutput.get(i).getExprId(), newOutput.get(i).getExprId());
        }
        return map;
    }

    /** Replaces SlotReference ExprIds in an expression tree using the given mapping. */
    private Expression remapExprId(Expression expr, Map<ExprId, ExprId> map) {
        return expr.rewriteDownShortCircuit(node -> {
            if (node instanceof SlotReference) {
                ExprId newId = map.get(((SlotReference) node).getExprId());
                if (newId != null) {
                    return ((SlotReference) node).withExprId(newId);
                }
            }
            return node;
        });
    }

    /**
     * Generates delta plans from the normalized plan by replacing each pending-delta
     * OlapScan with its delta source and rewriting other scans to pre/post snapshots.
     * Returns one plan per OlapScan that has pending delta data.
     *
     * <p>For the i-th delta scan Si in the collected scan list:
     * <ul>
     *   <li>Si is replaced with its delta source (LogicalOlapTableStreamScan)</li>
     *   <li>Sj where j &lt; i becomes a post-refresh snapshot scan</li>
     *   <li>Sj where j &gt; i becomes a pre-refresh snapshot scan</li>
     * </ul>
     *
     * @return list of delta plans, or empty if all streams are up-to-date
     */
    List<Plan> generateDeltaPlans(Plan normalizedPlan,
            IvmRefreshContext ctx,
            Predicate<LogicalOlapScan> isExcluded,
            boolean includeUpToDate) {
        long mvId = ctx.getMtmv().getId();
        List<DeltaPlanContext> deltaPlanContexts = generateDeltaPlanContexts(normalizedPlan, ctx,
                isExcluded, includeUpToDate, mvId);
        if (deltaPlanContexts.isEmpty()) {
            return Collections.emptyList();
        }

        List<Plan> deltaPlans = new ArrayList<>();
        for (DeltaPlanContext deltaPlanContext : deltaPlanContexts) {
            deltaPlans.add(deltaPlanContext.deltaPlan);
        }
        return deltaPlans;
    }

    private List<DeltaPlanContext> generateDeltaPlanContexts(Plan normalizedPlan,
            IvmRefreshContext ctx,
            Predicate<LogicalOlapScan> isExcluded,
            boolean includeUpToDateStreams, long mvId) {
        List<DeltaScanContext> scanContexts = collectDeltaScanContexts(normalizedPlan, ctx, isExcluded, mvId);
        if (scanContexts.isEmpty()) {
            return Collections.emptyList();
        }

        List<DeltaPlanContext> deltaPlanContexts = new ArrayList<>();
        for (int i = 0; i < scanContexts.size(); i++) {
            DeltaScanContext scanContext = scanContexts.get(i);
            if (!includeUpToDateStreams && scanContext.isUpToDate()) {
                continue;
            }
            Plan deltaPlan = generateDeltaPlan(normalizedPlan, isExcluded, scanContexts, i, mvId);
            deltaPlanContexts.add(new DeltaPlanContext(scanContext, deltaPlan));
        }
        return deltaPlanContexts;
    }

    private List<DeltaScanContext> collectDeltaScanContexts(Plan normalizedPlan,
            IvmRefreshContext ctx,
            Predicate<LogicalOlapScan> isExcluded, long mvId) {
        List<LogicalOlapScan> allScans = new ArrayList<>();
        List<TableNameInfo> tableNames = new ArrayList<>();
        Map<TableNameInfo, Integer> occurrences = new HashMap<>();
        List<Integer> occurrenceIndexes = new ArrayList<>();
        rewriteOlapScans(normalizedPlan, isExcluded, scan -> {
            allScans.add(scan);
            TableNameInfo tableNameInfo = IvmRefreshContext.toTableNameInfo(scan);
            if (tableNameInfo == null) {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM: failed to resolve base table for scan: " + scan.getTable().getName());
            }
            tableNames.add(tableNameInfo);
            int occurrence = occurrences.getOrDefault(tableNameInfo, 0) + 1;
            occurrences.put(tableNameInfo, occurrence);
            occurrenceIndexes.add(occurrence);
            return scan;
        });

        if (allScans.isEmpty()) {
            return Collections.emptyList();
        }

        List<DeltaScanContext> contexts = new ArrayList<>();
        for (int i = 0; i < allScans.size(); i++) {
            LogicalOlapScan scan = allScans.get(i);
            OlapTableStream stream = getStream((OlapTable) scan.getTable(), mvId);
            contexts.add(new DeltaScanContext(tableNames.get(i),
                    occurrenceIndexes.get(i), stream, hasPendingData(stream, scan.getSelectedPartitionIds())));
        }
        return contexts;
    }

    private Plan generateDeltaPlan(Plan normalizedPlan,
            Predicate<LogicalOlapScan> isExcluded,
            List<DeltaScanContext> scanContexts,
            int deltaIndex, long mvId) {
        AtomicInteger scanIdx = new AtomicInteger(0);
        Plan modifiedPlan = rewriteOlapScans(normalizedPlan, isExcluded, scan -> {
            int currentIndex = scanIdx.getAndIncrement();
            DeltaScanContext ctx = scanContexts.get(currentIndex);
            if (currentIndex == deltaIndex) {
                return replaceWithDelta(scan, ctx);
            } else if (currentIndex < deltaIndex) {
                return helper.remapOlapScanToPlan(scan, scan.withPostSnapshot());
            } else {
                return helper.remapOlapScanToPlan(scan,
                        scan.withPreSnapshot(Optional.of(ctx.stream)));
            }
        });

        long deltaCount = modifiedPlan.collectToList(
                n -> n instanceof LogicalOlapTableStreamScan
                        && ((LogicalOlapTableStreamScan) n).isIncremental()).size();
        Preconditions.checkState(deltaCount == 1,
                "IVM: expected exactly 1 delta scan per bundle, got " + deltaCount);

        return detachMemo(modifiedPlan);
    }

    private Plan detachMemo(Plan plan) {
        // The normalized plan comes from the MV-query CascadesContext. Delta commands are
        // analyzed in fresh contexts, so stale GroupExpression pointers must not be reused.
        return plan.rewriteUp(node -> node.getGroupExpression().isPresent()
                ? node.withGroupExpression(Optional.empty()) : node);
    }

    /**
     * Visits every {@link LogicalOlapScan} in the plan tree using
     * {@link Plan#rewriteDownShortCircuit}, skipping scans matched by
     * {@code isExcluded}, and applying {@code visitor} to each non-excluded scan.
     */
    private Plan rewriteOlapScans(Plan plan, Predicate<LogicalOlapScan> isExcluded,
            Function<LogicalOlapScan, Plan> visitor) {
        return plan.rewriteDownShortCircuit(node -> {
            if (node instanceof LogicalOlapScan) {
                LogicalOlapScan scan = (LogicalOlapScan) node;
                if (isExcluded.test(scan)) {
                    return node;
                }
                return visitor.apply(scan);
            }
            return node;
        });
    }

    /**
     * Replaces a scan with its delta source, wrapped in a Project that remaps
     * StreamScan output slots to the OlapScan slot ExprIds for matching base columns,
     * so parent expressions that reference old ExprIds continue to work.
     *
     * <p>Project output = base columns (mapped to old ExprId) + stream-only columns (passthrough).
     */
    private boolean hasPendingData(OlapTableStream stream, List<Long> partitionIds) {
        OlapTable baseTable = stream.getBaseTableNullable();
        if (baseTable == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM: stream base table is null for stream " + stream.getName());
        }
        for (Long partitionId : partitionIds) {
            if (baseTable.getPartition(partitionId) != null
                    && stream.hasData(baseTable.getPartition(partitionId))) {
                return true;
            }
        }
        return false;
    }

    private Plan replaceWithDelta(LogicalOlapScan scan, DeltaScanContext ctx) {
        LogicalOlapTableStreamScan streamScan = createStreamScan(scan, ctx.stream);
        return helper.remapOlapScanToPlan(scan, streamScan);
    }

    private LogicalOlapTableStreamScan createStreamScan(LogicalOlapScan scan, OlapTableStream stream) {
        OlapTable originTable = (OlapTable) scan.getTable();
        OlapTableStreamWrapper streamWrapper = new OlapTableStreamWrapper(
                stream, originTable, scan.getSelectedPartitionIds());
        return new LogicalOlapTableStreamScan(
                StatementScopeIdGenerator.newRelationId(),
                streamWrapper,
                scan.getQualifier(),
                scan.getSelectedPartitionIds(),
                scan.getSelectedTabletIds(),
                scan.getHints(),
                scan.getTableSample(),
                scan.getOperativeSlots()
        );
    }

    private OlapTableStream getStream(OlapTable originTable, long mvId) {
        String streamName = IvmUtil.streamName(mvId, originTable.getName());
        String dbName = originTable.getQualifiedDbName();
        try {
            TableIf streamTable = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName)
                    .getTableOrAnalysisException(streamName);
            if (!(streamTable instanceof OlapTableStream)) {
                throw new IvmException(IvmFailureReason.STREAM_UNSUPPORTED,
                        "IVM: stream " + streamName + " is not an OlapTableStream");
            }
            return (OlapTableStream) streamTable;
        } catch (Exception e) {
            throw new IvmException(IvmFailureReason.STREAM_UNSUPPORTED,
                    "IVM: stream not found for base table " + originTable.getName(), e);
        }
    }

    boolean isExcludedTriggerTable(LogicalOlapScan scan, Set<TableNameInfo> excludedTriggerTables) {
        if (excludedTriggerTables == null || excludedTriggerTables.isEmpty()) {
            return false;
        }
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(scan.getTable());
        if (tableNameInfo == null) {
            return false;
        }
        return MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, tableNameInfo);
    }

    private static class DeltaScanContext {
        private final TableNameInfo tableNameInfo;
        // 1-based scan occurrence for the same base table, used to identify self-join delta plans.
        private final int occurrence;
        private final OlapTableStream stream;
        private final boolean hasPendingData;

        private DeltaScanContext(TableNameInfo tableNameInfo,
                int occurrence, OlapTableStream stream, boolean hasPendingData) {
            this.tableNameInfo = tableNameInfo;
            this.occurrence = occurrence;
            this.stream = stream;
            this.hasPendingData = hasPendingData;
        }

        private boolean isUpToDate() {
            return !hasPendingData;
        }
    }

    private static class DeltaPlanContext {
        private final DeltaScanContext scanContext;
        private final Plan deltaPlan;

        private DeltaPlanContext(DeltaScanContext scanContext, Plan deltaPlan) {
            this.scanContext = scanContext;
            this.deltaPlan = deltaPlan;
        }
    }
}

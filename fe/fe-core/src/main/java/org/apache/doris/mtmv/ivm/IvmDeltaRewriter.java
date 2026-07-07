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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.ivm.agg.IvmAggMeta;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

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
 * <p>The rewriter generates one bundle per OlapScan that has pending delta data
 * ({@code consumedTso != latestTso}). OlapScans belonging to excluded trigger tables
 * are skipped entirely (assumed unchanged). For the i-th delta scan Si:
 * <ul>
 *   <li>Si → {@link #replaceWithDelta} (LogicalOlapTableStreamScan as delta source)</li>
 *   <li>Sj where j &lt; i → {@code Sj.withTso(latestTso)} (v2, post-delta snapshot)</li>
 *   <li>Sj where j &gt; i → {@code Sj.withTso(consumedTso)} (v1, pre-delta snapshot)</li>
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

        // --- Step 3: per-table visitor rewrite ---
        IvmDeltaRewriteVisitor visitor = new IvmDeltaRewriteVisitor();
        List<Plan> rewrittenPlans = new ArrayList<>();
        for (Plan deltaPlan : deltaPlans) {
            IvmDeltaRewriteResult result = visitor.rewritePlan(deltaPlan, ctx);
            rewrittenPlans.add(result.plan);
        }

        // --- Step 4: UNION ALL ---
        Plan mergedPlan;
        if (rewrittenPlans.size() == 1) {
            mergedPlan = rewrittenPlans.get(0);
        } else {
            mergedPlan = helper.buildUnionAll(rewrittenPlans);
        }

        if (isAgg) {
            // --- Step 5 (AGG only): re-attach AGG, call aggHandler directly ---
            mergedPlan = reattachAggAndProcess(savedAgg, workPlan, mergedPlan, aggMeta, ctx);
            // --- Step 6 (AGG only): rebuild above-AGG chain bottom-up ---
            mergedPlan = rebuildAboveAggChain(savedChain, mergedPlan);
        } else if (hasMowIncrementalStreamScan(mergedPlan)) {
            // --- Step 5 (non-AGG): binlog order rewrite ---
            mergedPlan = applyBinlogOrderRewrite(mergedPlan, ctx);
        }

        // --- Final step: dml_factor → delete_sign (all paths converge here) ---
        Slot dmlSlot = helper.findSlotByName(mergedPlan.getOutput(), Column.IVM_DML_FACTOR_COL);
        mergedPlan = helper.finalizeQuery(prefixChain, new IvmDeltaRewriteResult(mergedPlan, dmlSlot, null), ctx);
        return mergedPlan;
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
     * OlapScan with its delta source and binding TSO snapshots on other scans.
     * Returns one plan per OlapScan that has pending delta data.
     *
     * <p>For the i-th delta scan Si in the collected scan list:
     * <ul>
     *   <li>Si is replaced with its delta source (LogicalOlapTableStreamScan)</li>
     *   <li>Sj where j &lt; i gets bound to latestTso (v2, post-delta snapshot)</li>
     *   <li>Sj where j &gt; i gets bound to consumedTso (v1, pre-delta snapshot)</li>
     * </ul>
     *
     * @return list of plans with TSO bindings, or empty if all scans are up-to-date
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
        List<DeltaScanContext> scanContexts = collectDeltaScanContexts(normalizedPlan, ctx, isExcluded);
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
            Predicate<LogicalOlapScan> isExcluded) {
        List<LogicalOlapScan> allScans = new ArrayList<>();
        List<TableNameInfo> tableNames = new ArrayList<>();
        Map<TableNameInfo, Integer> occurrences = new HashMap<>();
        List<Integer> occurrenceIndexes = new ArrayList<>();
        rewriteOlapScans(normalizedPlan, isExcluded, scan -> {
            allScans.add(scan);
            TableNameInfo tableNameInfo = IvmRefreshContext.toTableNameInfo(scan);
            if (tableNameInfo == null) {
                throw new AnalysisException(
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

        // TODO: Compute consumedTso/latestTso from OlapTableStream.getStreamUpdate()
        // once streams are auto-created (Phase 1). For now use placeholder values.
        List<DeltaScanContext> contexts = new ArrayList<>();
        for (int i = 0; i < allScans.size(); i++) {
            // Placeholder: stream.getStreamUpdate(partitionId) → (consumed, latest)
            contexts.add(new DeltaScanContext(tableNames.get(i),
                    occurrenceIndexes.get(i), 0L, Long.MAX_VALUE));
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
                return replaceWithDelta(scan, ctx, mvId);
            } else if (currentIndex < deltaIndex) {
                return scan.withTso(ctx.latestTso);
            } else {
                return scan.withTso(ctx.consumedTso);
            }
        });

        long deltaCount = modifiedPlan.collectToList(
                n -> n instanceof LogicalOlapTableStreamScan
                        && ((LogicalOlapTableStreamScan) n).isIncrementalScan()).size();
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
    private Plan replaceWithDelta(LogicalOlapScan scan, DeltaScanContext ctx, long mvId) {
        LogicalOlapTableStreamScan streamScan = createStreamScan(scan, mvId);
        return replaceOlapScanWithStreamScan(scan, streamScan);
    }

    private LogicalOlapTableStreamScan createStreamScan(LogicalOlapScan scan, long mvId) {
        OlapTable originTable = (OlapTable) scan.getTable();
        OlapTableStream stream = getStreamOrNull(scan, originTable, mvId);
        if (stream == null) {
            // In production the stream must exist; only FE unit tests may lack a stream.
            if (FeConstants.runningUnitTest) {
                return new LogicalOlapTableStreamScan(
                        StatementScopeIdGenerator.newRelationId(),
                        originTable,
                        scan.getQualifier(),
                        scan.getSelectedPartitionIds(),
                        scan.getSelectedTabletIds(),
                        scan.getHints(),
                        scan.getTableSample(),
                        scan.getOperativeSlots()
                ).withIncrementalScan(true);
            }
            throw new AnalysisException("IVM: stream not found for base table "
                    + originTable.getName());
        }
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
        ).withIncrementalScan(true);
    }

    /**
     * Wraps the StreamScan with a Project that maps base column slots back to
     * the old OlapScan ExprIds so parent expressions are not broken.
     *
     * <p>Project expressions:
     * <ul>
     *   <li>Base columns (same name in both): {@code Alias(oldExprId, streamSlot, name)}</li>
     *   <li>Stream-only columns (e.g. seq, changeType): passthrough as raw SlotReference</li>
     * </ul>
     */
    private LogicalProject<?> replaceOlapScanWithStreamScan(LogicalOlapScan oldScan,
            LogicalOlapTableStreamScan streamScan) {
        List<Slot> oldOutput = oldScan.getOutput();
        List<Slot> streamOutput = streamScan.getOutput();

        Map<String, Slot> streamSlotByName = new HashMap<>();
        for (Slot slot : streamOutput) {
            streamSlotByName.put(slot.getName(), slot);
        }

        List<NamedExpression> projects = new ArrayList<>();
        for (Slot oldSlot : oldOutput) {
            Slot streamSlot = streamSlotByName.get(oldSlot.getName());
            if (streamSlot != null) {
                projects.add(new Alias(oldSlot.getExprId(), streamSlot, oldSlot.getName()));
            } else if (oldSlot.getName().startsWith(Column.HIDDEN_COLUMN_PREFIX)) {
                // Hidden columns (e.g. __DORIS_DELETE_SIGN__, __DORIS_VERSION_COL__)
                // exist in old OlapScan output but not in stream scan output;
                // fill with NULL literal to keep output schema consistent.
                projects.add(new Alias(oldSlot.getExprId(),
                        new NullLiteral(oldSlot.getDataType()), oldSlot.getName()));
            } else {
                throw new AnalysisException("IVM: stream scan missing column "
                        + oldSlot.getName() + " for table " + oldScan.getTable().getName());
            }
        }

        return new LogicalProject<>(projects, streamScan);
    }

    private OlapTableStream getStreamOrNull(LogicalOlapScan scan, OlapTable originTable, long mvId) {
        String streamName = IvmUtil.streamName(mvId, originTable.getName());
        String dbName = originTable.getQualifiedDbName();
        try {
            TableIf streamTable = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName)
                    .getTableOrAnalysisException(streamName);
            if (!(streamTable instanceof OlapTableStream)) {
                return null;
            }
            return (OlapTableStream) streamTable;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Checks whether the merged delta plan contains an incremental stream scan on a MOW table.
     * Only non-AGG MVs with MOW base table deltas need the binlog order rewrite.
     */
    private boolean hasMowIncrementalStreamScan(Plan mergedPlan) {
        return mergedPlan.anyMatch(node -> {
            if (!(node instanceof LogicalOlapTableStreamScan)) {
                return false;
            }
            LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) node;
            if (!streamScan.isIncrementalScan()) {
                return false;
            }
            TableIf table = streamScan.getTable();
            if (table instanceof OlapTable) {
                return ((OlapTable) table).getKeysType() == KeysType.UNIQUE_KEYS;
            }
            return false;
        });
    }

    /**
     * Wraps the merged delta plan with CTE + split + FULL OUTER JOIN + IF(isnull) Project
     * to ensure delete rows are processed before insert rows for the same key.
     * This replaces the Sort approach, avoiding full-data spill in AP scenarios.
     */
    Plan applyBinlogOrderRewrite(Plan mergedPlan, IvmRefreshContext ctx) {
        Slot rowIdSlot = IvmUtil.findRowIdSlot(mergedPlan.getOutput(), "merged delta plan");

        // ① CTE: share the delta plan via producer/consumer instead of deep copy.
        //    The FOJ children are LogicalCTEConsumer (leaf nodes) so checkConflictAlias
        //    never reaches the stream scan — no "Not unique table/alias" error.
        CTEId cteId = StatementScopeIdGenerator.newCTEId();
        LogicalSubQueryAlias<Plan> producerAlias = new LogicalSubQueryAlias<>(
                "__doris_ivm_cte", (Plan) mergedPlan);
        LogicalCTEProducer<Plan> producer = new LogicalCTEProducer<>(cteId,
                (LogicalPlan) producerAlias);
        LogicalCTEConsumer insertConsumer = new LogicalCTEConsumer(
                StatementScopeIdGenerator.newRelationId(), cteId, "__doris_ivm_cte_insert",
                (LogicalPlan) mergedPlan);
        LogicalCTEConsumer deleteConsumer = new LogicalCTEConsumer(
                StatementScopeIdGenerator.newRelationId(), cteId, "__doris_ivm_cte_delete",
                (LogicalPlan) mergedPlan);

        // ② Split: filter by baseOp on each consumer, wrapped in SubQueryAlias
        //    like the parser does for "SELECT ... FROM (SELECT * FROM cte WHERE ...) alias".
        Plan insertBranch = new LogicalSubQueryAlias<>("__doris_ivm_insert",
                new LogicalFilter<>(ImmutableSet.of(
                        new GreaterThan(helper.findSlotByName(insertConsumer.getOutput(),
                                Column.IVM_BASE_OP_COL), new TinyIntLiteral((byte) 0))), insertConsumer));
        Plan deleteBranch = new LogicalSubQueryAlias<>("__doris_ivm_delete",
                new LogicalFilter<>(ImmutableSet.of(
                        new LessThan(helper.findSlotByName(deleteConsumer.getOutput(),
                                Column.IVM_BASE_OP_COL), new TinyIntLiteral((byte) 0))), deleteConsumer));

        Slot insertRowId = helper.findSlotByName(insertBranch.getOutput(), rowIdSlot.getName());
        Slot deleteRowId = helper.findSlotByName(deleteBranch.getOutput(), rowIdSlot.getName());

        // ③ FULL OUTER JOIN on row_id
        LogicalJoin<Plan, Plan> foj = new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(new EqualTo(insertRowId, deleteRowId)), ImmutableList.of(),
                insertBranch, deleteBranch, JoinReorderContext.EMPTY);

        // ④ Final Project: every column through IF(insert.row_id IS NULL, delete.col, insert.col)
        List<Slot> outputs = insertBranch.getOutput();
        ImmutableList.Builder<NamedExpression> finalProjects = ImmutableList.builderWithExpectedSize(
                outputs.size());
        for (Slot col : outputs) {
            Slot deleteCol = helper.findSlotByName(deleteBranch.getOutput(), col.getName());
            finalProjects.add(new Alias(col.getExprId(),
                    new If(new IsNull(insertRowId), deleteCol, col), col.getName()));
        }
        LogicalProject<Plan> ifProject = new LogicalProject<>(finalProjects.build(), foj);

        // ⑤ Wrap with CTE Anchor: Producer (left) + main plan (right)
        return new LogicalCTEAnchor<>(cteId, producer, ifProject);
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
        private final long consumedTso;
        private final long latestTso;

        private DeltaScanContext(TableNameInfo tableNameInfo,
                int occurrence, long consumedTso, long latestTso) {
            this.tableNameInfo = tableNameInfo;
            this.occurrence = occurrence;
            this.consumedTso = consumedTso;
            this.latestTso = latestTso;
        }

        private boolean isUpToDate() {
            return consumedTso == latestTso;
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

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
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVPropertyUtil;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.ConnectContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Entry point for IVM delta rewriting.
 *
 * <p>The rewriter visits the normalized tree once and constructs its delta relation recursively. Join children
 * use pre- and post-refresh snapshots as required by their algebraic expansion.
 */
public class IvmDeltaRewriter {

    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;

    /**
     * Generates the rewritten sink child for the analyzer rule path.
     * The input sink child may already contain bind-sink adapter projects.
     */
    public Plan generateIncrRefreshPlan(Plan sinkChild, IvmRewriteResult rewriteResult,
            IvmRewriteContext rewriteContext, ConnectContext connectContext) {
        IvmIncrRefreshContext refreshContext = new IvmIncrRefreshContext(
                rewriteContext.getMtmv(), connectContext, rewriteResult,
                rewriteContext.isIncludeExhaustedStreams());
        Pair<Plan, List<LogicalProject<?>>> prefixChain = helper.detachAdaptProjectChain(sinkChild);
        Plan rootPlan = prefixChain.first;
        long refreshVersion = refreshContext.getMtmv().getNextRefreshVersion();
        IvmDeltaRewriteState rewriteState = createDeltaRewriteState(rootPlan, refreshContext, refreshVersion,
                rewriteContext.getIncrementalScopePartitionIds());
        Optional<IvmDeltaRewriteResult> deltaResult = rewriteDelta(rootPlan, refreshContext, rewriteState);
        if (!deltaResult.isPresent()) {
            return new LogicalEmptyRelation(
                    refreshContext.getConnectContext().getStatementContext().getNextRelationId(),
                    sinkChild.getOutput());
        }
        // A non-empty delta reaches this point, and only an aggregate delta joins the MV's
        // old rows with the new delta. If the previous refresh txn committed but its data is
        // not yet visible (an MV partition's committed version is ahead of its visible
        // version), that join misses the old rows and the delta is permanently lost. Fail
        // here; the fallback chain recomputes from the base tables without reading old MV
        // state. EXPLAIN REFRESH is exempt: it only produces a plan and neither executes it
        // nor reads MV data.
        if (!rewriteContext.isExplain()
                && rewriteResult.isAggMv()
                && hasUnpublishedCommittedMvData(rewriteContext.getMtmv())) {
            throw new IvmException(IvmFailureReason.MV_COMMIT_NOT_VISIBLE,
                    "previous refresh txn committed but its MV data is not visible yet; "
                            + "aggregate delta would join stale old MV rows");
        }
        Plan deltaPlan = deltaResult.get().plan;
        IvmDeltaRewriteResult result = deltaResult.get();
        IvmDeltaRewriteResult mergedResult = new IvmDeltaRewriteResult(deltaPlan,
                helper.findSlotByName(deltaPlan.getOutput(), Column.IVM_DML_FACTOR_COL),
                helper.findSlotByName(deltaPlan.getOutput(), Column.SEQUENCE_COL), result.maxDeltaIndex);
        Slot rootRowId = IvmUtil.findRowIdSlot(rootPlan.getOutput(), "normalized plan root");
        if (!refreshContext.getRewriteResult().isDeterministic(rootRowId)) {
            mergedResult = helper.wrapDmlFactorWithRootNonDetGuard(mergedResult);
        }
        return helper.finalizeQuery(prefixChain, mergedResult, refreshContext);
    }

    Optional<IvmDeltaRewriteResult> rewriteDelta(Plan plan, IvmIncrRefreshContext ctx,
            IvmDeltaRewriteState rewriteState) {
        IvmDeltaRewriteVisitor visitor = new IvmDeltaRewriteVisitor(
                new IvmLinearDeltaHandler(), new IvmJoinDeltaHandler(), new IvmAggDeltaHandler(), rewriteState);
        return visitor.rewritePlan(plan, ctx);
    }

    /**
     * A refresh txn assigns the next partition version when it commits, while the
     * partition's visible version only advances when the txn publishes. Any MV
     * partition whose committed version is still ahead of its visible version
     * therefore holds unpublished refresh data.
     */
    private boolean hasUnpublishedCommittedMvData(MTMV mtmv) {
        if (Config.isCloudMode()) {
            return false;
        }
        for (Partition partition : mtmv.getPartitions()) {
            if (partition.getCommittedVersion() > partition.getVisibleVersion()) {
                return true;
            }
        }
        return false;
    }

    static Pair<Plan, Map<Slot, Slot>> preSnapshot(Plan plan, IvmDeltaRewriteState rewriteState) {
        return rewriteSnapshot(plan, rewriteState, true);
    }

    static Pair<Plan, Map<Slot, Slot>> postSnapshot(Plan plan, IvmDeltaRewriteState rewriteState) {
        return rewriteSnapshot(plan, rewriteState, false);
    }

    private static Pair<Plan, Map<Slot, Slot>> rewriteSnapshot(Plan plan, IvmDeltaRewriteState rewriteState,
            boolean preSnapshot) {
        Plan rewritten = plan.rewriteDownShortCircuit(node -> {
            if (!(node instanceof LogicalOlapScan)) {
                return node;
            }
            LogicalOlapScan scan = (LogicalOlapScan) node;
            if (rewriteState.isExcluded(scan)) {
                // Excluded tables produce no delta, but their snapshot side still joins
                // the delta side; apply the window so the property also saves their scan.
                return rewriteState.restrictWindow(scan);
            }
            scan = rewriteState.restrictWindow(scan);
            LogicalPlan snapshotScan = preSnapshot
                    ? scan.withPreSnapshot(Optional.of(rewriteState.getStream(scan)))
                    : scan.withPostSnapshot();
            return IvmDeltaRewriteHelper.INSTANCE.remapOlapScanToPlan(scan, snapshotScan);
        });
        return IvmDeltaRewriteHelper.INSTANCE.freshPlan(rewritten);
    }

    private IvmDeltaRewriteState createDeltaRewriteState(Plan plan, IvmIncrRefreshContext ctx, long refreshVersion,
            Map<BaseTableInfo, Set<Long>> scopePartitionIds) {
        Map<OlapTable, OlapTableStream> streams = new HashMap<>();
        // Window limits apply to every base table in the plan, including excluded
        // trigger tables (their snapshot side is windowed too, so the property saves
        // the join-opposite scan cost even when the table produces no delta).
        Set<OlapTable> planTables = new HashSet<>();
        Set<TableNameInfo> excludedTriggerTables = ctx.getMtmv().getExcludedTriggerTables();
        plan.foreach(node -> {
            if (!(node instanceof LogicalOlapScan)) {
                return;
            }
            LogicalOlapScan scan = (LogicalOlapScan) node;
            OlapTable table = (OlapTable) scan.getTable();
            planTables.add(table);
            if (isExcludedTriggerTable(scan, excludedTriggerTables)) {
                return;
            }
            streams.put(table, IvmUtil.getIvmStream(ctx.getMtmv(), table));
        });
        Map<TableNameInfo, Integer> windowLimits =
                MTMVPropertyUtil.getIvmPartitionWindowLimit(ctx.getMtmv().getMvProperties());
        Map<OlapTable, List<Long>> windowPartitionIdsByTable = new HashMap<>();
        if (!windowLimits.isEmpty()) {
            for (OlapTable table : planTables) {
                windowPartitionIdsByTable.put(table, MTMVPropertyUtil.getIvmPartitionWindowIds(table,
                        new TableNameInfo(table.getFullQualifiers()), windowLimits));
            }
        }
        applyScopePartitionIds(windowPartitionIdsByTable, planTables, scopePartitionIds);
        return new IvmDeltaRewriteState(streams, ctx.isIncludeExhaustedStreams(), refreshVersion,
                DataType.fromCatalogType(ctx.getMtmv().getColumn(Column.SEQUENCE_COL).getType()),
                windowPartitionIdsByTable);
    }

    /**
     * Limits every base table whose partition column feeds the MV's partition column to the base
     * partitions the MV's partition definition keeps. The delta and the join-opposite snapshot
     * would otherwise also read base partitions the MV does not keep, expired by
     * partition_sync_limit, and then try to write their rows into MV partitions that do not exist,
     * which fails the whole insert with "no partition for this tuple" and takes the partitions
     * that do exist down with it.
     *
     * <p>The limit is the partition set the MV is aligned to rather than the partitions it already
     * has, so a base partition whose MV partition has not been added yet stays readable: the
     * refresh still reports the missing partition and recovers it by syncing.
     *
     * <p>Tables outside {@code scopePartitionIds} keep their full read: the MV partition column
     * does not come from them, so limiting them would change join results without narrowing the
     * set of MV partitions the delta can target. A scope that covers every partition of its table
     * is left alone as well, so an MV that mirrors all of its base partitions keeps the plan it
     * had before this restriction existed. An entry with no partitions is the opposite case and is
     * applied as it stands: the scan then reads nothing, which is how the delta and the snapshot
     * side both stay out of a table the MV has no partition for.
     *
     * <p>The scope is keyed by base table identity, but only olap tables are reachable here: the
     * delta reads them through {@code OlapTableStream} and restricts a scan by partition id.
     */
    private static void applyScopePartitionIds(Map<OlapTable, List<Long>> windowPartitionIdsByTable,
            Set<OlapTable> planTables, Map<BaseTableInfo, Set<Long>> scopePartitionIds) {
        if (scopePartitionIds.isEmpty()) {
            return;
        }
        for (OlapTable table : planTables) {
            Set<Long> tableScope = scopePartitionIds.get(new BaseTableInfo(table));
            if (tableScope == null || tableScope.containsAll(table.getPartitionIds())) {
                continue;
            }
            List<Long> windowPartitionIds = windowPartitionIdsByTable.get(table);
            List<Long> readablePartitionIds;
            if (windowPartitionIds == null) {
                readablePartitionIds = new ArrayList<>(tableScope);
            } else {
                // Both limits apply, so only their intersection is readable. One keeps the
                // partitions above a time cutoff and the other the last N by partition value, so
                // each is a suffix of the same value order and the two cannot be disjoint.
                readablePartitionIds = new ArrayList<>();
                for (Long partitionId : windowPartitionIds) {
                    if (tableScope.contains(partitionId)) {
                        readablePartitionIds.add(partitionId);
                    }
                }
            }
            // Sorted like every other partition selection handed to a scan, so the plan shape does
            // not depend on the order the two limits were combined in.
            readablePartitionIds.sort(Long::compareTo);
            windowPartitionIdsByTable.put(table, readablePartitionIds);
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

}

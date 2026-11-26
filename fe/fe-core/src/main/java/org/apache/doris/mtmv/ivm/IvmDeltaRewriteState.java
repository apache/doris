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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.CloudTableStreamReadStateHelper;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPropertyUtil;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan.OlapSnapshotContext;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Mutable state owned by one recursive delta rewrite.
 *
 * <p>It deliberately is not the visitor context: visitor callbacks still receive the immutable
 * {@link IvmIncrRefreshContext}. The state only carries scan-local rewrite data and the deterministic
 * left-to-right delta scan index.
 */
class IvmDeltaRewriteState {
    private final Map<OlapTable, OlapTableStream> streams;
    private final boolean includeExhaustedStreams;
    /** Null for an append-only detail MV, whose delta allocates no sequence. */
    private final IvmSequenceCalculator sequenceCalculator;
    /**
     * Window partition ids (last N partitions by partition value) per base table,
     * configured via the {@code ivm_partition_window_limit} MTMV property. A {@code null}
     * value means the table is unlimited (full table).
     */
    private final Map<OlapTable, List<Long>> windowPartitionIdsByTable;
    // Null on the integrated path. The cloud path fixes all source boundaries before choosing delta branches.
    private Map<Cloud.TableStreamIdentityPB, Map<Long, Cloud.TableStreamPartitionReadStatePB>> cloudReadStates;
    private int nextDeltaScanIndex;

    /**
     * The state of a delta rewrite whose delta allocates sequence numbers, which is what a unique-key MV
     * needs: its sequence column orders the rows a delta writes against the rows the MV holds.
     */
    static IvmDeltaRewriteState forUniqueKeys(Plan plan, IvmIncrRefreshContext ctx, long sequencePrefix,
            IvmRewriteContext rewriteContext) {
        DataType sequenceType = DataType.fromCatalogType(ctx.getMtmv().getColumn(Column.SEQUENCE_COL).getType());
        return create(plan, ctx, IvmSequenceCalculator.create(sequencePrefix, sequenceType), rewriteContext);
    }

    /**
     * The state of a delta rewrite that allocates no sequence and carries no sign: an append-only detail
     * MV has no sequence column, and every row its delta produces is an append, so there is nothing for a
     * sequence to order. Nothing here reads the MV's schema, which is what keeps the sequence column the
     * unique-key model carries out of this path entirely.
     */
    static IvmDeltaRewriteState forAppendOnlyDup(Plan plan, IvmIncrRefreshContext ctx,
            IvmRewriteContext rewriteContext) {
        return create(plan, ctx, null, rewriteContext);
    }

    private static IvmDeltaRewriteState create(Plan plan, IvmIncrRefreshContext ctx,
            IvmSequenceCalculator sequenceCalculator, IvmRewriteContext rewriteContext) {
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
            if (IvmUtil.isExcludedTriggerTable(excludedTriggerTables, scan)) {
                return;
            }
            streams.put(table, IvmUtil.getIvmStream(ctx.getMtmv(), table));
        });
        Map<TableNameInfo, Integer> windowLimits =
                MTMVPropertyUtil.getIvmPartitionWindowLimit(ctx.getMtmv().getMvProperties());
        Map<OlapTable, List<Long>> windowPartitionIdsByTable = new HashMap<>();
        for (OlapTable table : planTables) {
            windowPartitionIdsByTable.put(table, windowPartitionIds(table, windowLimits,
                    rewriteContext.getIncrementalScopePartitionIds()));
        }
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(streams, ctx.isIncludeExhaustedStreams(),
                sequenceCalculator, windowPartitionIdsByTable);
        if (Config.isCloudMode()) {
            state.cloudReadStates = rewriteContext.getCloudReadStates();
            state.checkCloudReadStateCoverage(plan);
        }
        return state;
    }

    static Map<Cloud.TableStreamIdentityPB, Map<Long, Cloud.TableStreamPartitionReadStatePB>>
            preloadCloudReadStates(IvmRewriteContext context, Collection<TableIf> tables) {
        Map<Cloud.TableStreamIdentityPB, Set<Long>> requestedPartitions = new HashMap<>();
        Set<TableNameInfo> excludedTriggerTables = context.getMtmv().getExcludedTriggerTables();
        Map<TableNameInfo, Integer> windowLimits =
                MTMVPropertyUtil.getIvmPartitionWindowLimit(context.getMtmv().getMvProperties());
        for (TableIf collectedTable : new HashSet<>(tables)) {
            if (!(collectedTable instanceof OlapTable)) {
                continue;
            }
            OlapTable table = (OlapTable) collectedTable;
            if (!table.tryReadLock(1, TimeUnit.MINUTES)) {
                throw new AnalysisException("Failed to get read lock on table: " + table.getName());
            }
            try {
                List<String> qualifier = table.getFullQualifiers().subList(0, 2);
                if (IvmUtil.isExcludedTriggerTable(excludedTriggerTables, table, qualifier)) {
                    continue;
                }
                List<Long> window = windowPartitionIds(table, windowLimits, context.getIncrementalScopePartitionIds());
                List<Long> partitionIds = window == null ? table.getPartitionIds() : window;
                if (partitionIds.isEmpty()) {
                    continue;
                }
                OlapTableStream stream;
                try {
                    stream = IvmUtil.getIvmStream(context.getMtmv(), table);
                } catch (IvmException unavailableStream) {
                    // The normalizer may reject the table's changed schema or changelog before it needs the stream.
                    // Leave validation and the original error to analysis.
                    continue;
                }
                requestedPartitions.put(stream.getCloudIdentity(), new HashSet<>(partitionIds));
            } finally {
                table.readUnlock();
            }
        }
        if (requestedPartitions.isEmpty()) {
            return ImmutableMap.of();
        }
        // Only the copied stream identities and partition ids are needed by the RPC.
        try {
            return CloudTableStreamReadStateHelper.getReadStates(requestedPartitions);
        } catch (UserException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    private void checkCloudReadStateCoverage(Plan plan) {
        for (LogicalOlapScan scan : plan.<LogicalOlapScan>collectToList(LogicalOlapScan.class::isInstance)) {
            if (isExcluded(scan)) {
                continue;
            }
            List<Long> partitionIds = restrictWindow(scan).getSelectedPartitionIds();
            Map<Long, Cloud.TableStreamPartitionReadStatePB> states =
                    cloudReadStates.getOrDefault(getStream(scan).getCloudIdentity(), ImmutableMap.of());
            if (!states.keySet().containsAll(partitionIds)) {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM: Cloud Stream binding or partitions changed while planning refresh for "
                                + scan.getTable().getName() + ", retry the refresh");
            }
        }
    }

    private IvmDeltaRewriteState(Map<OlapTable, OlapTableStream> streams,
            boolean includeExhaustedStreams, IvmSequenceCalculator sequenceCalculator,
            Map<OlapTable, List<Long>> windowPartitionIdsByTable) {
        this.streams = new HashMap<>(streams);
        this.includeExhaustedStreams = includeExhaustedStreams;
        this.sequenceCalculator = sequenceCalculator;
        this.windowPartitionIdsByTable = windowPartitionIdsByTable;
    }

    /**
     * The unique-key state over streams, windows and sequence encoding that have already been collected.
     */
    IvmDeltaRewriteState(Map<OlapTable, OlapTableStream> streams, boolean includeExhaustedStreams,
            long sequencePrefix, DataType sequenceType, Map<OlapTable, List<Long>> windowPartitionIdsByTable) {
        this(streams, includeExhaustedStreams, IvmSequenceCalculator.create(sequencePrefix, sequenceType),
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
    private static List<Long> windowPartitionIds(OlapTable table, Map<TableNameInfo, Integer> windowLimits,
            Map<BaseTableInfo, Set<Long>> scopePartitionIds) {
        List<Long> window = windowLimits.isEmpty() ? null : MTMVPropertyUtil.getIvmPartitionWindowIds(table,
                new TableNameInfo(table.getFullQualifiers()), windowLimits);
        if (scopePartitionIds.isEmpty()) {
            return window;
        }
        Set<Long> scope = scopePartitionIds.get(new BaseTableInfo(table));
        if (scope == null || scope.containsAll(table.getPartitionIds())) {
            return window;
        }
        Collection<Long> candidates = window == null ? scope : window;
        // Sort the intersection so the order of applying the limits does not change the plan shape.
        return candidates.stream().filter(scope::contains).sorted().collect(Collectors.toList());
    }

    /**
     * Restricts a scan's selected partitions to the configured window. Returns the same scan
     * when the table is unlimited or the selection already fits the window.
     */
    LogicalOlapScan restrictWindow(LogicalOlapScan scan) {
        List<Long> windowPartitionIds = windowPartitionIdsByTable.get(scan.getTable());
        if (windowPartitionIds == null) {
            return scan;
        }
        List<Long> restricted = windowPartitionIds(scan, windowPartitionIds);
        if (restricted.equals(scan.getSelectedPartitionIds())) {
            return scan;
        }
        return scan.withSelectedPartitionIds(restricted);
    }

    private List<Long> windowPartitionIds(LogicalOlapScan scan, List<Long> windowPartitionIds) {
        List<Long> selectedPartitionIds = scan.getSelectedPartitionIds();
        if (selectedPartitionIds.isEmpty()) {
            // Empty selection means all partitions; the window is the restriction.
            return windowPartitionIds;
        }
        Set<Long> windowPartitionIdSet = new HashSet<>(windowPartitionIds);
        return selectedPartitionIds.stream()
                .filter(windowPartitionIdSet::contains)
                .collect(Collectors.toList());
    }

    Optional<LogicalOlapTableStreamScan> createDeltaScan(LogicalOlapScan scan) {
        if (isExcluded(scan)) {
            return Optional.empty();
        }
        OlapTable originTable = (OlapTable) scan.getTable();
        LogicalOlapScan restricted = restrictWindow(scan);
        List<Long> partitionIds = restricted.getSelectedPartitionIds();
        if (!includeExhaustedStreams && cloudReadStates == null && !hasPendingData(getStream(scan), partitionIds)) {
            return Optional.empty();
        }
        List<Long> tabletIds = scan.getSelectedTabletIds();
        if (!tabletIds.isEmpty() && !partitionIds.equals(scan.getSelectedPartitionIds())) {
            // The partition selection was narrowed by the window; narrow the tablet
            // selection to the window partitions' tablets as well.
            Set<Long> windowTabletIds = new HashSet<>();
            for (Long partitionId : partitionIds) {
                Partition partition = originTable.getPartition(partitionId);
                if (partition != null && partition.getIndex(originTable.getBaseIndexId()) != null) {
                    windowTabletIds.addAll(partition.getIndex(originTable.getBaseIndexId()).getTabletIdsInOrder());
                }
            }
            tabletIds = tabletIds.stream()
                    .filter(windowTabletIds::contains)
                    .collect(Collectors.toList());
        }
        OlapTableStreamWrapper streamWrapper = createStreamWrapper(restricted);
        if (!includeExhaustedStreams && streamWrapper.hasCloudReadStates() && !streamWrapper.hasPendingCloudData()) {
            return Optional.empty();
        }
        return Optional.of(new LogicalOlapTableStreamScan(
                StatementScopeIdGenerator.newRelationId(),
                streamWrapper,
                scan.getQualifier(),
                partitionIds,
                tabletIds,
                scan.getHints(),
                scan.getTableSample(),
                scan.getOperativeSlots()));
    }

    OlapTableStreamWrapper createStreamWrapper(LogicalOlapScan scan) {
        return snapshotContext(scan).wrap(scan.getTable(), scan.getSelectedPartitionIds());
    }

    OlapSnapshotContext snapshotContext(LogicalOlapScan scan) {
        OlapTableStream stream = getStream(scan);
        List<Long> partitionIds = scan.getSelectedPartitionIds();
        if (cloudReadStates == null) {
            return new OlapSnapshotContext(stream, Optional.empty());
        }
        ImmutableMap.Builder<Long, Cloud.TableStreamPartitionReadStatePB> states = ImmutableMap.builder();
        if (!partitionIds.isEmpty()) {
            Map<Long, Cloud.TableStreamPartitionReadStatePB> streamStates =
                    cloudReadStates.get(stream.getCloudIdentity());
            for (long partitionId : partitionIds) {
                states.put(partitionId, streamStates.get(partitionId));
            }
        }
        return new OlapSnapshotContext(stream, Optional.of(states.build()));
    }

    boolean isExcluded(LogicalOlapScan scan) {
        return !streams.containsKey(scan.getTable());
    }

    OlapTableStream getStream(LogicalOlapScan scan) {
        OlapTableStream stream = streams.get(scan.getTable());
        if (stream == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM: missing delta scan context for " + scan.getTable().getName());
        }
        return stream;
    }

    int nextDeltaIndex() {
        sequenceEncoding();
        int index = nextDeltaScanIndex++;
        if (index >= 1024) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM: too many delta scans for sequence encoding: " + index);
        }
        return index;
    }

    Literal toSequence(int deltaIndex) {
        return sequenceEncoding().encode(deltaIndex, BigInteger.ZERO, true);
    }

    Expression toSequenceByDmlFactor(Expression dmlFactor, int deltaIndex) {
        return sequenceEncoding().encodeByDmlFactor(dmlFactor, deltaIndex);
    }

    /**
     * The sequence encoding of this delta rewrite. Only a unique-key MV's delta has one: an append-only
     * detail MV's rows carry no sign, and nothing about them needs ordering.
     */
    private IvmSequenceCalculator sequenceEncoding() {
        Preconditions.checkState(sequenceCalculator != null,
                "an append-only detail MV's delta allocates no sequence");
        return sequenceCalculator;
    }

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
}

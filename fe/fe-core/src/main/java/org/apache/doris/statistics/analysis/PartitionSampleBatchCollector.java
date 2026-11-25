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

package org.apache.doris.statistics.analysis;

import org.apache.doris.analysis.FixedBlockSampleSpec;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.analysis.TableStatisticsSqlBuilder.BatchedPartitionColumnSpec;
import org.apache.doris.statistics.sampling.ColumnPhysicalLayout;
import org.apache.doris.statistics.sampling.RandomAggTabletSamplingMethod;
import org.apache.doris.statistics.sampling.SampledScanSqlRenderer;
import org.apache.doris.statistics.sampling.SamplingBudget;
import org.apache.doris.statistics.sampling.TabletSampleSpec;
import org.apache.doris.statistics.sampling.TabletSampleSpec.EqualProbabilitySamplingSemantics;
import org.apache.doris.statistics.sampling.TabletSampleSpec.FullScanClause;
import org.apache.doris.statistics.sampling.TabletSampleSpec.KeyHashScanClause;
import org.apache.doris.statistics.sampling.TabletSamplingMethod;
import org.apache.doris.statistics.sampling.TabletSamplingRequest;
import org.apache.doris.statistics.sampling.TabletSamplingScope;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

/**
 * Shares a single partition-scoped sampled OLAP scan across up to {@code batchSize} columns of the
 * same index, writing {@code (ndv HLL, unseen_ndv BIGINT)} rows directly into
 * {@code __internal_schema.partition_statistics}.
 */
final class PartitionSampleBatchCollector {
    private static final Logger LOG = LogManager.getLogger(PartitionSampleBatchCollector.class);

    private enum State {
        PENDING, RUNNING, SUCCEEDED, SKIPPED, FALLBACK, CANCELLED
    }

    private record Target(long jobId, long indexId, Set<String> partitions) {
    }

    private record PlannedColumn(
            int ord,
            String name,
            Column column,
            ColumnPhysicalLayout layout,
            boolean singleUniqueKey,
            boolean singleNonNullUnique) {
    }

    private static final class PartitionRun {
        State state = State.PENDING;
        String skipReason;
        StmtExecutor activeExecutor;
    }

    private final List<Batch> batches = new ArrayList<>();

    PartitionSampleBatchCollector(Collection<? extends BaseAnalysisTask> tasks, int batchSize) {
        if (batchSize <= 1) {
            return;
        }
        Map<Target, Map<String, List<OlapAnalysisTask>>> groups = new HashMap<>();
        for (BaseAnalysisTask task : tasks) {
            if (!(task instanceof OlapAnalysisTask olapTask) || task.info == null
                    || !(task.tbl instanceof OlapTable olapTable) || !olapTable.isPartitionedTable()) {
                continue;
            }
            if (!task.info.partitionOnly && !StatisticsUtil.enablePartitionAnalyze()) {
                continue;
            }
            Set<String> partNames = task.info.partitionNames == null ? Set.of() : Set.copyOf(task.info.partitionNames);
            Target target = new Target(task.info.jobId, task.info.indexId, partNames);
            groups.computeIfAbsent(target, key -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
                    .computeIfAbsent(task.info.colName, key -> new ArrayList<>()).add(olapTask);
        }
        groups.forEach((target, byName) -> {
            for (List<String> names : Lists.partition(new ArrayList<>(byName.keySet()), batchSize)) {
                if (names.size() < 2) {
                    continue;
                }
                List<OlapAnalysisTask> members = names.stream().flatMap(name -> byName.get(name).stream())
                        .collect(Collectors.toList());
                Batch batch = new Batch(target, names, members);
                batches.add(batch);
                members.forEach(task -> task.partitionSampleBatch = batch);
            }
        });
    }

    final class Batch {
        private final Target target;
        private final List<String> columns;
        private final List<OlapAnalysisTask> memberTasks;
        private final Map<Long, PartitionRun> runs = new HashMap<>();
        private boolean cancelled;

        private Batch(Target target, List<String> columns, List<OlapAnalysisTask> memberTasks) {
            this.target = target;
            this.columns = List.copyOf(columns);
            this.memberTasks = List.copyOf(memberTasks);
        }

        boolean collectPartition(OlapAnalysisTask caller, OlapTable table, long resolvedIndexId,
                Partition partition, TabletSamplingMethod method, long partRows, long targetSampleRows,
                boolean logicalSample) throws InterruptedException {
            long partId = partition.getId();
            while (true) {
                PartitionRun run;
                synchronized (PartitionSampleBatchCollector.this) {
                    checkCaller(caller);
                    run = runs.computeIfAbsent(partId, id -> new PartitionRun());
                    if (run.state == State.FALLBACK) {
                        return false;
                    }
                    if (run.state == State.SKIPPED) {
                        throw new AnalyzeSkipException(run.skipReason);
                    }
                    if (run.state == State.SUCCEEDED) {
                        return true;
                    }
                    if (run.state == State.RUNNING) {
                        PartitionSampleBatchCollector.this.wait();
                        continue;
                    }
                    run.state = State.RUNNING;
                }
                execute(run, caller, table, resolvedIndexId, partition, method, partRows,
                        targetSampleRows, logicalSample);
            }
        }

        private void checkCaller(OlapAnalysisTask caller) throws InterruptedException {
            caller.checkMinMaxCancelled();
            if (cancelled) {
                throw new CancellationException("Partition sample batch cancelled");
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Partition sample batch interrupted");
            }
        }

        private void execute(PartitionRun run, OlapAnalysisTask caller, OlapTable table, long resolvedIndexId,
                Partition partition, TabletSamplingMethod method, long partRows, long targetSampleRows,
                boolean logicalSample) throws InterruptedException {
            State next = State.FALLBACK;
            String skipped = null;
            try {
                boolean ok = runPartitionBatchInsert(run, caller, table, resolvedIndexId, partition,
                        method, partRows, targetSampleRows, logicalSample);
                next = ok ? State.SUCCEEDED : State.FALLBACK;
            } catch (AnalyzeSkipException e) {
                skipped = e.getMessage();
                next = State.SKIPPED;
            } catch (Exception e) {
                synchronized (PartitionSampleBatchCollector.this) {
                    checkCaller(caller);
                }
                if (Throwables.getRootCause(e) instanceof InterruptedException interrupted) {
                    throw interrupted;
                }
                if (BaseAnalysisTask.containsKeyRangeSchemaSkipMarker(e)) {
                    skipped = "Deferring statistics sampling while key schema change segments compact";
                    next = State.SKIPPED;
                } else {
                    LOG.warn("Batch partition sample failed for job {}, index {}, partition {}, columns {}. "
                            + "Falling back to per-column.",
                            target.jobId, target.indexId, partition.getName(), columns, e);
                    next = State.FALLBACK;
                }
            } finally {
                synchronized (PartitionSampleBatchCollector.this) {
                    run.activeExecutor = null;
                    if (!cancelled && run.state == State.RUNNING) {
                        run.skipReason = skipped;
                        run.state = next;
                    }
                    PartitionSampleBatchCollector.this.notifyAll();
                }
            }
        }

        private boolean runPartitionBatchInsert(PartitionRun run, OlapAnalysisTask caller, OlapTable table,
                long resolvedIndexId, Partition partition, TabletSamplingMethod method, long partRows,
                long targetSampleRows, boolean logicalSample) throws Exception {
            List<PlannedColumn> planned = new ArrayList<>(columns.size());
            List<String> keyColumnNames;
            boolean uniqueAcrossTablets;
            boolean anyKey = false;
            boolean anyPartition = false;
            boolean anyDistribution = false;
            table.readLock();
            try {
                MaterializedIndexMeta meta = table.getIndexMetaByIndexId(resolvedIndexId);
                if (meta == null) {
                    return false;
                }
                uniqueAcrossTablets = resolvedIndexId == table.getBaseIndexId()
                        || FixedBlockSampleSpec.guaranteesGlobalKeyUniqueness(table, meta, resolvedIndexId);
                keyColumnNames = meta.getKeysType() == KeysType.AGG_KEYS
                        ? meta.getSchema().stream().filter(Column::isKey).map(Column::getName).toList()
                        : List.of();
                Map<String, Boolean> seen = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (OlapAnalysisTask member : memberTasks) {
                    if (!member.killed && member.info != null) {
                        seen.put(member.info.colName, Boolean.TRUE);
                    }
                }
                Set<String> liveNames = seen.keySet();
                for (String name : columns) {
                    if (!liveNames.contains(name)) {
                        continue;
                    }
                    Column col = OlapAnalysisTask.resolveColumn(meta, name);
                    if (col == null) {
                        continue;
                    }
                    ColumnPhysicalLayout layout = OlapAnalysisTask.columnLayout(table, meta, col);
                    boolean singleUnique = layout.singleUniqueKey() && uniqueAcrossTablets;
                    boolean singleNonNullUnique = singleUnique && !col.isAllowNull();
                    anyKey |= layout.key();
                    anyPartition |= layout.partition();
                    anyDistribution |= layout.distribution();
                    planned.add(new PlannedColumn(planned.size(), name, col, layout,
                            singleUnique, singleNonNullUnique));
                }
            } finally {
                table.readUnlock();
            }
            if (planned.size() < 2) {
                return false;
            }
            String partName = partition.getName();
            long partId = partition.getId();
            String baseFromPrefix = caller.partitionFromPrefix(partName);
            List<BatchedPartitionColumnSpec> measureSpecs = buildSpecs(planned, false, null);
            int baseSqlBytes = TableStatisticsSqlBuilder.batchedPartitionSampleInsertSql(
                    baseFromPrefix, caller.catalog.getId(), caller.db.getId(), caller.tbl.getId(),
                    caller.info.indexId, StatisticsUtil.escapeSQL(partName), partId, false, logicalSample,
                    0.00000000000000001, Math.max(1L, partRows), measureSpecs)
                    .getBytes(StandardCharsets.UTF_8).length;
            int whereReserve = method instanceof RandomAggTabletSamplingMethod
                    ? SampledScanSqlRenderer.renderKeyHashWhere(
                            new KeyHashScanClause(Optional.empty(), Long.MAX_VALUE, 1), keyColumnNames)
                            .getBytes(StandardCharsets.UTF_8).length
                    : 0;
            int clauseBytes = OlapAnalysisTask.MAX_INTERNAL_SQL_BYTES - baseSqlBytes - whereReserve - 64;
            if (clauseBytes < 256) {
                return false;
            }
            ColumnPhysicalLayout batchLayout = new ColumnPhysicalLayout(anyKey, anyPartition, false, anyDistribution);
            SamplingBudget budget = new SamplingBudget(targetSampleRows,
                    StatisticsUtil.getStatisticsSampleUnitRows(),
                    StatisticsUtil.getStatisticsMaxSampleTablets(), 4, clauseBytes);
            TabletSamplingScope scope = caller.job.partitionSamplingFrame(table, resolvedIndexId, partId).scope();
            long seedSeek = caller.tableSample == null ? -1L : caller.tableSample.getSeek();
            TabletSamplingRequest request = new TabletSamplingRequest(scope, batchLayout, budget,
                    Math.max(1L, partRows), caller.job.samplingSeed(seedSeek) ^ partId);
            TabletSampleSpec spec = method.build(request);
            boolean census = spec.scanClause() instanceof FullScanClause;
            EqualProbabilitySamplingSemantics sem = census ? null
                    : (EqualProbabilitySamplingSemantics) spec.semantics();
            String sampleHints = census ? ""
                    : SampledScanSqlRenderer.render(spec.scanClause(), OlapAnalysisTask.MAX_INTERNAL_SQL_BYTES);
            String sampleWhere = census ? ""
                    : SampledScanSqlRenderer.renderKeyHashWhere(spec.scanClause(), keyColumnNames);
            String fullFrom = baseFromPrefix
                    + (sampleHints.isEmpty() ? "" : " " + sampleHints)
                    + sampleWhere;
            List<BatchedPartitionColumnSpec> finalSpecs = buildSpecs(planned, census, sem);
            String sql = SampledScanSqlRenderer.checked(
                    TableStatisticsSqlBuilder.batchedPartitionSampleInsertSql(
                            fullFrom, caller.catalog.getId(), caller.db.getId(), caller.tbl.getId(),
                            caller.info.indexId, StatisticsUtil.escapeSQL(partName), partId, census,
                            logicalSample && !census, census ? 1.0 : sem.probability(),
                            Math.max(1L, partRows), finalSpecs),
                    OlapAnalysisTask.MAX_INTERNAL_SQL_BYTES);

            try (AutoCloseConnectContext ctx = StatisticsUtil.buildConnectContext(false)) {
                ctx.connectContext.getState().setPlanWithUnKnownColumnStats(true);
                BaseAnalysisTask.configureStatsCte(ctx.connectContext.getSessionVariable());
                BaseAnalysisTask.configureJobParallelism(ctx.connectContext.getSessionVariable(), caller.info);
                StmtExecutor executor = new StmtExecutor(ctx.connectContext, sql);
                synchronized (PartitionSampleBatchCollector.this) {
                    checkCaller(caller);
                    run.activeExecutor = executor;
                }
                try {
                    executor.execute();
                    QueryState queryState = executor.getContext().getState();
                    if (queryState.getStateType().equals(QueryState.MysqlStateType.ERR)) {
                        throw new RuntimeException("Failed to insert partition sample stats: "
                                + queryState.getErrorMessage());
                    }
                } finally {
                    AuditLogHelper.logAuditLog(executor.getContext(), executor.getOriginStmt().toString(),
                            executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog(), true);
                }
            }

            List<String> updatedColumns = planned.stream().map(PlannedColumn::name).toList();
            Env.getServingEnv().getAnalysisManager().updatePartitionStatsCacheForColumns(
                    caller.info.catalogId, caller.info.dbId, caller.info.tblId, caller.info.indexId,
                    Set.of(partName), updatedColumns);
            return true;
        }

        private List<BatchedPartitionColumnSpec> buildSpecs(List<PlannedColumn> planned, boolean census,
                EqualProbabilitySamplingSemantics sem) {
            List<BatchedPartitionColumnSpec> specs = new ArrayList<>(planned.size());
            for (PlannedColumn pc : planned) {
                Column col = pc.column();
                double domRate = sem == null
                        ? (census ? 1.0 : 0.00000000000000001)
                        : OlapAnalysisTask.subdomainRate(pc.layout(), pc.singleUniqueKey(), sem);
                specs.add(new BatchedPartitionColumnSpec(
                        pc.ord(),
                        StatisticsUtil.escapeSQL(pc.name()),
                        SqlUtils.getIdentSql(col.getName()),
                        col.getType().isStringType(),
                        col.getType().getSlotSize(),
                        pc.singleNonNullUnique(),
                        domRate));
            }
            return specs;
        }

        void onTaskCancelled() {
            List<StmtExecutor> toCancel = new ArrayList<>();
            synchronized (PartitionSampleBatchCollector.this) {
                if (!cancelled && memberTasks.stream().allMatch(t -> t.killed)) {
                    toCancel.addAll(cancelLocked());
                }
                PartitionSampleBatchCollector.this.notifyAll();
            }
            toCancel.forEach(PartitionSampleBatchCollector::cancelQuery);
        }

        private List<StmtExecutor> cancelLocked() {
            cancelled = true;
            List<StmtExecutor> executors = new ArrayList<>();
            for (PartitionRun run : runs.values()) {
                if (run.state == State.PENDING || run.state == State.RUNNING) {
                    run.state = State.CANCELLED;
                    if (run.activeExecutor != null) {
                        executors.add(run.activeExecutor);
                        run.activeExecutor = null;
                    }
                }
            }
            return executors;
        }
    }

    void cancelAll() {
        List<StmtExecutor> toCancel = new ArrayList<>();
        synchronized (this) {
            for (Batch batch : batches) {
                toCancel.addAll(batch.cancelLocked());
            }
            notifyAll();
        }
        toCancel.forEach(PartitionSampleBatchCollector::cancelQuery);
    }

    private static void cancelQuery(StmtExecutor executor) {
        try {
            executor.cancel(Status.CANCELLED);
        } catch (RuntimeException e) {
            LOG.warn("Failed to cancel a batched partition sample analysis query", e);
        }
    }
}

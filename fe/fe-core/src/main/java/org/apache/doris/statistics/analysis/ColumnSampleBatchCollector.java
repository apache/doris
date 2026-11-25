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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.analysis.TableStatisticsSqlBuilder.BatchedColumnSpec;
import org.apache.doris.statistics.repository.ResultRow;
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
import org.apache.doris.statistics.util.SubcolumnStatisticsUtil;

import com.google.common.base.Preconditions;
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
 * Shares a single sampled OLAP scan (MultiCastDataStreamSink CTE) across multiple columns of the
 * same index, avoiding repeated storage scans, key merges, Delete Bitmap checks, and key-range RPCs.
 */
final class ColumnSampleBatchCollector {
    private static final Logger LOG = LogManager.getLogger(ColumnSampleBatchCollector.class);

    private enum State {
        PENDING, RUNNING, SUCCEEDED, SKIPPED, FALLBACK, CANCELLED
    }

    private record Target(long jobId, long indexId, Set<String> partitions) {
    }

    record ColumnSampleOutcome(
            boolean overLimit,
            ResultRow row,
            double sampleProbability,
            boolean singleNonNullUnique,
            double domainRate,
            boolean inSampleBounds,
            boolean fullScanOfNonNullUniqueKey) {
    }

    private record PlannedColumn(
            int ord,
            String name,
            String sqlExpr,
            Column column,
            ColumnPhysicalLayout layout,
            boolean singleUniqueKey,
            boolean singleNonNullUnique,
            boolean inSampleBounds) {
    }

    private final List<Batch> batches = new ArrayList<>();

    ColumnSampleBatchCollector(Collection<? extends BaseAnalysisTask> tasks, int batchSize) {
        if (batchSize <= 1) {
            return;
        }
        Map<Target, Map<String, List<OlapAnalysisTask>>> groups = new HashMap<>();
        for (BaseAnalysisTask task : tasks) {
            if (!(task instanceof OlapAnalysisTask olapTask) || task.tableSample == null
                    || task.info == null || task.info.rowCount == 0 || !(task.tbl instanceof OlapTable)
                    || olapTask.usesPartitionTable()) {
                continue;
            }
            Set<String> partNames = task.info.partitionNames == null ? Set.of() : Set.copyOf(task.info.partitionNames);
            Target target = new Target(task.info.jobId, task.info.indexId, partNames);
            groups.computeIfAbsent(target, key -> new TreeMap<>())
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
                members.forEach(task -> task.sampleBatch = batch);
            }
        });
    }

    final class Batch {
        private final Target target;
        private final List<String> columns;
        private final List<OlapAnalysisTask> memberTasks;
        // All mutable fields are guarded by the collector's monitor.
        private State state = State.PENDING;
        private Map<String, ColumnSampleOutcome> outcomes;
        private String skipReason;
        private StmtExecutor activeExecutor;

        private Batch(Target target, List<String> columns, List<OlapAnalysisTask> memberTasks) {
            this.target = target;
            this.columns = List.copyOf(columns);
            this.memberTasks = List.copyOf(memberTasks);
        }

        ColumnSampleOutcome collect(OlapAnalysisTask caller, OlapTable table, long resolvedIndexId,
                TabletSamplingMethod method, long rows, boolean logicalSample) throws InterruptedException {
            while (true) {
                synchronized (ColumnSampleBatchCollector.this) {
                    checkCaller(caller);
                    if (state == State.FALLBACK) {
                        return null;
                    }
                    if (state == State.SKIPPED) {
                        throw new AnalyzeSkipException(skipReason);
                    }
                    if (state == State.SUCCEEDED) {
                        ColumnSampleOutcome outcome = outcomes.get(caller.info.colName);
                        if (outcome == null) {
                            return null;
                        }
                        return adaptForCaller(outcome, caller);
                    }
                    if (state == State.RUNNING) {
                        ColumnSampleBatchCollector.this.wait();
                        continue;
                    }
                    state = State.RUNNING;
                }
                execute(caller, table, resolvedIndexId, method, rows, logicalSample);
            }
        }

        private ColumnSampleOutcome adaptForCaller(ColumnSampleOutcome outcome, OlapAnalysisTask caller) {
            if (outcome.overLimit()) {
                return outcome;
            }
            List<String> values = new ArrayList<>(outcome.row().getValues());
            values.set(0, caller.concatColumnStatsId());
            values.set(5, caller.info.colName);
            return new ColumnSampleOutcome(false, new ResultRow(values), outcome.sampleProbability(),
                    outcome.singleNonNullUnique(), outcome.domainRate(), outcome.inSampleBounds(),
                    outcome.fullScanOfNonNullUniqueKey());
        }

        private void checkCaller(OlapAnalysisTask caller) throws InterruptedException {
            caller.checkMinMaxCancelled();
            if (state == State.CANCELLED) {
                throw new CancellationException("Sample batch cancelled");
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Sample batch interrupted");
            }
        }

        private void execute(OlapAnalysisTask caller, OlapTable table, long resolvedIndexId,
                TabletSamplingMethod method, long rows, boolean logicalSample) throws InterruptedException {
            State next = State.PENDING;
            Map<String, ColumnSampleOutcome> computed = null;
            String skipped = null;
            try {
                computed = runBatchQuery(caller, table, resolvedIndexId, method, rows, logicalSample);
                next = computed == null ? State.FALLBACK : State.SUCCEEDED;
            } catch (AnalyzeSkipException e) {
                skipped = e.getMessage();
                next = State.SKIPPED;
            } catch (RuntimeException e) {
                synchronized (ColumnSampleBatchCollector.this) {
                    checkCaller(caller);
                }
                if (Throwables.getRootCause(e) instanceof InterruptedException interrupted) {
                    throw interrupted;
                }
                if (BaseAnalysisTask.containsKeyRangeSchemaSkipMarker(e)) {
                    skipped = "Deferring statistics sampling while key schema change segments compact";
                    next = State.SKIPPED;
                } else if (BaseAnalysisTask.containsSkipMarker(e)) {
                    next = State.FALLBACK;
                } else {
                    LOG.warn("Batch sample scan failed for job {}, index {}, columns {}. Falling back to per-column.",
                            target.jobId, target.indexId, columns, e);
                    next = State.FALLBACK;
                }
            } finally {
                synchronized (ColumnSampleBatchCollector.this) {
                    activeExecutor = null;
                    if (state == State.RUNNING) {
                        outcomes = computed;
                        skipReason = skipped;
                        state = next;
                    }
                    ColumnSampleBatchCollector.this.notifyAll();
                }
            }
        }

        private Map<String, ColumnSampleOutcome> runBatchQuery(OlapAnalysisTask caller, OlapTable table,
                long resolvedIndexId, TabletSamplingMethod method, long rows, boolean logicalSample)
                throws InterruptedException {
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
                    return null;
                }
                uniqueAcrossTablets = resolvedIndexId == table.getBaseIndexId()
                        || FixedBlockSampleSpec.guaranteesGlobalKeyUniqueness(table, meta, resolvedIndexId);
                keyColumnNames = meta.getKeysType() == KeysType.AGG_KEYS
                        ? meta.getSchema().stream().filter(Column::isKey).map(Column::getName).toList()
                        : List.of();
                Map<String, Boolean> seen = new TreeMap<>();
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
                    Column col = OlapAnalysisTask.resolveColumn(meta, table, name);
                    if (col == null) {
                        continue;
                    }
                    String sqlExpr = SubcolumnStatisticsUtil.resolveSqlExpr(meta, table, name, col);
                    ColumnPhysicalLayout layout = OlapAnalysisTask.columnLayout(table, meta, col);
                    boolean singleUnique = layout.singleUniqueKey() && uniqueAcrossTablets;
                    boolean singleNonNullUnique = singleUnique && !col.isAllowNull();
                    boolean inSampleBounds = OlapAnalysisTask.prefersInSampleBounds(
                            table, meta, col, resolvedIndexId, caller.info);
                    anyKey |= layout.key();
                    anyPartition |= layout.partition();
                    anyDistribution |= layout.distribution();
                    planned.add(new PlannedColumn(planned.size(), name, sqlExpr, col, layout,
                            singleUnique, singleNonNullUnique, inSampleBounds));
                }
            } finally {
                table.readUnlock();
            }
            if (planned.size() < 2) {
                return null;
            }
            long population = logicalSample ? rows : caller.job.freezeIndexRows(resolvedIndexId, () -> rows);
            if (!logicalSample && population <= 0) {
                return null;
            }
            int hotCount = SessionVariable.getHotValueCollectCount();
            long maxStrCfg = Config.statistics_max_string_column_length;
            String tableRef = caller.qualifiedTableName();
            String indexClause = caller.getIndex();
            String baseFromPrefix = indexClause.isEmpty() ? tableRef : (tableRef + " " + indexClause);
            List<BatchedColumnSpec> measureSpecs = buildSpecs(caller, planned, false, logicalSample,
                    population, maxStrCfg, null);
            int baseSqlBytes = TableStatisticsSqlBuilder.batchedSampleSql(baseFromPrefix, measureSpecs, hotCount)
                    .getBytes(StandardCharsets.UTF_8).length;
            int whereReserve = method instanceof RandomAggTabletSamplingMethod
                    ? SampledScanSqlRenderer.renderKeyHashWhere(
                            new KeyHashScanClause(Optional.empty(), Long.MAX_VALUE, 1), keyColumnNames)
                            .getBytes(StandardCharsets.UTF_8).length
                    : 0;
            int clauseBytes = OlapAnalysisTask.MAX_INTERNAL_SQL_BYTES - baseSqlBytes - whereReserve - 64;
            if (clauseBytes < 256) {
                return null;
            }
            ColumnPhysicalLayout batchLayout = new ColumnPhysicalLayout(anyKey, anyPartition, false, anyDistribution);
            SamplingBudget budget = new SamplingBudget(caller.targetSampleRowsForBatch(rows),
                    StatisticsUtil.getStatisticsSampleUnitRows(),
                    StatisticsUtil.getStatisticsMaxSampleTablets(), 4, clauseBytes);
            TabletSamplingScope scope = caller.job.samplingFrame(table, resolvedIndexId).scope();
            TabletSamplingRequest request = new TabletSamplingRequest(scope, batchLayout, budget, rows,
                    caller.job.samplingSeed(caller.tableSample.getSeek()));
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
            List<BatchedColumnSpec> finalSpecs = buildSpecs(caller, planned, census, logicalSample && !census,
                    population, maxStrCfg, sem);
            String sql = SampledScanSqlRenderer.checked(
                    TableStatisticsSqlBuilder.batchedSampleSql(fullFrom, finalSpecs, hotCount),
                    OlapAnalysisTask.MAX_INTERNAL_SQL_BYTES);
            List<ResultRow> resultRows;
            try (AutoCloseConnectContext ctx = StatisticsUtil.buildConnectContext(false)) {
                ctx.connectContext.getState().setPlanWithUnKnownColumnStats(true);
                BaseAnalysisTask.configureStatsCte(ctx.connectContext.getSessionVariable());
                BaseAnalysisTask.configureJobParallelism(ctx.connectContext.getSessionVariable(), caller.info);
                StmtExecutor executor = new StmtExecutor(ctx.connectContext, sql);
                synchronized (ColumnSampleBatchCollector.this) {
                    checkCaller(caller);
                    activeExecutor = executor;
                }
                resultRows = executor.executeInternalQuery();
            }
            Preconditions.checkState(resultRows.size() == planned.size(),
                    "Expected %s batched sample rows, got %s", planned.size(), resultRows.size());
            Map<String, ColumnSampleOutcome> byCol = new TreeMap<>();
            for (ResultRow r : resultRows) {
                Preconditions.checkState(r.getValues().size() == 18,
                        "Expected 18 columns in batched sample row, got %s", r.getValues().size());
                int ord = Integer.parseInt(r.get(0));
                Preconditions.checkState(ord >= 0 && ord < planned.size(), "Invalid column ordinal %s", ord);
                PlannedColumn pc = planned.get(ord);
                boolean overLimit = Integer.parseInt(r.get(1)) != 0;
                boolean raw = logicalSample && !census;
                ResultRow trimmed = new ResultRow(new ArrayList<>(r.getValues().subList(2, raw ? 18 : 17)));
                double prob = raw ? sem.probability() : 0.0;
                double domRate = census ? 1.0 : OlapAnalysisTask.subdomainRate(pc.layout(), pc.singleUniqueKey(), sem);
                boolean effectiveInSampleBounds = census || pc.inSampleBounds();
                byCol.put(pc.name(), new ColumnSampleOutcome(overLimit, trimmed, prob,
                        pc.singleNonNullUnique(), domRate, effectiveInSampleBounds,
                        census && pc.singleNonNullUnique()));
            }
            Preconditions.checkState(byCol.size() == planned.size(), "Missing columns in batched sample result");
            return byCol;
        }

        private List<BatchedColumnSpec> buildSpecs(OlapAnalysisTask caller, List<PlannedColumn> planned,
                boolean census, boolean raw, long population, long maxStrCfg,
                EqualProbabilitySamplingSemantics sem) {
            long catalogId = caller.catalog.getId();
            long dbId = caller.db.getId();
            long tblId = caller.tbl.getId();
            long idxId = caller.info.indexId;
            List<BatchedColumnSpec> specs = new ArrayList<>(planned.size());
            for (PlannedColumn pc : planned) {
                Column col = pc.column();
                boolean str = col.getType().isStringType();
                long maxLen = str && maxStrCfg > 0 ? maxStrCfg : -1L;
                double domRate = sem == null ? 0.00000000000000001
                        : OlapAnalysisTask.subdomainRate(pc.layout(), pc.singleUniqueKey(), sem);
                specs.add(new BatchedColumnSpec(
                        pc.ord(),
                        catalogId,
                        dbId,
                        tblId,
                        idxId,
                        StatisticsUtil.escapeSQL(pc.name()),
                        pc.sqlExpr(),
                        str,
                        col.getType().getSlotSize(),
                        pc.singleUniqueKey(),
                        pc.singleNonNullUnique(),
                        raw,
                        census,
                        census || pc.inSampleBounds(),
                        domRate,
                        population,
                        maxLen));
            }
            return specs;
        }

        void onTaskCancelled() {
            StmtExecutor executor = null;
            synchronized (ColumnSampleBatchCollector.this) {
                if (state != State.PENDING && state != State.RUNNING) {
                    return;
                }
                if (memberTasks.stream().allMatch(t -> t.killed)) {
                    executor = cancelLocked();
                }
                ColumnSampleBatchCollector.this.notifyAll();
            }
            if (executor != null) {
                cancelQuery(executor);
            }
        }

        private StmtExecutor cancelLocked() {
            if (state == State.PENDING || state == State.RUNNING) {
                state = State.CANCELLED;
                StmtExecutor executor = activeExecutor;
                activeExecutor = null;
                return executor;
            }
            return null;
        }
    }

    void cancelAll() {
        List<StmtExecutor> toCancel = new ArrayList<>();
        synchronized (this) {
            for (Batch batch : batches) {
                StmtExecutor executor = batch.cancelLocked();
                if (executor != null) {
                    toCancel.add(executor);
                }
            }
            notifyAll();
        }
        toCancel.forEach(ColumnSampleBatchCollector::cancelQuery);
    }

    private static void cancelQuery(StmtExecutor executor) {
        try {
            executor.cancel(Status.CANCELLED);
        } catch (RuntimeException e) {
            LOG.warn("Failed to cancel a batched sample analysis query", e);
        }
    }
}

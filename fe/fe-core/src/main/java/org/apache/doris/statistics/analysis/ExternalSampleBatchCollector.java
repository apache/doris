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

import org.apache.doris.catalog.Column;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.analysis.PluginDrivenSampleAnalysisTask.SamplePlan;
import org.apache.doris.statistics.analysis.TableStatisticsSqlBuilder.BatchedExternalColumnSpec;
import org.apache.doris.statistics.repository.ResultRow;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

/**
 * Shares one external {@code TABLESAMPLE} CTE scan ({@code MultiCastDataStreamSink}) across up to
 * {@code statistics_sample_batch_columns} columns of a plugin-driven external table (Hive, Iceberg, Paimon),
 * avoiding repeated split planning and file scans per column.
 */
final class ExternalSampleBatchCollector {
    private static final Logger LOG = LogManager.getLogger(ExternalSampleBatchCollector.class);

    private enum State {
        PENDING, RUNNING, SUCCEEDED, FALLBACK, CANCELLED
    }

    private record Target(long jobId, long catalogId, long dbId, long tblId) {
    }

    record ExternalColumnOutcome(boolean overLimit, ResultRow row) {
    }

    private record PlannedColumn(int ord, String name, String sqlExpr, Column column, boolean linearEstimator) {
    }

    private final List<Batch> batches = new ArrayList<>();

    ExternalSampleBatchCollector(Collection<? extends BaseAnalysisTask> tasks, int batchSize) {
        if (batchSize <= 1) {
            return;
        }
        Map<Target, Map<String, List<PluginDrivenSampleAnalysisTask>>> groups = new HashMap<>();
        for (BaseAnalysisTask task : tasks) {
            if (!(task instanceof PluginDrivenSampleAnalysisTask extTask)
                    || task.tableSample == null || task.info == null
                    || !(task.tbl instanceof ExternalTable)) {
                continue;
            }
            Target target = new Target(task.info.jobId, task.info.catalogId, task.info.dbId, task.info.tblId);
            groups.computeIfAbsent(target, key -> new TreeMap<>())
                    .computeIfAbsent(task.info.colName, key -> new ArrayList<>()).add(extTask);
        }
        groups.forEach((target, byName) -> {
            for (List<String> names : Lists.partition(new ArrayList<>(byName.keySet()), batchSize)) {
                if (names.size() < 2) {
                    continue;
                }
                List<PluginDrivenSampleAnalysisTask> members = names.stream()
                        .flatMap(name -> byName.get(name).stream())
                        .collect(Collectors.toList());
                Batch batch = new Batch(target, names, members);
                batches.add(batch);
                members.forEach(task -> task.externalSampleBatch = batch);
            }
        });
    }

    final class Batch {
        private final Target target;
        private final List<String> columns;
        private final List<PluginDrivenSampleAnalysisTask> memberTasks;
        private State state = State.PENDING;
        private Map<String, ExternalColumnOutcome> outcomes;
        private StmtExecutor activeExecutor;

        private Batch(Target target, List<String> columns, List<PluginDrivenSampleAnalysisTask> memberTasks) {
            this.target = target;
            this.columns = List.copyOf(columns);
            this.memberTasks = List.copyOf(memberTasks);
        }

        ExternalColumnOutcome collect(PluginDrivenSampleAnalysisTask caller) throws InterruptedException {
            while (true) {
                synchronized (ExternalSampleBatchCollector.this) {
                    checkCaller(caller);
                    if (state == State.FALLBACK) {
                        return null;
                    }
                    if (state == State.SUCCEEDED) {
                        ExternalColumnOutcome outcome = outcomes.get(caller.info.colName);
                        if (outcome == null) {
                            return null;
                        }
                        return adaptForCaller(outcome, caller);
                    }
                    if (state == State.RUNNING) {
                        ExternalSampleBatchCollector.this.wait();
                        continue;
                    }
                    state = State.RUNNING;
                }
                execute(caller);
            }
        }

        private ExternalColumnOutcome adaptForCaller(
                ExternalColumnOutcome outcome, PluginDrivenSampleAnalysisTask caller) {
            if (outcome.overLimit()) {
                return outcome;
            }
            List<String> values = new ArrayList<>(outcome.row().getValues());
            values.set(0, caller.tbl.getId() + "--1-" + caller.info.colName);
            values.set(5, caller.info.colName);
            return new ExternalColumnOutcome(false, new ResultRow(values));
        }

        private void checkCaller(PluginDrivenSampleAnalysisTask caller) throws InterruptedException {
            caller.checkCancelled();
            if (state == State.CANCELLED) {
                throw new CancellationException("External sample batch cancelled");
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("External sample batch interrupted");
            }
        }

        private void execute(PluginDrivenSampleAnalysisTask caller) throws InterruptedException {
            State next = State.FALLBACK;
            Map<String, ExternalColumnOutcome> computed = null;
            try {
                computed = runBatchQuery(caller);
                next = computed == null ? State.FALLBACK : State.SUCCEEDED;
            } catch (RuntimeException e) {
                synchronized (ExternalSampleBatchCollector.this) {
                    checkCaller(caller);
                }
                if (Throwables.getRootCause(e) instanceof InterruptedException interrupted) {
                    throw interrupted;
                }
                LOG.warn("Batched external sample scan failed for job {}, table {}, columns {}. "
                        + "Falling back to per-column.", target.jobId, target.tblId, columns, e);
                next = State.FALLBACK;
            } finally {
                synchronized (ExternalSampleBatchCollector.this) {
                    activeExecutor = null;
                    if (state == State.RUNNING) {
                        outcomes = computed;
                        state = next;
                    }
                    ExternalSampleBatchCollector.this.notifyAll();
                }
            }
        }

        private Map<String, ExternalColumnOutcome> runBatchQuery(PluginDrivenSampleAnalysisTask caller)
                throws InterruptedException {
            Map<String, Column> liveColumns = new TreeMap<>();
            for (PluginDrivenSampleAnalysisTask member : memberTasks) {
                if (!member.killed && member.info != null && member.col != null) {
                    liveColumns.putIfAbsent(member.info.colName, member.col);
                }
            }
            Set<String> distColumns = caller.tbl.getDistributionColumnNames();
            boolean singleDist = distColumns != null && distColumns.size() == 1;
            List<PlannedColumn> planned = new ArrayList<>(columns.size());
            for (String name : columns) {
                Column col = liveColumns.get(name);
                if (col == null) {
                    continue;
                }
                String sqlExpr = SubcolumnStatisticsUtil.resolveSqlExpr(caller.tbl, -1L, name, col);
                boolean linear = singleDist && distColumns.contains(col.getName().toLowerCase());
                planned.add(new PlannedColumn(planned.size(), name, sqlExpr, col, linear));
            }
            if (planned.size() < 2) {
                return null;
            }
            SamplePlan samplePlan = caller.planExternalSample();
            String fromClause = caller.qualifiedExternalFromClause(samplePlan);
            long maxStrCfg = Config.statistics_max_string_column_length;
            long catalogId = caller.catalog.getId();
            long dbId = caller.db.getId();
            long tblId = caller.tbl.getId();
            List<BatchedExternalColumnSpec> specs = new ArrayList<>(planned.size());
            for (PlannedColumn pc : planned) {
                Column col = pc.column();
                boolean str = col.getType().isStringType();
                long maxLen = str && maxStrCfg > 0 ? maxStrCfg : -1L;
                specs.add(new BatchedExternalColumnSpec(
                        pc.ord(),
                        catalogId,
                        dbId,
                        tblId,
                        StatisticsUtil.escapeSQL(pc.name()),
                        pc.sqlExpr(),
                        str,
                        col.getType().getSlotSize(),
                        pc.linearEstimator(),
                        maxLen));
            }
            int hotCount = SessionVariable.getHotValueCollectCount();
            String sql = TableStatisticsSqlBuilder.batchedExternalSampleSql(
                    fromClause, specs, samplePlan.scaleFactor(), hotCount);
            if (sql.getBytes(StandardCharsets.UTF_8).length > OlapAnalysisTask.MAX_INTERNAL_SQL_BYTES) {
                return null;
            }
            List<ResultRow> resultRows;
            try (AutoCloseConnectContext ctx = StatisticsUtil.buildConnectContext(false)) {
                ctx.connectContext.getState().setPlanWithUnKnownColumnStats(true);
                BaseAnalysisTask.configureStatsCte(ctx.connectContext.getSessionVariable());
                BaseAnalysisTask.configureJobParallelism(ctx.connectContext.getSessionVariable(), caller.info);
                StmtExecutor executor = new StmtExecutor(ctx.connectContext, sql);
                synchronized (ExternalSampleBatchCollector.this) {
                    checkCaller(caller);
                    activeExecutor = executor;
                }
                resultRows = executor.executeInternalQuery();
            }
            Preconditions.checkState(resultRows.size() == planned.size(),
                    "Expected %s batched external sample rows, got %s", planned.size(), resultRows.size());
            Map<String, ExternalColumnOutcome> byCol = new TreeMap<>();
            for (ResultRow r : resultRows) {
                Preconditions.checkState(r.getValues().size() == 17,
                        "Expected 17 columns in batched external sample row, got %s", r.getValues().size());
                int ord = Integer.parseInt(r.get(0));
                Preconditions.checkState(ord >= 0 && ord < planned.size(), "Invalid column ordinal %s", ord);
                PlannedColumn pc = planned.get(ord);
                boolean overLimit = Integer.parseInt(r.get(1)) != 0;
                ResultRow trimmed = new ResultRow(new ArrayList<>(r.getValues().subList(2, 17)));
                byCol.put(pc.name(), new ExternalColumnOutcome(overLimit, trimmed));
            }
            Preconditions.checkState(byCol.size() == planned.size(),
                    "Missing columns in batched external sample result");
            return byCol;
        }

        void onTaskCancelled() {
            StmtExecutor executor = null;
            synchronized (ExternalSampleBatchCollector.this) {
                if (state != State.PENDING && state != State.RUNNING) {
                    return;
                }
                if (memberTasks.stream().allMatch(t -> t.killed)) {
                    executor = cancelLocked();
                }
                ExternalSampleBatchCollector.this.notifyAll();
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
        toCancel.forEach(ExternalSampleBatchCollector::cancelQuery);
    }

    private static void cancelQuery(StmtExecutor executor) {
        try {
            executor.cancel(Status.CANCELLED);
        } catch (RuntimeException e) {
            LOG.warn("Failed to cancel a batched external sample analysis query", e);
        }
    }
}

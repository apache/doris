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

package org.apache.doris.statistics;

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatisticsJobAppender extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsJobAppender.class);

    public static final long INTERVAL = 1000;
    public static final int JOB_MAP_SIZE = 1000;
    public static final int TABLE_BATCH_SIZE = 100;

    private long currentDbId = 0;
    private long currentTableId = 0;
    private long lastRoundFinishTime = 0;
    private final long lowJobIntervalMs = TimeUnit.MINUTES.toMillis(1);

    public StatisticsJobAppender() {
        super("Statistics Job Appender", INTERVAL);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!StatisticsUtil.enableAutoAnalyze()) {
            return;
        }
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        if (!StatisticsUtil.statsTblAvailable()) {
            LOG.info("Stats table not available, skip");
            return;
        }
        if (Env.isCheckpointThread()) {
            return;
        }
        appendJobs();
    }

    protected void appendJobs() {
        AnalysisManager manager = Env.getCurrentEnv().getAnalysisManager();
        appendColumnsToJobs(manager.highPriorityColumns, manager.highPriorityJobs);
        appendColumnsToJobs(manager.midPriorityColumns, manager.midPriorityJobs);
        if (StatisticsUtil.enableAutoAnalyzeInternalCatalog()) {
            appendToLowJobs(manager.lowPriorityJobs);
        }
    }

    protected void appendColumnsToJobs(Queue<QueryColumn> columnQueue, Map<TableName, Set<Pair<String, String>>> jobs) {
        int size = columnQueue.size();
        int processed = 0;
        for (int i = 0; i < size; i++) {
            QueryColumn column = columnQueue.poll();
            if (column == null) {
                continue;
            }
            TableIf table;
            try {
                table = StatisticsUtil.findTable(column.catalogId, column.dbId, column.tblId);
            } catch (Exception e) {
                LOG.warn("Fail to find table {}.{}.{} for column {}",
                        column.catalogId, column.dbId, column.tblId, column.colName);
                continue;
            }
            if (StatisticConstants.SYSTEM_DBS.contains(table.getDatabase().getFullName())) {
                continue;
            }
            Column col = table.getColumn(column.colName);
            if (col == null || !col.isVisible() || StatisticsUtil.isUnsupportedType(col.getType())) {
                continue;
            }
            Set<Pair<String, String>> columnIndexPairs = table.getColumnIndexPairs(
                    Collections.singleton(column.colName)).stream()
                    .filter(p -> StatisticsUtil.needAnalyzeColumn(table, p))
                    .collect(Collectors.toSet());
            if (columnIndexPairs.isEmpty()) {
                continue;
            }
            TableName tableName = new TableName(table.getDatabase().getCatalog().getName(),
                    table.getDatabase().getFullName(), table.getName());
            synchronized (jobs) {
                // If job map reach the upper limit, stop putting new jobs.
                if (!jobs.containsKey(tableName) && jobs.size() >= JOB_MAP_SIZE) {
                    LOG.info("High or mid job map full.");
                    break;
                }
                if (jobs.containsKey(tableName)) {
                    jobs.get(tableName).addAll(columnIndexPairs);
                } else {
                    jobs.put(tableName, columnIndexPairs);
                }
            }
            processed++;
        }
        if (size > 0 && LOG.isDebugEnabled()) {
            LOG.debug("{} of {} columns append to jobs", processed, size);
        }
    }

    protected void appendToLowJobs(Map<TableName, Set<Pair<String, String>>> jobs) {
        if (System.currentTimeMillis() - lastRoundFinishTime < lowJobIntervalMs) {
            return;
        }
        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        List<Long> sortedDbs = catalog.getDbIds().stream().sorted().collect(Collectors.toList());
        int processed = 0;
        for (long dbId : sortedDbs) {
            if (dbId < currentDbId || catalog.getDbNullable(dbId) == null
                    || StatisticConstants.SYSTEM_DBS.contains(catalog.getDbNullable(dbId).getFullName())) {
                continue;
            }
            currentDbId = dbId;
            Optional<Database> db = catalog.getDb(dbId);
            if (!db.isPresent()) {
                continue;
            }
            List<Table> tables = db.get().getTables().stream()
                    .sorted((t1, t2) -> (int) (t1.getId() - t2.getId())).collect(Collectors.toList());
            for (Table t : tables) {
                if (!(t instanceof OlapTable) || t.getId() <= currentTableId) {
                    continue;
                }
                if (t.getBaseSchema().size() > StatisticsUtil.getAutoAnalyzeTableWidthThreshold()) {
                    continue;
                }
                Set<Pair<String, String>> columnIndexPairs = t.getColumnIndexPairs(
                        t.getSchemaAllIndexes(false).stream()
                                .filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                                .map(Column::getName).collect(Collectors.toSet()))
                        .stream().filter(p -> StatisticsUtil.needAnalyzeColumn(t, p))
                        .collect(Collectors.toSet());
                if (columnIndexPairs.isEmpty()) {
                    continue;
                }
                TableName tableName = new TableName(t.getDatabase().getCatalog().getName(),
                        t.getDatabase().getFullName(), t.getName());
                synchronized (jobs) {
                    // If job map reach the upper limit, stop adding new jobs.
                    if (!jobs.containsKey(tableName) && jobs.size() >= JOB_MAP_SIZE) {
                        LOG.info("Low job map full.");
                        return;
                    }
                    if (jobs.containsKey(tableName)) {
                        jobs.get(tableName).addAll(columnIndexPairs);
                    } else {
                        jobs.put(tableName, columnIndexPairs);
                    }
                }
                currentTableId = t.getId();
                if (++processed >= TABLE_BATCH_SIZE) {
                    return;
                }
            }
        }
        // All tables have been processed once, reset for the next loop.
        if (LOG.isDebugEnabled()) {
            LOG.debug("All low priority internal tables are appended once.");
        }
        currentDbId = 0;
        currentTableId = 0;
        lastRoundFinishTime = System.currentTimeMillis();
    }

    // For unit test only.
    public void setLastRoundFinishTime(long value) {
        lastRoundFinishTime = value;
    }

}

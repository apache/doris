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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatisticsJobAppender extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsJobAppender.class);

    public static final long INTERVAL = 1000;
    public static final int JOB_MAP_SIZE = 100;
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
        if (Env.getCurrentEnv().getStatisticsAutoCollector() == null
                || !Env.getCurrentEnv().getStatisticsAutoCollector().isReady()) {
            LOG.info("Statistics auto collector not ready, skip");
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
            appendToLowJobs(manager.lowPriorityJobs, manager.veryLowPriorityJobs);
        }
    }

    protected void appendColumnsToJobs(Queue<QueryColumn> columnQueue, Queue<PriorityTableJob> jobQueue) {
        int size = columnQueue.size();
        int processed = 0;
        AnalysisManager manager = Env.getCurrentEnv().getAnalysisManager();
        Map<TableNameInfo, Set<Pair<String, String>>> tempJobMap = new HashMap<>();
        
        // First, collect all columns for each table
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
            TableNameInfo tableName = new TableNameInfo(table.getDatabase().getCatalog().getName(),
                    table.getDatabase().getFullName(), table.getName());
            tempJobMap.computeIfAbsent(tableName, k -> new HashSet<>()).addAll(columnIndexPairs);
            processed++;
        }
        
        // Then, add jobs to priority queue based on query frequency
        synchronized (jobQueue) {
            for (Map.Entry<TableNameInfo, Set<Pair<String, String>>> entry : tempJobMap.entrySet()) {
                TableNameInfo tableName = entry.getKey();
                Set<Pair<String, String>> columns = entry.getValue();
                
                // Calculate priority score based on query frequency
                double priorityScore = TableQueryStats.getInstance().calculatePriorityScore(tableName);
                
                // Check if job already exists and merge columns
                PriorityTableJob existingJob = null;
                if (jobQueue == manager.highPriorityJobs) {
                    existingJob = manager.highPriorityJobMap.get(tableName);
                } else if (jobQueue == manager.midPriorityJobs) {
                    existingJob = manager.midPriorityJobMap.get(tableName);
                }
                
                if (existingJob != null) {
                    // Merge columns
                    existingJob.getColumns().addAll(columns);
                    // Update priority score (may have increased due to new queries)
                    double newScore = TableQueryStats.getInstance().calculatePriorityScore(tableName);
                    if (newScore > existingJob.getPriorityScore()) {
                        // Remove old job and add new one with updated score
                        jobQueue.remove(existingJob);
                        PriorityTableJob newJob = new PriorityTableJob(tableName, existingJob.getColumns(), newScore);
                        jobQueue.offer(newJob);
                        if (jobQueue == manager.highPriorityJobs) {
                            manager.highPriorityJobMap.put(tableName, newJob);
                        } else if (jobQueue == manager.midPriorityJobs) {
                            manager.midPriorityJobMap.put(tableName, newJob);
                        }
                    }
                } else {
                    // Check queue size limit
                    if (jobQueue.size() >= JOB_MAP_SIZE) {
                        LOG.info("Job queue full, skipping table {}", tableName);
                        continue;
                    }
                    // Create new job
                    PriorityTableJob newJob = new PriorityTableJob(tableName, columns, priorityScore);
                    jobQueue.offer(newJob);
                    if (jobQueue == manager.highPriorityJobs) {
                        manager.highPriorityJobMap.put(tableName, newJob);
                    } else if (jobQueue == manager.midPriorityJobs) {
                        manager.midPriorityJobMap.put(tableName, newJob);
                    }
                }
            }
        }
        
        if (size > 0 && LOG.isDebugEnabled()) {
            LOG.debug("{} of {} columns append to jobs, queue size: {}", processed, size, jobQueue.size());
        }
    }

    protected int appendToLowJobs(Queue<PriorityTableJob> lowPriorityJobs,
                                   Queue<PriorityTableJob> veryLowPriorityJobs) {
        if (System.currentTimeMillis() - lastRoundFinishTime < lowJobIntervalMs) {
            return 0;
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
            List<Table> tables = sortTables(db.get().getTables());
            for (Table t : tables) {
                if (!(t instanceof OlapTable) || t.getId() <= currentTableId) {
                    continue;
                }
                if (t.getBaseSchema().size() > StatisticsUtil.getAutoAnalyzeTableWidthThreshold()) {
                    continue;
                }
                Set<String> columns = t.getSchemaAllIndexes(false).stream()
                        .filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                        .map(Column::getName).collect(Collectors.toSet());
                TableNameInfo tableName = new TableNameInfo(t.getDatabase().getCatalog().getName(),
                        t.getDatabase().getFullName(), t.getName());
                boolean appended = false;
                long version = Config.isCloudMode() ? 0 : StatisticsUtil.getOlapTableVersion((OlapTable) t);
                
                // Collect columns for low and very low priority
                Set<Pair<String, String>> lowPriorityColumns = new HashSet<>();
                Set<Pair<String, String>> veryLowPriorityColumns = new HashSet<>();
                
                for (Pair<String, String> p : t.getColumnIndexPairs(columns)) {
                    if (StatisticsUtil.needAnalyzeColumn(t, p)) {
                        lowPriorityColumns.add(p);
                        appended = true;
                    } else if (StatisticsUtil.isLongTimeColumn(t, p, version)) {
                        veryLowPriorityColumns.add(p);
                        appended = true;
                    }
                }
                
                // Add to priority queues based on query frequency
                AnalysisManager manager = Env.getCurrentEnv().getAnalysisManager();
                double priorityScore = TableQueryStats.getInstance().calculatePriorityScore(tableName);
                
                if (!lowPriorityColumns.isEmpty()) {
                    synchronized (lowPriorityJobs) {
                        if (lowPriorityJobs.size() >= JOB_MAP_SIZE) {
                            LOG.debug("Low Priority job queue is full.");
                            return processed;
                        }
                        PriorityTableJob existingJob = manager.lowPriorityJobMap.get(tableName);
                        if (existingJob != null) {
                            existingJob.getColumns().addAll(lowPriorityColumns);
                        } else {
                            PriorityTableJob newJob = new PriorityTableJob(tableName, lowPriorityColumns, priorityScore);
                            lowPriorityJobs.offer(newJob);
                            manager.lowPriorityJobMap.put(tableName, newJob);
                        }
                    }
                }
                
                if (!veryLowPriorityColumns.isEmpty()) {
                    synchronized (veryLowPriorityJobs) {
                        if (veryLowPriorityJobs.size() >= JOB_MAP_SIZE) {
                            LOG.debug("Very low Priority job queue is full.");
                        } else {
                            PriorityTableJob existingJob = manager.veryLowPriorityJobMap.get(tableName);
                            if (existingJob != null) {
                                existingJob.getColumns().addAll(veryLowPriorityColumns);
                            } else {
                                PriorityTableJob newJob = new PriorityTableJob(tableName, veryLowPriorityColumns, priorityScore);
                                veryLowPriorityJobs.offer(newJob);
                                manager.veryLowPriorityJobMap.put(tableName, newJob);
                            }
                        }
                    }
                }
                currentTableId = t.getId();
                if (appended) {
                    processed++;
                }
                if (processed >= TABLE_BATCH_SIZE) {
                    return processed;
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
        return processed;
    }

    protected List<Table> sortTables(List<Table> tables) {
        if (tables == null) {
            return Lists.newArrayList();
        }
        return tables.stream().sorted(Comparator.comparingLong(Table::getId)).collect(Collectors.toList());
    }

    @VisibleForTesting
    public boolean doAppend(Map<TableNameInfo, Set<Pair<String, String>>> jobMap,
                         Pair<String, String> columnIndexPair,
                         TableNameInfo tableName) {
        synchronized (jobMap) {
            if (!jobMap.containsKey(tableName) && jobMap.size() >= JOB_MAP_SIZE) {
                return false;
            }
            if (jobMap.containsKey(tableName)) {
                jobMap.get(tableName).add(columnIndexPair);
            } else {
                Set<Pair<String, String>> columnSet = Sets.newHashSet();
                columnSet.add(columnIndexPair);
                jobMap.put(tableName, columnSet);
            }
        }
        return true;
    }

    // For unit test only.
    public void setLastRoundFinishTime(long value) {
        lastRoundFinishTime = value;
    }

}

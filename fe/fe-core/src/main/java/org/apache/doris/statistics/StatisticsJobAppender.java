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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class StatisticsJobAppender extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsJobAppender.class);

    public static final long INTERVAL = 1000;
    public static final int JOB_MAP_SIZE = 1000;

    private long currentDbId;
    private long currentTableId;

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
        // LOG.info("Append column to high priority job map.");
        appendColumnsToJobs(manager.highPriorityColumns, manager.highPriorityJobs);
        // LOG.info("Append column to mid priority job map.");
        appendColumnsToJobs(manager.midPriorityColumns, manager.midPriorityJobs);
        if (StatisticsUtil.enableAutoAnalyzeInternalCatalog()) {
            // LOG.info("Append column to low priority job map.");
            appendToLowQueue(manager.lowPriorityJobs);
        }
    }

    protected void appendColumnsToJobs(Queue<HighPriorityColumn> columnQueue, Map<TableName, Set<String>> jobsMap) {
        int size = columnQueue.size();
        for (int i = 0; i < size; i++) {
            HighPriorityColumn column = columnQueue.poll();
            LOG.info("Process column " + column.tblId + "." + column.colName);
            TableIf table = StatisticsUtil.findTable(column.catalogId, column.dbId, column.tblId);
            TableName tableName = new TableName(table.getDatabase().getCatalog().getName(),
                    table.getDatabase().getFullName(), table.getName());
            synchronized (jobsMap) {
                // If job map reach the upper limit, stop putting new jobs.
                if (!jobsMap.containsKey(tableName) && jobsMap.size() >= JOB_MAP_SIZE) {
                    LOG.info("Job map full.");
                    break;
                }
                if (jobsMap.containsKey(tableName)) {
                    jobsMap.get(tableName).add(column.colName);
                } else {
                    HashSet<String> columns = new HashSet<>();
                    columns.add(column.colName);
                    jobsMap.put(tableName, columns);
                }
                LOG.info("Column " + column.tblId + "." + column.colName + " added");
            }
        }
    }

    protected void appendToLowQueue(Map<TableName, Set<String>> jobsMap) {
        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        List<Long> sortedDbs = catalog.getDbIds().stream().sorted().collect(Collectors.toList());
        int batchSize = 100;
        for (long dbId : sortedDbs) {
            if (dbId < currentDbId
                    || StatisticConstants.SYSTEM_DBS.contains(catalog.getDbNullable(dbId).getFullName())) {
                continue;
            }
            currentDbId = dbId;
            Optional<Database> db = catalog.getDb(dbId);
            List<Table> tables = db.get().getTables().stream()
                    .sorted((t1, t2) -> (int) (t1.getId() - t2.getId())).collect(Collectors.toList());
            for (Table t : tables) {
                if (!(t instanceof OlapTable) || t.getId() <= currentTableId) {
                    continue;
                }
                TableName tableName = new TableName(t.getDatabase().getCatalog().getName(),
                        t.getDatabase().getFullName(), t.getName());
                synchronized (jobsMap) {
                    // If job map reach the upper limit, stop adding new jobs.
                    if (!jobsMap.containsKey(tableName) && jobsMap.size() >= JOB_MAP_SIZE) {
                        return;
                    }
                    Set<String> columns
                            = t.getColumns().stream().filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                            .map(c -> c.getName()).collect(Collectors.toSet());
                    if (jobsMap.containsKey(tableName)) {
                        jobsMap.get(tableName).addAll(columns);
                    } else {
                        jobsMap.put(tableName, columns);
                    }
                }
                currentTableId = t.getId();
                if (--batchSize <= 0) {
                    return;
                }
            }
        }
        // All tables have been processed once, reset for the next loop.
        currentDbId = 0;
        currentTableId = 0;
    }
}

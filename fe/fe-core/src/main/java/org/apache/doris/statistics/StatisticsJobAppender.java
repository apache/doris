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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class StatisticsJobAppender extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsJobAppender.class);

    public static final long INTERVAL = 10 * 1000;

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
        appendToHighAndNormalQueue(manager, manager.predicateColumns, manager.highPriorityJobs);
        appendToHighAndNormalQueue(manager, manager.queryColumns, manager.normalPriorityJobs);
        appendToLowQueue(manager, manager.lowPriorityJobs);
    }

    protected void appendToHighAndNormalQueue(AnalysisManager manager,
                                  Queue<CriticalColumn> columnQueue,
                                  LinkedHashMap<TableIf, Set<String>> jobQueue) {
        while (true) {
            CriticalColumn column = columnQueue.peek();
            if (column == null) {
                break;
            }
            boolean added = manager.addColumnToJobQueue(column, jobQueue);
            if (added) {
                columnQueue.poll();
            } else {
                // added == false means job queue is full. Stop adding.
                break;
            }
        }
    }

    protected void appendToLowQueue(AnalysisManager manager,
                                    LinkedHashMap<TableIf, Set<String>> jobQueue) {

        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        List<Long> sortedDbs = catalog.getDbIds().stream().sorted().collect(Collectors.toList());
        boolean added = false;
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
                if (t.getId() <= currentTableId) {
                    continue;
                }
                added = manager.addTableToJobQueue(t, jobQueue);
                if (added) {
                    currentTableId = t.getId();
                } else {
                    break;
                }
            }
            // added == false means job queue is full. Stop adding.
            if (!added) {
                break;
            }
        }
        // All tables have been processed once, reset for the next loop.
        if (added) {
            currentDbId = 0;
            currentTableId = 0;
        }
    }
}

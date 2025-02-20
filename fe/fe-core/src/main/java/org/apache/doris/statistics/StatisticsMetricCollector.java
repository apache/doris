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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Daemon thread to collect statistics related metrics.
 */
public class StatisticsMetricCollector extends MasterDaemon {

    public static final Logger LOG = LogManager.getLogger(StatisticsMetricCollector.class);

    public static final long INTERVAL = 300;

    private final AtomicLong failedTaskCounter;
    private final AtomicLong abandonedInvalidStatsCounter;
    private volatile long unhealthyTableCount;
    private volatile long unhealthyColumnCount;
    private volatile long notAnalyzedTableCount;
    private volatile long totalTableCount;
    private volatile long totalColumnCount;
    private volatile long emptyTableCount;
    private volatile long emptyTableColumnCount;

    public StatisticsMetricCollector() {
        super("Statistics Metric Collector", TimeUnit.SECONDS.toMillis(INTERVAL));
        failedTaskCounter = new AtomicLong(0);
        abandonedInvalidStatsCounter = new AtomicLong(0);
    }

    @Override
    protected void runAfterCatalogReady() {
        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        long tmpUnhealthyTableCount = 0;
        long tmpUnhealthyColumnCount = 0;
        long tmpNotAnalyzedTableCount = 0;
        long tmpTotalTableCount = 0;
        long tmpTotalColumnCount = 0;
        long tmpEmptyTableCount = 0;
        long tmpEmptyTableColumnCount = 0;
        for (DatabaseIf<? extends TableIf> db : catalog.getAllDbs()) {
            try {
                List<? extends TableIf> tables = db.getTables();
                tmpTotalTableCount += tables.size();
                for (TableIf table : db.getTables()) {
                    try {
                        // Get all supported columns, including all indexes.
                        Set<String> columns = table.getSchemaAllIndexes(false)
                                .stream()
                                .filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                                .map(Column::getName)
                                .collect(Collectors.toSet());
                        tmpTotalColumnCount += columns.size();
                        if (table.getRowCount() == 0) {
                            tmpEmptyTableCount += 1;
                            tmpEmptyTableColumnCount += columns.size();
                        }
                        if (analysisManager.findTableStatsStatus(table.getId()) == null) {
                            tmpNotAnalyzedTableCount += 1;
                        }
                        // Get all unhealthy columns.
                        Set<Pair<String, String>> columnIndexPairs = table.getColumnIndexPairs(columns)
                                .stream().filter(p -> StatisticsUtil.needAnalyzeColumn(table, p))
                                .collect(Collectors.toSet());
                        if (!columnIndexPairs.isEmpty()) {
                            tmpUnhealthyTableCount += 1;
                            tmpUnhealthyColumnCount += columnIndexPairs.size();
                        }
                    } catch (Exception e) {
                        LOG.info("Failed to get metrics for table {}. Reason {}", table.getName(), e.getMessage());
                    }
                }
            } catch (Exception e) {
                LOG.info("Failed to get metrics for db {}. Reason {}", db.getFullName(), e.getMessage());
            }
        }
        unhealthyTableCount = tmpUnhealthyTableCount;
        unhealthyColumnCount = tmpUnhealthyColumnCount;
        notAnalyzedTableCount = tmpNotAnalyzedTableCount;
        totalTableCount = tmpTotalTableCount;
        totalColumnCount = tmpTotalColumnCount;
        emptyTableCount = tmpEmptyTableCount;
        emptyTableColumnCount = tmpEmptyTableColumnCount;
    }

    public void increaseFailedTask() {
        failedTaskCounter.incrementAndGet();
    }

    public void increaseAbandonedInvalidStats() {
        abandonedInvalidStatsCounter.incrementAndGet();
    }

    public long getFailedTaskCount() {
        return failedTaskCounter.get();
    }

    public long getAbandonedInvalidStatsCount() {
        return abandonedInvalidStatsCounter.get();
    }

    public long getUnhealthyTableCount() {
        return unhealthyTableCount;
    }

    public double getUnhealthyTableRate() {
        return (totalTableCount == 0 ? 0 : (double) unhealthyTableCount / totalTableCount) * 100;
    }

    public long getUnhealthyColumnCount() {
        return unhealthyColumnCount;
    }

    public double getUnhealthyColumnRate() {
        return (totalColumnCount == 0 ? 0 : (double) unhealthyColumnCount / totalColumnCount) * 100;
    }

    public long getNotAnalyzedTableCount() {
        return notAnalyzedTableCount;
    }

    public long getEmptyTableCount() {
        return emptyTableCount;
    }

    public long getEmptyTableColumnCount() {
        return emptyTableColumnCount;
    }

    public int getHighPriorityQueueLength() {
        return Env.getCurrentEnv().getAnalysisManager().highPriorityJobs.size();
    }

    public int getMidPriorityQueueLength() {
        return Env.getCurrentEnv().getAnalysisManager().midPriorityJobs.size();
    }

    public int getLowPriorityQueueLength() {
        return Env.getCurrentEnv().getAnalysisManager().lowPriorityJobs.size();
    }

    public int getVeryLowPriorityQueueLength() {
        return Env.getCurrentEnv().getAnalysisManager().veryLowPriorityJobs.size();
    }
}

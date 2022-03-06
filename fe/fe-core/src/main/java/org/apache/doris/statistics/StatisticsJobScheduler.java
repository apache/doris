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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.StatisticsJob.JobState;

import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Schedule statistics job.
 * 1. divide job to multi task
 * 2. submit all task to StatisticsTaskScheduler
 * Switch job state from pending to scheduling.
 */
public class StatisticsJobScheduler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticsJobScheduler.class);

    public Queue<StatisticsJob> pendingJobQueue = Queues.newLinkedBlockingQueue();

    public StatisticsJobScheduler() {
        super("Statistics job scheduler", 0);
    }

    @Override
    protected void runAfterCatalogReady() {
        StatisticsJob pendingJob = pendingJobQueue.peek();
        // step0: check job state again
        if (pendingJob != null && pendingJob.getJobState() == JobState.PENDING) {
            List<StatisticsTask> taskList = null;
            try {
                // step1: divide statistics job to task
                taskList = this.divide(pendingJob);
                pendingJob.setTaskList(taskList);
            } catch (DdlException e) {
                LOG.warn("Failed to schedule the statistical job(id="
                        + pendingJob.getId() + "). " + e.getMessage());
            }
            if (taskList != null) {
                // step2: submit
                Catalog.getCurrentCatalog().getStatisticsTaskScheduler().addTasks(taskList);
            }
        }
    }

    public void addPendingJob(StatisticsJob statisticsJob) throws IllegalStateException {
        pendingJobQueue.add(statisticsJob);
    }

    /**
     * Statistics tasks are of the following types：
     * table:
     * - row_count: table row count are critical in estimating cardinality and memory usage of scan nodes.
     * - data_size: table size, not applicable to CBO, mainly used to monitor and manage table size.
     * column:
     * - num_distinct_value: used to determine the selectivity of an equivalent expression.
     * - min: The minimum value.
     * - max: The maximum value.
     * - num_nulls: number of nulls.
     * - avg_col_len: the average length of a column, in bytes, is used for memory and network IO evaluation.
     * - max_col_len: the Max length of the column, in bytes, is used for memory and network IO evaluation.
     * <p>
     * Divide:
     * - min, max, ndv: These three full indicators are collected by a sub-task.
     * - max_col_lens, avg_col_lens: Two sampling indicators were collected by a sub-task.
     * <p>
     * If the table row-count is greater than the maximum number of Be scans for a single BE,
     * we'll divide subtasks by partition. relevant values(3700000000L&600000000L) are derived from test.
     * <p>
     * Eventually, we will get several subtasks of the following types:
     *
     * @throws DdlException DdlException
     * @see MetaStatisticsTask
     * @see SampleSQLStatisticsTask
     * @see SQLStatisticsTask
     */
    private List<StatisticsTask> divide(StatisticsJob statisticsJob) throws DdlException {
        final long jobId = statisticsJob.getId();
        final long dbId = statisticsJob.getDbId();
        final Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        final Set<Long> tableIds = statisticsJob.relatedTableId();
        final Map<Long, List<String>> tableIdToColumnName = statisticsJob.getTableIdToColumnName();
        final List<StatisticsTask> taskList = statisticsJob.getTaskList();
        final List<Long> backendIds = Catalog.getCurrentSystemInfo().getBackendIds(true);

        for (Long tableId : tableIds) {
            final Table tbl = db.getTableOrDdlException(tableId);
            final long rowCount = tbl.getRowCount();
            final List<Long> partitionIds = ((OlapTable) tbl).getPartitionIds();
            final List<String> columnNameList = tableIdToColumnName.get(tableId);

            // step 0: generate data_size task
            StatsCategoryDesc dataSizeCategory = this.getTblStatsCategoryDesc(dbId, tableId);
            StatsGranularityDesc dataSizeGranularity = this.getTblStatsGranularityDesc(tableId);
            MetaStatisticsTask dataSizeTask = new MetaStatisticsTask(jobId,
                    dataSizeGranularity, dataSizeCategory, Collections.singletonList(StatsType.DATA_SIZE));
            taskList.add(dataSizeTask);

            // step 1: generate row_count task
            KeysType keysType = ((OlapTable) tbl).getKeysType();
            if (keysType == KeysType.DUP_KEYS) {
                StatsCategoryDesc rowCountCategory = this.getTblStatsCategoryDesc(dbId, tableId);
                StatsGranularityDesc rowCountGranularity = this.getTblStatsGranularityDesc(tableId);
                MetaStatisticsTask metaTask = new MetaStatisticsTask(jobId,
                        rowCountGranularity, rowCountCategory, Collections.singletonList(StatsType.ROW_COUNT));
                taskList.add(metaTask);
            } else {
                if (rowCount > backendIds.size() * 3700000000L) {
                    // divide subtasks by partition
                    for (Long partitionId : partitionIds) {
                        StatsCategoryDesc rowCountCategory = this.getTblStatsCategoryDesc(dbId, tableId);
                        StatsGranularityDesc rowCountGranularity = this.getPartitionStatsGranularityDesc(tableId, partitionId);
                        SQLStatisticsTask sqlTask = new SQLStatisticsTask(jobId,
                                rowCountGranularity, rowCountCategory, Collections.singletonList(StatsType.ROW_COUNT));
                        taskList.add(sqlTask);
                    }
                } else {
                    StatsCategoryDesc rowCountCategory = this.getTblStatsCategoryDesc(dbId, tableId);
                    StatsGranularityDesc rowCountGranularity = this.getTblStatsGranularityDesc(tableId);
                    SQLStatisticsTask sqlTask = new SQLStatisticsTask(jobId,
                            rowCountGranularity, rowCountCategory, Collections.singletonList(StatsType.ROW_COUNT));
                    taskList.add(sqlTask);
                }
            }

            // step 2: generate [min,max,ndv] task
            if (rowCount > backendIds.size() * 600000000L) {
                for (String columnName : columnNameList) {
                    // divide subtasks by partition
                    for (Long partitionId : partitionIds) {
                        StatsCategoryDesc columnCategory = this.getColStatsCategoryDesc(dbId, tableId, columnName);
                        StatsGranularityDesc columnGranularity = this.getPartitionStatsGranularityDesc(tableId, partitionId);
                        List<StatsType> statsTypes = Arrays.asList(StatsType.MIN_VALUE, StatsType.MAX_VALUE, StatsType.NDV);
                        SQLStatisticsTask sqlTask = new SQLStatisticsTask(jobId, columnGranularity, columnCategory, statsTypes);
                        taskList.add(sqlTask);
                    }
                }
            } else {
                for (String columnName : columnNameList) {
                    StatsCategoryDesc columnCategory = this.getColStatsCategoryDesc(dbId, tableId, columnName);
                    StatsGranularityDesc columnGranularity = this.getTblStatsGranularityDesc(tableId);
                    List<StatsType> statsTypes = Arrays.asList(StatsType.MIN_VALUE, StatsType.MAX_VALUE, StatsType.NDV);
                    SQLStatisticsTask sqlTask = new SQLStatisticsTask(jobId, columnGranularity, columnCategory, statsTypes);
                    taskList.add(sqlTask);
                }
            }

            // step 3: generate num_nulls task
            for (String columnName : columnNameList) {
                StatsCategoryDesc columnCategory = this.getColStatsCategoryDesc(dbId, tableId, columnName);
                StatsGranularityDesc columnGranularity = this.getTblStatsGranularityDesc(tableId);
                SQLStatisticsTask sqlTask = new SQLStatisticsTask(jobId,
                        columnGranularity, columnCategory, Collections.singletonList(StatsType.NUM_NULLS));
                taskList.add(sqlTask);
            }

            // step 4: generate [max_col_lens, avg_col_lens] task
            for (String columnName : columnNameList) {
                StatsCategoryDesc columnCategory = this.getColStatsCategoryDesc(dbId, tableId, columnName);
                StatsGranularityDesc columnGranularity = this.getTblStatsGranularityDesc(tableId);
                List<StatsType> statsTypes = Arrays.asList(StatsType.MAX_SIZE, StatsType.AVG_SIZE);
                Column column = tbl.getColumn(columnName);
                Type colType = column.getType();
                if (colType.isStringType()) {
                    SQLStatisticsTask sampleSqlTask = new SampleSQLStatisticsTask(jobId, columnGranularity, columnCategory, statsTypes);
                    taskList.add(sampleSqlTask);
                } else {
                    MetaStatisticsTask metaTask = new MetaStatisticsTask(jobId, columnGranularity, columnCategory, statsTypes);
                    taskList.add(metaTask);
                }
            }
        }

        return taskList;
    }

    private StatsCategoryDesc getTblStatsCategoryDesc(long dbId, long tableId) {
        StatsCategoryDesc statsCategoryDesc = new StatsCategoryDesc();
        statsCategoryDesc.setCategory(StatsCategoryDesc.StatsCategory.TABLE);
        statsCategoryDesc.setDbId(dbId);
        statsCategoryDesc.setTableId(tableId);
        return statsCategoryDesc;
    }

    private StatsCategoryDesc getColStatsCategoryDesc(long dbId, long tableId, String columnName) {
        StatsCategoryDesc statsCategoryDesc = new StatsCategoryDesc();
        statsCategoryDesc.setDbId(dbId);
        statsCategoryDesc.setTableId(tableId);
        statsCategoryDesc.setCategory(StatsCategoryDesc.StatsCategory.COLUMN);
        statsCategoryDesc.setColumnName(columnName);
        return statsCategoryDesc;
    }

    private StatsGranularityDesc getTblStatsGranularityDesc(long tableId) {
        StatsGranularityDesc statsGranularityDesc = new StatsGranularityDesc();
        statsGranularityDesc.setTableId(tableId);
        statsGranularityDesc.setGranularity(StatsGranularityDesc.StatsGranularity.TABLE);
        return statsGranularityDesc;
    }

    private StatsGranularityDesc getPartitionStatsGranularityDesc(long tableId, long partitionId) {
        StatsGranularityDesc statsGranularityDesc = new StatsGranularityDesc();
        statsGranularityDesc.setTableId(tableId);
        statsGranularityDesc.setPartitionId(partitionId);
        statsGranularityDesc.setGranularity(StatsGranularityDesc.StatsGranularity.PARTITION);
        return statsGranularityDesc;
    }

    private StatsGranularityDesc getTabletStatsGranularityDesc(long tableId) {
        StatsGranularityDesc statsGranularityDesc = new StatsGranularityDesc();
        statsGranularityDesc.setTableId(tableId);
        statsGranularityDesc.setGranularity(StatsGranularityDesc.StatsGranularity.PARTITION);
        return statsGranularityDesc;
    }
}

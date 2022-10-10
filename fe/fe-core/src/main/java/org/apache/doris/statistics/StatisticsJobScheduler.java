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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.StatsCategory.Category;
import org.apache.doris.statistics.StatsGranularity.Granularity;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

/**
  * Schedule statistics job.
  *     1. divide job to multi task
  *     2. submit all task to StatisticsTaskScheduler
  * Switch job state from pending to scheduling.
 */
public class StatisticsJobScheduler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticsJobScheduler.class);

    /**
     * If the table row-count is greater than the maximum number of Be scans for a single BE,
     * we'll divide subtasks by partition. relevant values(3700000000L&600000000L) are derived from test.
     * COUNT_MAX_SCAN_PER_TASK is for count(expr), NDV_MAX_SCAN_PER_TASK is for min(c1)/max(c1)/ndv(c1).
     */
    private static final long COUNT_MAX_SCAN_PER_TASK = 3700000000L;
    private static final long NDV_MAX_SCAN_PER_TASK = 600000000L;

    /**
     * if the table row count is greater than the value, use sampleSqlTask instead of SqlTask.
     */
    private static final int MIN_SAMPLE_ROWS = 200000;

    /**
     * Different statistics need to be collected for the jobs submitted by users.
     * if all statistics be collected at the same time, the cluster may be overburdened
     * and normal query services may be affected. Therefore, we put the jobs into the queue
     * and schedule them one by one, and finally divide each job to several subtasks and execute them.
     */
    public final Queue<StatisticsJob> pendingJobQueue
            = Queues.newLinkedBlockingQueue(Config.cbo_max_statistics_job_num);

    public StatisticsJobScheduler() {
        super("Statistics job scheduler",
                Config.statistic_job_scheduler_execution_interval_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        StatisticsJob pendingJob = pendingJobQueue.peek();
        if (pendingJob != null) {
            try {
                if (pendingJob.getTasks().size() == 0) {
                    divide(pendingJob);
                }
                List<StatisticsTask> tasks = pendingJob.getTasks();
                Env.getCurrentEnv().getStatisticsTaskScheduler().addTasks(tasks);
                pendingJob.updateJobState(StatisticsJob.JobState.SCHEDULING);
                pendingJobQueue.remove();
            } catch (IllegalStateException e) {
                // throw IllegalStateException if the queue is full, re-add the tasks next time
                LOG.info("The statistics task queue is full, schedule the job(id={}) later", pendingJob.getId());
            } catch (DdlException e) {
                pendingJobQueue.remove();
                try {
                    // TODO change to without exception
                    pendingJob.updateJobState(StatisticsJob.JobState.FAILED);
                } catch (DdlException ddlException) {
                    LOG.fatal(ddlException.getMessage(), e);
                }
                LOG.info("Failed to schedule the statistical job(id={})", pendingJob.getId(), e);
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
    private void divide(StatisticsJob job) throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(job.getDbId());
        Set<Long> tblIds = job.getTblIds();

        for (Long tblId : tblIds) {
            Optional<Table> optionalTbl = db.getTable(tblId);
            if (optionalTbl.isPresent()) {
                Table table = optionalTbl.get();
                if (!table.isPartitioned()) {
                    getStatsTaskByTable(job, tblId);
                } else {
                    getStatsTaskByPartition(job, tblId);
                }
            } else {
                LOG.warn("Table(id={}) not found in the database {}", tblId, db.getFullName());
            }
        }
    }

    /**
     * For non-partitioned table, dividing the job into several subtasks.
     *
     * @param job statistics job
     * @param tableId table id
     * @throws DdlException exception
     */
    private void getStatsTaskByTable(StatisticsJob job, long tableId) throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(job.getDbId());
        OlapTable table = (OlapTable) db.getTableOrDdlException(tableId);

        if (table.getDataSize() == 0) {
            LOG.info("Do not collect statistics for empty table {}", table.getName());
            return;
        }

        Map<Long, List<String>> tblIdToColName = job.getTableIdToColumnName();
        List<String> colNames = tblIdToColName.get(tableId);

        List<Long> backendIds = Env.getCurrentSystemInfo().getBackendIds(true);

        // step1: collect statistics by metadata
        List<StatisticsDesc> descs = Lists.newArrayList();

        // table data size
        StatsCategory dsCategory = getTableStatsCategory(job.getDbId(), tableId);
        StatsGranularity dsGranularity = getTableGranularity(tableId);
        StatisticsDesc dsStatsDesc = new StatisticsDesc(dsCategory,
                dsGranularity, Collections.singletonList(StatsType.DATA_SIZE));
        descs.add(dsStatsDesc);

        // table row count
        if (table.getKeysType() == KeysType.DUP_KEYS) {
            StatsCategory rcCategory = getTableStatsCategory(job.getDbId(), tableId);
            StatsGranularity rcGranularity = getTableGranularity(tableId);
            StatisticsDesc rcStatsDesc = new StatisticsDesc(rcCategory,
                    rcGranularity, Collections.singletonList(StatsType.ROW_COUNT));
            descs.add(rcStatsDesc);
        }

        // variable-length columns
        List<String> strColNames = Lists.newArrayList();

        // column max size and avg size
        for (String colName : colNames) {
            Column column = table.getColumn(colName);
            if (column == null) {
                LOG.info("Column {} not found in table {}", colName, table.getName());
                continue;
            }
            Type colType = column.getType();
            if (colType.isStringType()) {
                strColNames.add(colName);
                continue;
            }
            StatsCategory colCategory = getColumnStatsCategory(job.getDbId(), tableId, colName);
            StatsGranularity colGranularity = getTableGranularity(tableId);
            StatisticsDesc colStatsDesc = new StatisticsDesc(colCategory,
                    colGranularity, Arrays.asList(StatsType.MAX_SIZE, StatsType.AVG_SIZE));
            descs.add(colStatsDesc);
        }

        // all meta statistics are collected in one task
        MetaStatisticsTask metaStatsTask = new MetaStatisticsTask(job.getId(), descs);
        job.getTasks().add(metaStatsTask);

        long rowCount = table.getRowCount();

        // step2: collect statistics by sql
        // table row count (table model is AGGREGATE or UNIQUE)
        if (table.getKeysType() != KeysType.DUP_KEYS) {
            if (rowCount < backendIds.size() * COUNT_MAX_SCAN_PER_TASK) {
                StatsCategory rcCategory = getTableStatsCategory(job.getDbId(), tableId);
                StatsGranularity rcGranularity = getTableGranularity(tableId);
                StatisticsDesc rcStatsDesc = new StatisticsDesc(rcCategory,
                        rcGranularity, Collections.singletonList(StatsType.ROW_COUNT));
                SQLStatisticsTask sqlTask = new SQLStatisticsTask(job.getId(),
                        Collections.singletonList(rcStatsDesc));
                job.getTasks().add(sqlTask);
            } else {
                // divide subtasks by tablet
                Collection<Partition> partitions = table.getPartitions();
                for (Partition partition : partitions) {
                    Collection<Tablet> tablets = partition.getBaseIndex().getTablets();
                    tablets.forEach(tablet -> {
                        StatsCategory rcCategory = getTableStatsCategory(job.getDbId(), tableId);
                        StatsGranularity rcGranularity = getTabletGranularity(tablet.getId());
                        StatisticsDesc rcStatsDesc = new StatisticsDesc(rcCategory,
                                rcGranularity, Collections.singletonList(StatsType.ROW_COUNT));
                        SQLStatisticsTask sqlTask = new SQLStatisticsTask(job.getId(),
                                Collections.singletonList(rcStatsDesc));
                        job.getTasks().add(sqlTask);
                    });
                }
            }
        }

        // column max size, avg size
        for (String colName : strColNames) {
            StatsCategory colCategory = getColumnStatsCategory(job.getDbId(), tableId, colName);
            StatsGranularity colGranularity = getTableGranularity(tableId);
            getColumnSizeSqlTask(job, rowCount, colCategory, colGranularity);
        }

        // column num nulls
        for (String colName : colNames) {
            StatsCategory colCategory = getColumnStatsCategory(job.getDbId(), tableId, colName);
            StatsGranularity colGranularity = getTableGranularity(tableId);
            StatisticsDesc colStatsDesc = new StatisticsDesc(colCategory,
                    colGranularity, Collections.singletonList(StatsType.NUM_NULLS));
            SQLStatisticsTask sqlTask = new SQLStatisticsTask(job.getId(),
                    Collections.singletonList(colStatsDesc));
            job.getTasks().add(sqlTask);
        }

        // column max value, min value and ndv
        for (String colName : colNames) {
            if (rowCount < backendIds.size() * NDV_MAX_SCAN_PER_TASK) {
                StatsCategory colCategory = getColumnStatsCategory(job.getDbId(), tableId, colName);
                StatsGranularity colGranularity = getTableGranularity(tableId);
                StatisticsDesc colStatsDesc = new StatisticsDesc(colCategory,
                        colGranularity, Arrays.asList(StatsType.MAX_VALUE, StatsType.MIN_VALUE, StatsType.NDV));
                SQLStatisticsTask sqlTask = new SQLStatisticsTask(job.getId(),
                        Collections.singletonList(colStatsDesc));
                job.getTasks().add(sqlTask);
            } else {
                // for non-partitioned table system automatically
                // generates a partition with the same name as the table name
                Collection<Partition> partitions = table.getPartitions();
                for (Partition partition : partitions) {
                    List<Tablet> tablets = partition.getBaseIndex().getTablets();
                    tablets.forEach(tablet -> {
                        StatsCategory colCategory = getColumnStatsCategory(job.getDbId(), tableId, colName);
                        StatsGranularity colGranularity = getTabletGranularity(tablet.getId());
                        StatisticsDesc colStatsDesc = new StatisticsDesc(colCategory,
                                colGranularity, Arrays.asList(StatsType.MAX_VALUE, StatsType.MIN_VALUE, StatsType.NDV));
                        SQLStatisticsTask sqlTask = new SQLStatisticsTask(job.getId(),
                                Collections.singletonList(colStatsDesc));
                        job.getTasks().add(sqlTask);
                    });
                }
            }
        }
    }

    /**
     * If table is partitioned, dividing the job into several subtasks by partition.
     *
     * @param job statistics job
     * @param tableId table id
     * @throws DdlException exception
     */
    private void getStatsTaskByPartition(StatisticsJob job, long tableId) throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(job.getDbId());
        OlapTable table = (OlapTable) db.getTableOrDdlException(tableId);

        Map<Long, List<String>> tblIdToColName = job.getTableIdToColumnName();
        List<String> colNames = tblIdToColName.get(tableId);

        Map<Long, List<String>> tblIdToPartitionName = job.getTableIdToPartitionName();
        List<String> partitionNames = tblIdToPartitionName.get(tableId);

        List<Long> backendIds = Env.getCurrentSystemInfo().getBackendIds(true);

        for (String partitionName : partitionNames) {
            Partition partition = table.getPartition(partitionName);
            if (partition == null) {
                LOG.info("Partition {} not found in the table {}", partitionName, table.getName());
                continue;
            }
            if (partition.getDataSize() == 0) {
                LOG.info("Do not collect statistics for empty partition {} in the table {}",
                        partitionName, table.getName());
                continue;
            }

            long partitionId = partition.getId();
            long rowCount = partition.getBaseIndex().getRowCount();

            // step1: collect statistics by metadata
            List<StatisticsDesc> descs = Lists.newArrayList();

            // partition data size
            StatsCategory dsCategory = getPartitionStatsCategory(job.getDbId(), tableId, partitionName);
            StatsGranularity dsGranularity = getPartitionGranularity(partitionId);
            StatisticsDesc dsStatsDesc = new StatisticsDesc(dsCategory,
                    dsGranularity, Collections.singletonList(StatsType.DATA_SIZE));
            descs.add(dsStatsDesc);

            // partition row count
            if (table.getKeysType() == KeysType.DUP_KEYS) {
                StatsCategory rcCategory = getPartitionStatsCategory(job.getDbId(), tableId, partitionName);
                StatsGranularity rcGranularity = getPartitionGranularity(partitionId);
                StatisticsDesc rcStatsDesc = new StatisticsDesc(rcCategory,
                        rcGranularity, Collections.singletonList(StatsType.ROW_COUNT));
                descs.add(rcStatsDesc);
            }

            // variable-length columns
            List<String> strColNames = Lists.newArrayList();

            // column max size and avg size
            for (String colName : colNames) {
                Column column = table.getColumn(colName);
                if (column == null) {
                    LOG.info("Column {} not found in the table {}", colName, table.getName());
                    continue;
                }
                Type colType = column.getType();
                if (colType.isStringType()) {
                    strColNames.add(colName);
                    continue;
                }
                StatsCategory colCategory = getColumnStatsCategory(job.getDbId(), tableId, partitionName, colName);
                StatsGranularity colGranularity = getPartitionGranularity(partitionId);
                StatisticsDesc colStatsDesc = new StatisticsDesc(colCategory,
                        colGranularity, Arrays.asList(StatsType.MAX_SIZE, StatsType.AVG_SIZE));
                descs.add(colStatsDesc);
            }

            // all meta statistics are collected in one task
            MetaStatisticsTask metaStatsTask = new MetaStatisticsTask(job.getId(), descs);
            job.getTasks().add(metaStatsTask);

            // step2: collect statistics by sql
            // partition row count (table model is AGGREGATE or UNIQUE)
            if (table.getKeysType() != KeysType.DUP_KEYS) {
                if (rowCount < backendIds.size() * COUNT_MAX_SCAN_PER_TASK) {
                    StatsCategory rcCategory = getPartitionStatsCategory(job.getDbId(), tableId, partitionName);
                    StatsGranularity rcGranularity = getPartitionGranularity(partitionId);
                    StatisticsDesc rcStatsDesc = new StatisticsDesc(rcCategory,
                            rcGranularity, Collections.singletonList(StatsType.ROW_COUNT));
                    SQLStatisticsTask sqlTask = new SQLStatisticsTask(job.getId(),
                            Collections.singletonList(rcStatsDesc));
                    job.getTasks().add(sqlTask);
                } else {
                    // divide subtasks by tablet
                    List<Tablet> tablets = partition.getBaseIndex().getTablets();
                    tablets.forEach(tablet -> {
                        StatsCategory rcCategory = getPartitionStatsCategory(job.getDbId(), tableId, partitionName);
                        StatsGranularity rcGranularity = getTabletGranularity(tablet.getId());
                        StatisticsDesc rcStatsDesc = new StatisticsDesc(rcCategory,
                                rcGranularity, Collections.singletonList(StatsType.ROW_COUNT));
                        SQLStatisticsTask sqlTask = new SQLStatisticsTask(job.getId(),
                                Collections.singletonList(rcStatsDesc));
                        job.getTasks().add(sqlTask);
                    });
                }
            }

            // column max size, avg size
            for (String colName : strColNames) {
                StatsCategory colCategory = getColumnStatsCategory(job.getDbId(), tableId, partitionName, colName);
                StatsGranularity colGranularity = getPartitionGranularity(partitionId);
                getColumnSizeSqlTask(job, rowCount, colCategory, colGranularity);
            }

            // column null nums
            for (String colName : colNames) {
                StatsCategory colCategory = getColumnStatsCategory(job.getDbId(), tableId, partitionName, colName);
                StatsGranularity colGranularity = getPartitionGranularity(partitionId);
                StatisticsDesc colStatsDesc = new StatisticsDesc(colCategory,
                        colGranularity, Collections.singletonList(StatsType.NUM_NULLS));
                SQLStatisticsTask sqlTask = new SQLStatisticsTask(job.getId(),
                        Collections.singletonList(colStatsDesc));
                job.getTasks().add(sqlTask);
            }

            // column max value, min value and ndv
            for (String colName : colNames) {
                if (rowCount < backendIds.size() * NDV_MAX_SCAN_PER_TASK) {
                    StatsCategory colCategory = getColumnStatsCategory(job.getDbId(), tableId, partitionName, colName);
                    StatsGranularity colGranularity = getPartitionGranularity(partitionId);
                    StatisticsDesc colStatsDesc = new StatisticsDesc(colCategory,
                            colGranularity, Arrays.asList(StatsType.MAX_VALUE, StatsType.MIN_VALUE, StatsType.NDV));
                    SQLStatisticsTask sqlTask = new SQLStatisticsTask(job.getId(),
                            Collections.singletonList(colStatsDesc));
                    job.getTasks().add(sqlTask);
                } else {
                    // divide subtasks by tablet
                    List<Tablet> tablets = partition.getBaseIndex().getTablets();
                    tablets.forEach(tablet -> {
                        StatsCategory colCategory = getColumnStatsCategory(job.getDbId(),
                                tableId, partitionName, colName);
                        StatsGranularity colGranularity = getTabletGranularity(tablet.getId());
                        StatisticsDesc colStatsDesc = new StatisticsDesc(colCategory,
                                colGranularity, Arrays.asList(StatsType.MAX_VALUE, StatsType.MIN_VALUE, StatsType.NDV));
                        SQLStatisticsTask sqlTask = new SQLStatisticsTask(job.getId(),
                                Collections.singletonList(colStatsDesc));
                        job.getTasks().add(sqlTask);
                    });
                }
            }
        }
    }

    private void getColumnSizeSqlTask(StatisticsJob job, long rowCount,
                                      StatsCategory colCategory, StatsGranularity colGranularity) {
        StatisticsDesc colStatsDesc = new StatisticsDesc(colCategory,
                colGranularity, Arrays.asList(StatsType.MAX_SIZE, StatsType.AVG_SIZE));
        SQLStatisticsTask sqlTask;
        if (rowCount < MIN_SAMPLE_ROWS) {
            sqlTask = new SQLStatisticsTask(job.getId(), Collections.singletonList(colStatsDesc));
        } else {
            sqlTask = new SampleSQLStatisticsTask(job.getId(), Collections.singletonList(colStatsDesc));
        }
        job.getTasks().add(sqlTask);
    }

    private StatsCategory getTableStatsCategory(long dbId, long tableId) {
        StatsCategory category = new StatsCategory();
        category.setCategory(StatsCategory.Category.TABLE);
        category.setDbId(dbId);
        category.setTableId(tableId);
        return category;
    }

    private StatsCategory getPartitionStatsCategory(long dbId, long tableId, String partitionName) {
        StatsCategory category = new StatsCategory();
        category.setCategory(Category.PARTITION);
        category.setDbId(dbId);
        category.setTableId(tableId);
        category.setPartitionName(partitionName);
        return category;
    }

    private StatsCategory getColumnStatsCategory(long dbId, long tableId, String columnName) {
        StatsCategory category = new StatsCategory();
        category.setDbId(dbId);
        category.setTableId(tableId);
        category.setColumnName(columnName);
        category.setCategory(Category.COLUMN);
        category.setColumnName(columnName);
        return category;
    }

    private StatsCategory getColumnStatsCategory(long dbId, long tableId, String partitionName, String columnName) {
        StatsCategory category = new StatsCategory();
        category.setDbId(dbId);
        category.setTableId(tableId);
        category.setPartitionName(partitionName);
        category.setColumnName(columnName);
        category.setCategory(Category.COLUMN);
        category.setColumnName(columnName);
        return category;
    }

    private StatsGranularity getTableGranularity(long tableId) {
        StatsGranularity granularity = new StatsGranularity();
        granularity.setTableId(tableId);
        granularity.setGranularity(Granularity.TABLE);
        return granularity;
    }

    private StatsGranularity getPartitionGranularity(long partitionId) {
        StatsGranularity granularity = new StatsGranularity();
        granularity.setPartitionId(partitionId);
        granularity.setGranularity(Granularity.PARTITION);
        return granularity;
    }

    private StatsGranularity getTabletGranularity(long tabletId) {
        StatsGranularity granularity = new StatsGranularity();
        granularity.setTabletId(tabletId);
        granularity.setGranularity(Granularity.TABLET);
        return granularity;
    }
}

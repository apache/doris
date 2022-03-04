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

import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to store statistics job info,
 * including job status, progress, etc.
 * <p>
 * AnalyzeStmt: Analyze t1(c1, c2)...
 * StatisticsJob:
 * tableId [t1, t2]
 * tableIdToColumnName <t2, [c1,c2,c3]>
 */
public class StatisticsJob {

    public enum JobState {
        PENDING,
        SCHEDULING,
        RUNNING,
        FINISHED,
        CANCELLED,
        FAILED
    }

    private long id;
    private int progress;
    private JobState jobState;

    private final AnalyzeStmt analyzeStmt;

    private long dbId;

    /**
     * to be collected table stats.
     */
    private List<Long> tableIdList;

    /**
     * to be collected column stats.
     */
    private final Map<Long, List<String>> tableIdToColumnName;

    /**
     * to be executed tasks.
     */
    private final List<StatisticsTask> taskList;

    public StatisticsJob(AnalyzeStmt analyzeStmt) throws DdlException {
        this.id = Catalog.getCurrentCatalog().getNextId();
        this.jobState = JobState.PENDING;
        this.progress = 0;
        this.analyzeStmt = analyzeStmt;
        this.tableIdList = Lists.newArrayList();
        this.tableIdToColumnName = Maps.newHashMap();
        this.taskList = Lists.newArrayList();
        this.initializeJob();
    }

    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getProgress() {
        return this.progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public JobState getJobState() {
        return this.jobState;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public long getDbId() {
        return this.dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public List<Long> getTableIdList() {
        return this.tableIdList;
    }

    public void setTableIdList(List<Long> tableIdList) {
        this.tableIdList = tableIdList;
    }

    public Map<Long, List<String>> getTableIdToColumnName() {
        return this.tableIdToColumnName;
    }

    public List<StatisticsTask> getTaskList() {
        return this.taskList;
    }

    /**
     * To calculate statistics including data_size, row count, etc.
     * we'll get table information from analyzeStmt and generate some tasks:
     * @see MetaStatisticsTask
     * @see SampleSQLStatisticsTask
     * @see SQLStatisticsTask
     *
     * @throws DdlException DdlException
     */
    private void initializeJob() throws DdlException {
        Map<String, List<String>> dbTblMap = new HashMap<>();
        TableName dbTableName = this.analyzeStmt.getTableName();
        // step1: get table and column from analyzeStmt
        this.initTableIdToColumn(dbTblMap, dbTableName);
        // step2: check restrict with predefined rules
        this.checkRestrict();
        // step3: check whether the user has table permissions
        this.checkPermission(dbTblMap);
        // step4: now begin to generate tasks
        this.generateTasks();
    }

    private void initTableIdToColumn(Map<String, List<String>> dbTblMap, TableName dbTableName) throws DdlException {
        // analyze table
        if (dbTableName != null) {
            // get dbName
            String dbName = dbTableName.getDb();
            if (StringUtils.isNotBlank(dbName)) {
                dbName = this.analyzeStmt.getClusterName() + ":" + dbName;
            } else {
                dbName = this.analyzeStmt.getAnalyzer().getDefaultDb();
            }

            // check db
            Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbName);
            if (db == null) {
                throw new DdlException("The database(" + dbName + ") does not exist.");
            }

            // check table
            Table table = db.getOlapTableOrDdlException(dbTableName.getTbl());
            if (table == null) {
                throw new DdlException("The table(" + dbTableName.getTbl() + ") does not exist.");
            }

            // check column
            List<String> columnNames = this.analyzeStmt.getColumnNames();
            if (columnNames != null) {
                for (String columnName : columnNames) {
                    Column column = table.getColumn(columnName);
                    if (column == null) {
                        throw new DdlException("The column(" + columnName + ") does not exist.");
                    }
                }
            }

            // if columnNames isEmpty then analyze all columns
            if (columnNames == null || columnNames.isEmpty()) {
                List<Column> baseSchema = table.getBaseSchema();
                columnNames = baseSchema.stream().map(Column::getName).collect(Collectors.toList());
            }

            this.dbId = db.getId();
            long tableId = table.getId();
            this.tableIdList.add(tableId);
            this.tableIdToColumnName.put(tableId, columnNames);
            dbTblMap.put(dbName, Collections.singletonList(table.getName()));

        } else {
            // analyze all tables under the current db
            String dbName = this.analyzeStmt.getAnalyzer().getDefaultDb();
            Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbName);
            this.dbId = db.getId();

            List<Table> tables = db.getTables();
            List<String> tblNames = new ArrayList<>();
            for (Table table : tables) {
                tblNames.add(table.getName());
                long tableId = table.getId();
                this.tableIdList.add(tableId);
                List<Column> baseSchema = table.getBaseSchema();
                List<String> columnNames = baseSchema.stream().map(Column::getName).collect(Collectors.toList());
                this.tableIdToColumnName.put(tableId, columnNames);
            }
            dbTblMap.put(dbName, tblNames);
        }
    }

    private void checkPermission(Map<String, List<String>> dbTblMap) throws DdlException {
        UserIdentity userInfo = this.analyzeStmt.getUserInfo();
        PaloAuth auth = Catalog.getCurrentCatalog().getAuth();

        Set<Map.Entry<String, List<String>>> dbTblSet = dbTblMap.entrySet();
        for (Map.Entry<String, List<String>> dbTbl : dbTblSet) {
            String dbName = dbTbl.getKey();
            boolean dbPermission = auth.checkDbPriv(userInfo, dbName, PrivPredicate.SELECT);
            if (!dbPermission) {
                throw new DdlException("You do not have permissions to analyze the database(" + dbName + ").");
            }
            List<String> tblList = dbTbl.getValue();
            for (String tblName : tblList) {
                boolean tblPermission = auth.checkTblPriv(userInfo, dbName, tblName, PrivPredicate.SELECT);
                if (!tblPermission) {
                    throw new DdlException("You do not have permissions to analyze the table(" + tblName + ").");
                }
            }
        }
    }


    /**
     * Rule1: The job for external table is not supported
     * Rule2: The same table cannot have two unfinished statistics jobs
     * Rule3: The unfinished statistics job could not more then Config.max_statistics_job_num
     *
     * @throws DdlException DdlException
     */
    private void checkRestrict() throws DdlException {
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(this.dbId);
        Set<Long> tableIds = this.tableIdToColumnName.keySet();

        // check table type
        for (Long tableId : tableIds) {
            Table table = db.getTableOrDdlException(tableId);
            if (table.getType() != Table.TableType.OLAP) {
                throw new DdlException("The external table(" + table.getName() + ") is not supported.");
            }
        }

        int unfinishedJobs = 0;
        StatisticsJobScheduler jobScheduler = Catalog.getCurrentCatalog().getStatisticsJobScheduler();
        Queue<StatisticsJob> statisticsJobs = jobScheduler.pendingJobQueue;

        // check table unfinished job
        for (StatisticsJob statisticsJob : statisticsJobs) {
            JobState jobState = statisticsJob.getJobState();
            List<Long> tableIdList = statisticsJob.getTableIdList();
            if (jobState == JobState.PENDING || jobState == JobState.SCHEDULING || jobState == JobState.RUNNING) {
                for (Long tableId : tableIds) {
                    if (tableIdList.contains(tableId)) {
                        throw new DdlException("The same table(id=" + tableId + ") cannot have two unfinished statistics jobs.");
                    }
                }
                unfinishedJobs++;
            }
        }

        // check the number of unfinished tasks
        if (unfinishedJobs > Config.cbo_max_statistics_job_num) {
            throw new DdlException("The unfinished statistics job could not more then Config.cbo_max_statistics_job_num.");
        }
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
     * Calculate:
     * - min, max, ndv: These three full indicators are collected by a sub-task.
     * - max_col_lens, avg_col_lens: Two sampling indicators were collected by a sub-task.
     * <p>
     * If the size of the table is greater than the maximum number of Be scans for a single BE,
     * we'll divide subtasks by partition. relevant values(3700000000L&600000000L) are derived from test.
     *
     *
     * @throws DdlException DdlException
     */
    private void generateTasks() throws DdlException {
        final Database db = Catalog.getCurrentCatalog().getDbOrDdlException(this.dbId);
        final Set<Long> tableIds = this.tableIdToColumnName.keySet();
        final List<Long> backendIds = Catalog.getCurrentSystemInfo().getBackendIds(true);

        for (Long tableId : tableIds) {
            final Table tbl = db.getTableOrDdlException(tableId);
            final long rowCount = tbl.getRowCount();
            final List<Long> partitionIds = ((OlapTable) tbl).getPartitionIds();
            final List<String> columnNameList = this.tableIdToColumnName.get(tableId);

            // generate data size task
            StatsCategoryDesc dataSizeCategory = this.getTblStatsCategoryDesc(tableId);
            StatsGranularityDesc dataSizeGranularity = this.getTblStatsGranularityDesc(tableId);
            MetaStatisticsTask dataSizeTask = new MetaStatisticsTask(this.id,
                    dataSizeGranularity, dataSizeCategory, Collections.singletonList(StatsType.DATA_SIZE));
            this.taskList.add(dataSizeTask);

            // generate row count task
            KeysType keysType = ((OlapTable) tbl).getKeysType();
            if (keysType == KeysType.DUP_KEYS) {
                StatsCategoryDesc rowCountCategory = this.getTblStatsCategoryDesc(tableId);
                StatsGranularityDesc rowCountGranularity = this.getTblStatsGranularityDesc(tableId);
                MetaStatisticsTask metaTask = new MetaStatisticsTask(this.id,
                        rowCountGranularity, rowCountCategory, Collections.singletonList(StatsType.ROW_COUNT));
                this.taskList.add(metaTask);
            } else {
                // 如果表 size > 单个 be 最大扫描量 * be 个数，按照 partition 进行分割子任务
                if (rowCount > backendIds.size() * 3700000000L) {
                    for (Long partitionId : partitionIds) {
                        StatsCategoryDesc rowCountCategory = this.getTblStatsCategoryDesc(tableId);
                        StatsGranularityDesc rowCountGranularity = this.getPartitionStatsGranularityDesc(tableId, partitionId);
                        SQLStatisticsTask sqlTask = new SQLStatisticsTask(this.id,
                                rowCountGranularity, rowCountCategory, Collections.singletonList(StatsType.ROW_COUNT));
                        this.taskList.add(sqlTask);
                    }
                } else {
                    StatsCategoryDesc rowCountCategory = this.getTblStatsCategoryDesc(tableId);
                    StatsGranularityDesc rowCountGranularity = this.getTblStatsGranularityDesc(tableId);
                    SQLStatisticsTask sqlTask = new SQLStatisticsTask(this.id,
                            rowCountGranularity, rowCountCategory, Collections.singletonList(StatsType.ROW_COUNT));
                    this.taskList.add(sqlTask);
                }

                // generate [min,max,ndv] task
                if (rowCount > backendIds.size() * 600000000L) {
                    for (String columnName : columnNameList) {
                        for (Long partitionId : partitionIds) {
                            StatsCategoryDesc columnCategory = this.getColStatsCategoryDesc(this.dbId, tableId, columnName);
                            StatsGranularityDesc columnGranularity = this.getPartitionStatsGranularityDesc(tableId, partitionId);
                            List<StatsType> statsTypes = Arrays.asList(StatsType.MIN_VALUE, StatsType.MAX_VALUE, StatsType.NDV);
                            SQLStatisticsTask sqlTask = new SQLStatisticsTask(this.id, columnGranularity, columnCategory, statsTypes);
                            this.taskList.add(sqlTask);
                        }
                    }
                } else {
                    for (String columnName : columnNameList) {
                        StatsCategoryDesc columnCategory = this.getColStatsCategoryDesc(this.dbId, tableId, columnName);
                        StatsGranularityDesc columnGranularity = this.getTblStatsGranularityDesc(tableId);
                        List<StatsType> statsTypes = Arrays.asList(StatsType.MIN_VALUE, StatsType.MAX_VALUE, StatsType.NDV);
                        SQLStatisticsTask sqlTask = new SQLStatisticsTask(this.id, columnGranularity, columnCategory, statsTypes);
                        this.taskList.add(sqlTask);
                    }
                }

                // generate num_nulls task
                for (String columnName : columnNameList) {
                    StatsCategoryDesc columnCategory = this.getColStatsCategoryDesc(this.dbId, tableId, columnName);
                    StatsGranularityDesc columnGranularity = this.getTblStatsGranularityDesc(tableId);
                    SQLStatisticsTask sqlTask = new SQLStatisticsTask(this.id,
                            columnGranularity, columnCategory, Collections.singletonList(StatsType.NUM_NULLS));
                    this.taskList.add(sqlTask);
                }

                // generate [max_col_lens, avg_col_lens] task
                for (String columnName : columnNameList) {
                    StatsCategoryDesc columnCategory = this.getColStatsCategoryDesc(this.dbId, tableId, columnName);
                    StatsGranularityDesc columnGranularity = this.getTblStatsGranularityDesc(tableId);
                    List<StatsType> statsTypes = Arrays.asList(StatsType.MAX_SIZE, StatsType.AVG_SIZE);
                    Column column = tbl.getColumn(columnName);
                    Type colType = column.getType();
                    if (colType.isStringType()) {
                        SQLStatisticsTask sampleSqlTask = new SampleSQLStatisticsTask(this.id, columnGranularity, columnCategory, statsTypes);
                        this.taskList.add(sampleSqlTask);
                    } else {
                        MetaStatisticsTask metaTask = new MetaStatisticsTask(this.id, columnGranularity, columnCategory, statsTypes);
                        this.taskList.add(metaTask);
                    }
                }
            }
        }
    }

    private StatsCategoryDesc getTblStatsCategoryDesc(Long tableId) {
        StatsCategoryDesc statsCategoryDesc = new StatsCategoryDesc();
        statsCategoryDesc.setCategory(StatsCategoryDesc.StatsCategory.TABLE);
        statsCategoryDesc.setDbId(this.dbId);
        statsCategoryDesc.setTableId(tableId);
        return statsCategoryDesc;
    }

    @NotNull
    private StatsCategoryDesc getColStatsCategoryDesc(long dbId, long tableId, String columnName) {
        StatsCategoryDesc statsCategoryDesc = new StatsCategoryDesc();
        statsCategoryDesc.setDbId(dbId);
        statsCategoryDesc.setTableId(tableId);
        statsCategoryDesc.setCategory(StatsCategoryDesc.StatsCategory.COLUMN);
        statsCategoryDesc.setColumnName(columnName);
        return statsCategoryDesc;
    }

    @NotNull
    private StatsGranularityDesc getTblStatsGranularityDesc(long tableId) {
        StatsGranularityDesc statsGranularityDesc = new StatsGranularityDesc();
        statsGranularityDesc.setTableId(tableId);
        statsGranularityDesc.setGranularity(StatsGranularityDesc.StatsGranularity.TABLE);
        return statsGranularityDesc;
    }

    @NotNull
    private StatsGranularityDesc getPartitionStatsGranularityDesc(long tableId, long partitionId) {
        StatsGranularityDesc statsGranularityDesc = new StatsGranularityDesc();
        statsGranularityDesc.setTableId(tableId);
        statsGranularityDesc.setPartitionId(partitionId);
        statsGranularityDesc.setGranularity(StatsGranularityDesc.StatsGranularity.PARTITION);
        return statsGranularityDesc;
    }

    @NotNull
    private StatsGranularityDesc getTabletStatsGranularityDesc(long tableId) {
        StatsGranularityDesc statsGranularityDesc = new StatsGranularityDesc();
        statsGranularityDesc.setTableId(tableId);
        statsGranularityDesc.setGranularity(StatsGranularityDesc.StatsGranularity.PARTITION);
        return statsGranularityDesc;
    }
}

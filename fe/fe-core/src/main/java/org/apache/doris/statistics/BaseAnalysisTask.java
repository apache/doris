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

import org.apache.doris.analysis.TableSample;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.util.DBObjects;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public abstract class BaseAnalysisTask {

    public static final Logger LOG = LogManager.getLogger(BaseAnalysisTask.class);

    public static final long LIMIT_SIZE = 1024 * 1024 * 1024; // 1GB
    public static final double LIMIT_FACTOR = 1.2;

    protected static final String FULL_ANALYZE_TEMPLATE =
            "SELECT CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS `id`, "
            + "         ${catalogId} AS `catalog_id`, "
            + "         ${dbId} AS `db_id`, "
            + "         ${tblId} AS `tbl_id`, "
            + "         ${idxId} AS `idx_id`, "
            + "         '${colId}' AS `col_id`, "
            + "         NULL AS `part_id`, "
            + "         COUNT(1) AS `row_count`, "
            + "         NDV(`${colName}`) AS `ndv`, "
            + "         COUNT(1) - COUNT(`${colName}`) AS `null_count`, "
            + "         SUBSTRING(CAST(MIN(`${colName}`) AS STRING), 1, 1024) AS `min`, "
            + "         SUBSTRING(CAST(MAX(`${colName}`) AS STRING), 1, 1024) AS `max`, "
            + "         ${dataSizeFunction} AS `data_size`, "
            + "         NOW() AS `update_time` "
            + " FROM `${catalogName}`.`${dbName}`.`${tblName}` ${index}";

    protected static final String LINEAR_ANALYZE_TEMPLATE = " SELECT "
            + "CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS `id`, "
            + "${catalogId} AS `catalog_id`, "
            + "${dbId} AS `db_id`, "
            + "${tblId} AS `tbl_id`, "
            + "${idxId} AS `idx_id`, "
            + "'${colId}' AS `col_id`, "
            + "NULL AS `part_id`, "
            + "${rowCount} AS `row_count`, "
            + "${ndvFunction} as `ndv`, "
            + "ROUND(SUM(CASE WHEN `${colName}` IS NULL THEN 1 ELSE 0 END) * ${scaleFactor}) AS `null_count`, "
            + "SUBSTRING(CAST(${min} AS STRING), 1, 1024) AS `min`, "
            + "SUBSTRING(CAST(${max} AS STRING), 1, 1024) AS `max`, "
            + "${dataSizeFunction} * ${scaleFactor} AS `data_size`, "
            + "NOW() "
            + "FROM `${catalogName}`.`${dbName}`.`${tblName}` ${index} ${sampleHints} ${limit}";

    protected static final String DUJ1_ANALYZE_STRING_TEMPLATE = "SELECT "
            + "CONCAT('${tblId}', '-', '${idxId}', '-', '${colId}') AS `id`, "
            + "${catalogId} AS `catalog_id`, "
            + "${dbId} AS `db_id`, "
            + "${tblId} AS `tbl_id`, "
            + "${idxId} AS `idx_id`, "
            + "'${colId}' AS `col_id`, "
            + "NULL AS `part_id`, "
            + "${rowCount} AS `row_count`, "
            + "${ndvFunction} as `ndv`, "
            + "IFNULL(SUM(IF(`t1`.`column_key` IS NULL, `t1`.`count`, 0)), 0) * ${scaleFactor} as `null_count`, "
            + "SUBSTRING(CAST(${min} AS STRING), 1, 1024) AS `min`, "
            + "SUBSTRING(CAST(${max} AS STRING), 1, 1024) AS `max`, "
            + "${dataSizeFunction} * ${scaleFactor} AS `data_size`, "
            + "NOW() "
            + "FROM ( "
            + "    SELECT t0.`colValue` as `column_key`, COUNT(1) as `count` "
            + "    FROM "
            + "    (SELECT SUBSTRING(CAST(`${colName}` AS STRING), 1, 1024) AS `colValue` "
            + "         FROM `${catalogName}`.`${dbName}`.`${tblName}` ${index} "
            + "    ${sampleHints} ${limit}) as `t0` "
            + "    GROUP BY `t0`.`colValue` "
            + ") as `t1` ";

    protected static final String DUJ1_ANALYZE_TEMPLATE = "SELECT "
            + "CONCAT('${tblId}', '-', '${idxId}', '-', '${colId}') AS `id`, "
            + "${catalogId} AS `catalog_id`, "
            + "${dbId} AS `db_id`, "
            + "${tblId} AS `tbl_id`, "
            + "${idxId} AS `idx_id`, "
            + "'${colId}' AS `col_id`, "
            + "NULL AS `part_id`, "
            + "${rowCount} AS `row_count`, "
            + "${ndvFunction} as `ndv`, "
            + "IFNULL(SUM(IF(`t1`.`column_key` IS NULL, `t1`.`count`, 0)), 0) * ${scaleFactor} as `null_count`, "
            + "SUBSTRING(CAST(${min} AS STRING), 1, 1024) AS `min`, "
            + "SUBSTRING(CAST(${max} AS STRING), 1, 1024) AS `max`, "
            + "${dataSizeFunction} * ${scaleFactor} AS `data_size`, "
            + "NOW() "
            + "FROM ( "
            + "    SELECT t0.`${colName}` as `column_key`, COUNT(1) as `count` "
            + "    FROM "
            + "    (SELECT `${colName}` FROM `${catalogName}`.`${dbName}`.`${tblName}` ${index} "
            + "    ${sampleHints} ${limit}) as `t0` "
            + "    GROUP BY `t0`.`${colName}` "
            + ") as `t1` ";

    protected static final String ANALYZE_PARTITION_COLUMN_TEMPLATE = " SELECT "
            + "CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS `id`, "
            + "${catalogId} AS `catalog_id`, "
            + "${dbId} AS `db_id`, "
            + "${tblId} AS `tbl_id`, "
            + "${idxId} AS `idx_id`, "
            + "'${colId}' AS `col_id`, "
            + "NULL AS `part_id`, "
            + "${row_count} AS `row_count`, "
            + "${ndv} AS `ndv`, "
            + "${null_count} AS `null_count`, "
            + "SUBSTRING(CAST(${min} AS STRING), 1, 1024) AS `min`, "
            + "SUBSTRING(CAST(${max} AS STRING), 1, 1024) AS `max`, "
            + "${data_size} AS `data_size`, "
            + "NOW() ";

    protected static final String PARTITION_ANALYZE_TEMPLATE = " SELECT "
            + "${catalogId} AS `catalog_id`, "
            + "${dbId} AS `db_id`, "
            + "${tblId} AS `tbl_id`, "
            + "${idxId} AS `idx_id`, "
            + "${partName} AS `part_name`, "
            + "${partId} AS `part_id`, "
            + "'${colId}' AS `col_id`, "
            + "COUNT(1) AS `row_count`, "
            + "HLL_UNION(HLL_HASH(`${colName}`)) as ndv, "
            + "COUNT(1) - COUNT(`${colName}`) AS `null_count`, "
            + "SUBSTRING(CAST(MIN(`${colName}`) AS STRING), 1, 1024) AS `min`, "
            + "SUBSTRING(CAST(MAX(`${colName}`) AS STRING), 1, 1024) AS `max`, "
            + "${dataSizeFunction} AS `data_size`, "
            + "NOW() AS `update_time` "
            + " FROM `${catalogName}`.`${dbName}`.`${tblName}` ${index} ${partitionInfo}";

    protected static final String MERGE_PARTITION_TEMPLATE =
            "SELECT CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS `id`, "
            + "${catalogId} AS `catalog_id`, "
            + "${dbId} AS `db_id`, "
            + "${tblId} AS `tbl_id`, "
            + "${idxId} AS `idx_id`, "
            + "'${colId}' AS `col_id`, "
            + "NULL AS `part_id`, "
            + "SUM(count) AS `row_count`, "
            + "HLL_CARDINALITY(HLL_UNION(ndv)) AS `ndv`, "
            + "SUM(null_count) AS `null_count`, "
            + "MIN(${min}) AS `min`, "
            + "MAX(${max}) AS `max`, "
            + "SUM(data_size_in_bytes) AS `data_size`, "
            + "NOW() AS `update_time` FROM "
            + StatisticConstants.FULL_QUALIFIED_PARTITION_STATS_TBL_NAME
            + " WHERE `catalog_id` = ${catalogId} "
            + " AND `db_id` = ${dbId} "
            + " AND `tbl_id` = ${tblId} "
            + " AND `idx_id` = ${idxId} "
            + " AND `col_id` = '${colId}'";

    protected AnalysisInfo info;

    protected CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog;

    protected DatabaseIf<? extends TableIf> db;

    protected TableIf tbl;

    protected Column col;

    protected StmtExecutor stmtExecutor;

    protected volatile boolean killed;

    protected TableSample tableSample = null;

    protected AnalysisJob job;

    @VisibleForTesting
    public BaseAnalysisTask() {

    }

    public BaseAnalysisTask(AnalysisInfo info) {
        this.info = info;
        init(info);
    }

    protected void init(AnalysisInfo info) {
        DBObjects dbObjects = StatisticsUtil.convertIdToObjects(info.catalogId, info.dbId, info.tblId);
        catalog = dbObjects.catalog;
        db = dbObjects.db;
        tbl = dbObjects.table;
        tableSample = getTableSample();
        if (info.analysisType != null && (info.analysisType.equals(AnalysisType.FUNDAMENTALS)
                || info.analysisType.equals(AnalysisType.HISTOGRAM))) {
            col = tbl.getColumn(info.colName);
            if (col == null) {
                throw new RuntimeException(String.format("Column with name %s not exists", tbl.getName()));
            }
            Preconditions.checkArgument(!StatisticsUtil.isUnsupportedType(col.getType()),
                    String.format("Column with type %s is not supported", col.getType().toString()));
        }
    }

    public void execute() throws Exception {
        prepareExecution();
        doExecute();
        afterExecution();
    }

    protected void prepareExecution() {
        setTaskStateToRunning();
    }

    public abstract void doExecute() throws Exception;

    protected abstract void doSample() throws Exception;

    protected void afterExecution() {}

    protected void setTaskStateToRunning() {
        Env.getCurrentEnv().getAnalysisManager()
                .updateTaskStatus(info, AnalysisState.RUNNING, "", System.currentTimeMillis());
    }

    public void cancel() {
        killed = true;
        if (stmtExecutor != null) {
            stmtExecutor.cancel();
        }
        Env.getCurrentEnv().getAnalysisManager()
                .updateTaskStatus(info, AnalysisState.FAILED,
                        String.format("Job has been cancelled: %s", info.message), System.currentTimeMillis());
    }

    public long getJobId() {
        return info.jobId;
    }

    protected String getDataSizeFunction(Column column, boolean useDuj1) {
        if (useDuj1) {
            if (column.getType().isStringType()) {
                return "SUM(LENGTH(`column_key`) * count)";
            } else {
                return "SUM(t1.count) * " + column.getType().getSlotSize();
            }
        } else {
            if (column.getType().isStringType()) {
                return "SUM(LENGTH(`${colName}`))";
            } else {
                return "COUNT(1) * " + column.getType().getSlotSize();
            }
        }
    }

    protected String getMinFunction() {
        if (tableSample == null) {
            return "CAST(MIN(`${colName}`) as ${type}) ";
        } else {
            // Min value is not accurate while sample, so set it to NULL to avoid optimizer generate bad plan.
            return "NULL";
        }
    }

    protected String getNdvFunction(String totalRows) {
        String sampleRows = "SUM(`t1`.`count`)";
        String onceCount = "SUM(IF(`t1`.`count` = 1, 1, 0))";
        String countDistinct = "COUNT(1)";
        // DUJ1 estimator: n*d / (n - f1 + f1*n/N)
        // f1 is the count of element that appears only once in the sample.
        // (https://github.com/postgres/postgres/blob/master/src/backend/commands/analyze.c)
        // (http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.93.8637&rep=rep1&type=pdf)
        // sample_row * count_distinct / ( sample_row - once_count + once_count * sample_row / total_row)
        return MessageFormat.format("{0} * {1} / ({0} - {2} + {2} * {0} / {3})", sampleRows,
                countDistinct, onceCount, totalRows);
    }

    // Max value is not accurate while sample, so set it to NULL to avoid optimizer generate bad plan.
    protected String getMaxFunction() {
        if (tableSample == null) {
            return "CAST(MAX(`${colName}`) as ${type}) ";
        } else {
            return "NULL";
        }
    }

    protected TableSample getTableSample() {
        if (info.forceFull) {
            return null;
        }
        // If user specified sample percent or sample rows, use it.
        if (info.samplePercent > 0) {
            return new TableSample(true, (long) info.samplePercent);
        } else if (info.sampleRows > 0) {
            return new TableSample(false, info.sampleRows);
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return String.format("Job id [%d], Task id [%d], catalog [%s], db [%s], table [%s], column [%s]",
                info.jobId, info.taskId, catalog.getName(), db.getFullName(), tbl.getName(),
                col == null ? "TableRowCount" : col.getName());
    }

    public void setJob(AnalysisJob job) {
        this.job = job;
    }

    /**
     * 1. Remove not exist partition stats
     * 2. Get stats of each partition
     * 3. insert partition in batch
     * 4. Skip large partition and fallback to sample analyze if large partition exists.
     * 5. calculate column stats based on partition stats
     */
    protected void doPartitionTable() throws Exception {
        AnalysisManager analysisManager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tableStatsStatus = analysisManager.findTableStatsStatus(tbl.getId());
        // Find jobInfo for this task.
        AnalysisInfo jobInfo = analysisManager.findJobInfo(job.getJobInfo().jobId);
        // For sync job, get jobInfo from job.jobInfo.
        boolean isSync = jobInfo == null;
        jobInfo = isSync ? job.jobInfo : jobInfo;

        // 1. Remove not exist partition stats
        deleteNotExistPartitionStats(jobInfo);

        // 2. Get stats of each partition
        Map<String, String> params = buildSqlParams();
        params.put("dataSizeFunction", getDataSizeFunction(col, false));
        boolean isAllPartitions = info.partitionNames.isEmpty();
        Set<String> partitionNames = isAllPartitions ? tbl.getPartitionNames() : info.partitionNames;
        List<String> sqls = Lists.newArrayList();
        Set<String> partNames = Sets.newHashSet();
        int count = 0;
        long batchRowCount = 0;
        String idxName = info.indexId == -1 ? tbl.getName() : ((OlapTable) tbl).getIndexNameById(info.indexId);
        ColStatsMeta columnStatsMeta = tableStatsStatus == null
                ? null : tableStatsStatus.findColumnStatsMeta(idxName, col.getName());
        boolean hasHughPartition = false;
        long hugePartitionThreshold = StatisticsUtil.getHugePartitionLowerBoundRows();
        int partitionBatchSize = StatisticsUtil.getPartitionAnalyzeBatchSize();
        for (String part : partitionNames) {
            // External table partition is null.
            Partition partition = tbl.getPartition(part);
            if (partition != null) {
                // For huge partition, skip analyze it.
                long partitionRowCount = partition.getBaseIndex().getRowCount();
                if (partitionRowCount > hugePartitionThreshold && AnalysisInfo.JobType.SYSTEM.equals(info.jobType)) {
                    hasHughPartition = true;
                    // -1 means it's skipped because this partition is too large.
                    jobInfo.partitionUpdateRows.putIfAbsent(partition.getId(), -1L);
                    LOG.info("Partition {} in table {} is too large, skip it.", part, tbl.getName());
                    continue;
                }
                batchRowCount += partitionRowCount;
                // For cluster upgrade compatible (older version metadata doesn't have partition update rows map)
                // and insert before first analyze, set partition update rows to 0.
                jobInfo.partitionUpdateRows.putIfAbsent(partition.getId(), 0L);
            }
            params.put("partId", partition == null ? "-1" : String.valueOf(partition.getId()));
            // Skip partitions that not changed after last analyze.
            // External table getPartition always return null. So external table doesn't skip any partitions.
            if (partition != null && tableStatsStatus != null && tableStatsStatus.partitionUpdateRows != null) {
                ConcurrentMap<Long, Long> tableUpdateRows = tableStatsStatus.partitionUpdateRows;
                if (columnStatsMeta != null && columnStatsMeta.partitionUpdateRows != null) {
                    ConcurrentMap<Long, Long> columnUpdateRows = columnStatsMeta.partitionUpdateRows;
                    long id = partition.getId();
                    if (Objects.equals(tableUpdateRows.getOrDefault(id, 0L), columnUpdateRows.get(id))) {
                        LOG.debug("Partition {} doesn't change after last analyze for column {}, skip it.",
                                part, col.getName());
                        continue;
                    }
                }
            }
            params.put("partName", "'" + StatisticsUtil.escapeColumnName(part) + "'");
            params.put("partitionInfo", getPartitionInfo(part));
            StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
            sqls.add(stringSubstitutor.replace(PARTITION_ANALYZE_TEMPLATE));
            count++;
            partNames.add(part);
            // 3. insert partition in batch
            if (count == partitionBatchSize || batchRowCount > hugePartitionThreshold) {
                String sql = "INSERT INTO " + StatisticConstants.FULL_QUALIFIED_PARTITION_STATS_TBL_NAME
                        + Joiner.on(" UNION ALL ").join(sqls);
                runInsert(sql);
                analysisManager.updatePartitionStatsCache(info.catalogId, info.dbId, info.tblId, info.indexId,
                        partNames, info.colName);
                partNames.clear();
                sqls.clear();
                count = 0;
                batchRowCount = 0;
            }
        }
        // 3. insert partition in batch
        if (count > 0) {
            String sql = "INSERT INTO " + StatisticConstants.FULL_QUALIFIED_PARTITION_STATS_TBL_NAME
                    + Joiner.on(" UNION ALL ").join(sqls);
            runInsert(sql);
            analysisManager.updatePartitionStatsCache(info.catalogId, info.dbId, info.tblId, info.indexId,
                    partNames, info.colName);
        }
        // 4. Skip large partition and fallback to sample analyze if large partition exists.
        if (hasHughPartition) {
            tableSample = new TableSample(false, StatisticsUtil.getHugeTableSampleRows());
            if (!isSync) {
                long startTime = jobInfo.startTime;
                jobInfo = new AnalysisInfoBuilder(jobInfo).setAnalysisMethod(AnalysisMethod.SAMPLE).build();
                jobInfo.markStartTime(startTime);
                analysisManager.replayCreateAnalysisJob(jobInfo);
            }
            if (tableStatsStatus == null || columnStatsMeta == null
                    || tableStatsStatus.updatedRows.get() > columnStatsMeta.updatedRows) {
                doSample();
            } else {
                job.taskDoneWithoutData(this);
            }
        } else {
            // 5. calculate column stats based on partition stats
            if (isAllPartitions) {
                params = buildSqlParams();
                params.put("min", castToNumeric("min"));
                params.put("max", castToNumeric("max"));
                StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
                runQuery(stringSubstitutor.replace(MERGE_PARTITION_TEMPLATE));
            } else {
                job.taskDoneWithoutData(this);
            }
        }
    }

    protected abstract void deleteNotExistPartitionStats(AnalysisInfo jobInfo) throws DdlException;

    protected String getPartitionInfo(String partitionName) {
        return "";
    }

    protected Map<String, String> buildSqlParams() {
        return Maps.newHashMap();
    }

    protected String castToNumeric(String colName) {
        Type type = col.getType();
        if (type.isNumericType()) {
            return "CAST(" + colName + " AS " + type.toSql() + ")";
        } else {
            return colName;
        }
    }

    protected void runQuery(String sql) {
        long startTime = System.currentTimeMillis();
        String queryId = "";
        try (AutoCloseConnectContext a  = StatisticsUtil.buildConnectContext()) {
            stmtExecutor = new StmtExecutor(a.connectContext, sql);
            ColStatsData colStatsData = new ColStatsData(stmtExecutor.executeInternalQuery().get(0));
            // Update index row count after analyze.
            if (this instanceof OlapAnalysisTask) {
                AnalysisInfo jobInfo = Env.getCurrentEnv().getAnalysisManager().findJobInfo(job.getJobInfo().jobId);
                // For sync job, get jobInfo from job.jobInfo.
                jobInfo = jobInfo == null ? job.jobInfo : jobInfo;
                long indexId = info.indexId == -1 ? ((OlapTable) tbl).getBaseIndexId() : info.indexId;
                jobInfo.addIndexRowCount(indexId, colStatsData.count);
            }
            Env.getCurrentEnv().getStatisticsCache().syncColStats(colStatsData);
            queryId = DebugUtil.printId(stmtExecutor.getContext().queryId());
            job.appendBuf(this, Collections.singletonList(colStatsData));
        } catch (Exception e) {
            LOG.warn("Failed to execute sql {}", sql);
            throw e;
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.debug("End cost time in millisec: " + (System.currentTimeMillis() - startTime)
                        + " Analyze SQL: " + sql + " QueryId: " + queryId);
            }
            // Release the reference to stmtExecutor, reduce memory usage.
            stmtExecutor = null;
        }
    }

    protected void runInsert(String sql) throws Exception {
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
            stmtExecutor = new StmtExecutor(r.connectContext, sql);
            try {
                stmtExecutor.execute();
                QueryState queryState = stmtExecutor.getContext().getState();
                if (queryState.getStateType().equals(QueryState.MysqlStateType.ERR)) {
                    throw new RuntimeException(
                        "Failed to insert : " + stmtExecutor.getOriginStmt().originStmt + "Error msg: "
                            + queryState.getErrorMessage());
                }
            } finally {
                AuditLogHelper.logAuditLog(stmtExecutor.getContext(), stmtExecutor.getOriginStmt().toString(),
                        stmtExecutor.getParsedStmt(), stmtExecutor.getQueryStatisticsForAuditLog(), true);
            }
        }
    }
}

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

import org.apache.doris.catalog.Partition;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.text.StringSubstitutor;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Each task analyze one column.
 */
public class OlapAnalysisTask extends BaseAnalysisTask {

    // TODO Currently, NDV is computed for the full table; in fact,
    //  NDV should only be computed for the relevant partition.
    private static final String ANALYZE_COLUMN_SQL_TEMPLATE = INSERT_COL_STATISTICS
            + "     (SELECT NDV(`${colName}`) AS ndv "
            + "     FROM `${dbName}`.`${tblName}` ${sampleExpr}) t2\n";

    private static final String collectPartitionStatsSQLTemplate =
            " SELECT "
                    + "CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}', '-', ${partId}) AS id, "
                    + "${catalogId} AS catalog_id, "
                    + "${dbId} AS db_id, "
                    + "${tblId} AS tbl_id, "
                    + "${idxId} AS idx_id, "
                    + "'${colId}' AS col_id, "
                    + "${partId} AS part_id, "
                    + "COUNT(1) AS row_count, "
                    + "NDV(`${colName}`) AS ndv, "
                    + "SUM(CASE WHEN `${colName}` IS NULL THEN 1 ELSE 0 END) AS null_count, "
                    + "MIN(`${colName}`) AS min, "
                    + "MAX(`${colName}`) AS max, "
                    + "${dataSizeFunction} AS data_size, "
                    + "NOW() FROM `${dbName}`.`${tblName}` PARTITION ${partitionName}  ${sampleExpr}";

    // cache stats for each partition, it would be inserted into column_statistics in a batch.
    private final List<List<ColStatsData>> buf = new ArrayList<>();

    @VisibleForTesting
    public OlapAnalysisTask() {
    }

    public OlapAnalysisTask(AnalysisInfo info) {
        super(info);
    }

    public void doExecute() throws Exception {
        Set<String> partitionNames = info.colToPartitions.get(info.colName);
        if (partitionNames.isEmpty()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", String.valueOf(info.indexId));
        params.put("colId", String.valueOf(info.colName));
        params.put("dataSizeFunction", getDataSizeFunction(col));
        params.put("dbName", info.dbName);
        params.put("colName", String.valueOf(info.colName));
        params.put("tblName", String.valueOf(info.tblName));
        params.put("sampleExpr", getSampleExpression());
        List<String> partitionAnalysisSQLs = new ArrayList<>();
        try {
            tbl.readLock();

            for (String partitionName : partitionNames) {
                Partition part = tbl.getPartition(partitionName);
                if (part == null) {
                    continue;
                }
                params.put("partId", String.valueOf(tbl.getPartition(partitionName).getId()));
                // Avoid error when get the default partition
                params.put("partitionName", "`" + partitionName + "`");
                StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
                partitionAnalysisSQLs.add(stringSubstitutor.replace(collectPartitionStatsSQLTemplate));
            }
        } finally {
            tbl.readUnlock();
        }
        execSQLs(partitionAnalysisSQLs, params);
    }

    @VisibleForTesting
    public void execSQLs(List<String> partitionAnalysisSQLs, Map<String, String> params) throws Exception {
        long startTime = System.currentTimeMillis();
        LOG.debug("analyze task {} start at {}", info.toString(), new Date());
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(info.jobType.equals(JobType.SYSTEM))) {
            List<List<String>> sqlGroups = Lists.partition(partitionAnalysisSQLs, StatisticConstants.UNION_ALL_LIMIT);
            for (List<String> group : sqlGroups) {
                if (killed) {
                    return;
                }
                StringJoiner partitionCollectSQL = new StringJoiner("UNION ALL");
                group.forEach(partitionCollectSQL::add);
                stmtExecutor = new StmtExecutor(r.connectContext, partitionCollectSQL.toString());
                buf.add(stmtExecutor.executeInternalQuery()
                        .stream().map(ColStatsData::new).collect(Collectors.toList()));
                QueryState queryState = r.connectContext.getState();
                if (queryState.getStateType().equals(MysqlStateType.ERR)) {
                    throw new RuntimeException(String.format("Failed to analyze %s.%s.%s, error: %s sql: %s",
                            info.catalogName, info.dbName, info.colName, partitionCollectSQL,
                            queryState.getErrorMessage()));
                }
            }
            for (List<ColStatsData> colStatsDataList : buf) {
                StringBuilder batchInsertSQL =
                        new StringBuilder("INSERT INTO " + StatisticConstants.FULL_QUALIFIED_STATS_TBL_NAME
                                + " VALUES ");
                StringJoiner sj = new StringJoiner(",");
                colStatsDataList.forEach(c -> sj.add(c.toSQL(true)));
                batchInsertSQL.append(sj.toString());
                stmtExecutor = new StmtExecutor(r.connectContext, batchInsertSQL.toString());
                executeWithExceptionOnFail(stmtExecutor);
            }
            params.put("type", col.getType().toString());
            StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
            String sql = stringSubstitutor.replace(ANALYZE_COLUMN_SQL_TEMPLATE);
            stmtExecutor = new StmtExecutor(r.connectContext, sql);
            executeWithExceptionOnFail(stmtExecutor);
        } finally {
            LOG.debug("analyze task {} end. cost {}ms", info,
                    System.currentTimeMillis() - startTime);
        }

    }
}

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
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.text.StringSubstitutor;

import java.security.SecureRandom;
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
            + "     FROM `${dbName}`.`${tblName}`) t2";

    private static final String COLLECT_PARTITION_STATS_SQL_TEMPLATE =
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
                    + "NOW() FROM `${dbName}`.`${tblName}` PARTITION ${partitionName}";

    private static final String SAMPLE_COLUMN_SQL_TEMPLATE = "SELECT "
            + "CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "${catalogId} AS catalog_id, "
            + "${dbId} AS db_id, "
            + "${tblId} AS tbl_id, "
            + "${idxId} AS idx_id, "
            + "'${colId}' AS col_id, "
            + "NULL AS part_id, "
            + "NDV(`${colName}`) AS ndv, "
            + "SUM(CASE WHEN `${colName}` IS NULL THEN 1 ELSE 0 END) * ${scaleFactor} AS null_count, "
            + "${dataSizeFunction} * ${scaleFactor} AS data_size, "
            + "NOW() "
            + "FROM "
            + "`${dbName}`.`${tblName}` "
            + "${tablets} ";

    private static final String BASIC_STATS_TEMPLATE = "SELECT "
            + "COUNT(1) as row_count, "
            + "MIN(`${colName}`) as min, "
            + "MAX(`${colName}`) as max "
            + "FROM `${dbName}`.`${tblName}`";

    // cache stats for each partition, it would be inserted into column_statistics in a batch.
    private final List<List<ColStatsData>> buf = new ArrayList<>();

    @VisibleForTesting
    public OlapAnalysisTask() {
    }

    public OlapAnalysisTask(AnalysisInfo info) {
        super(info);
    }

    public void doExecute() throws Exception {

        if (tableSample != null) {
            doSample();
        } else {
            doFull();
        }
    }

    /**
     * 1. Get col stats in sample ways twice with sample rows and (sample rows)/2
     * 2. Calculate col stats using the two values sampled.
     * 3. insert col stats.
     */
    protected void doSample() throws Exception {
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(info.jobType.equals(JobType.SYSTEM))) {
            ResultRow basicStats = collectBasicStat(r);
            Pair<List<Long>, Long> pair1 = calcActualSampleTablets(tableSample, true);

            // Second sample rate is half of the first time.
            // Don't set seek value to get random result, avoid sample a subset of the first sample.
            TableSample halfSample = new TableSample(tableSample.isPercent(),
                    Math.max(tableSample.getSampleValue() / 2, 1));
            Pair<List<Long>, Long> pair2 = calcActualSampleTablets(halfSample, false);

            Pair<ColStatsData, Double> colStatsDataDoublePair1;
            Pair<ColStatsData, Double> colStatsDataDoublePair2;
            if (pair1.first.isEmpty()) {
                // If the first pair's tablet id list is empty, will sample the full table, doesn't need second sample.
                colStatsDataDoublePair1 = sampleOnce(1, pair1.first, r, basicStats);
                colStatsDataDoublePair2 = colStatsDataDoublePair1;
            } else {
                long rowCount = tbl.getRowCount();
                double scaleFactor1 = (double) rowCount / (double) pair1.second;
                double scaleFactor2 = (double) rowCount / (double) pair2.second;
                colStatsDataDoublePair1 = sampleOnce(scaleFactor1, pair1.first, r, basicStats);
                colStatsDataDoublePair2 = sampleOnce(scaleFactor2, pair2.first, r, basicStats);
            }

            String value = combineTwoSamples(colStatsDataDoublePair1, colStatsDataDoublePair2);
            String insertSQL = "INSERT INTO "
                    + StatisticConstants.FULL_QUALIFIED_STATS_TBL_NAME
                    + " VALUES "
                    + value;
            stmtExecutor = new StmtExecutor(r.connectContext, insertSQL);
            executeWithExceptionOnFail(stmtExecutor);
        }
    }

    /**
     * 1. Get stats of each partition
     * 2. insert partition in batch
     * 3. calculate column stats based on partition stats
     */
    protected void doFull() throws Exception {
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
        params.put("dbName", db.getFullName());
        params.put("colName", String.valueOf(info.colName));
        params.put("tblName", String.valueOf(tbl.getName()));
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
                partitionAnalysisSQLs.add(stringSubstitutor.replace(COLLECT_PARTITION_STATS_SQL_TEMPLATE));
            }
        } finally {
            tbl.readUnlock();
        }
        execSQLs(partitionAnalysisSQLs, params);
    }

    private ResultRow collectBasicStat(AutoCloseConnectContext context) {
        Map<String, String> params = new HashMap<>();
        params.put("dbName", db.getFullName());
        params.put("colName", info.colName);
        params.put("tblName", tbl.getName());
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        stmtExecutor = new StmtExecutor(context.connectContext, stringSubstitutor.replace(BASIC_STATS_TEMPLATE));
        return stmtExecutor.executeInternalQuery().get(0);
    }

    /**
     * Sample once with the given table sample rate.
     * @param scaleFactor Scale factor rate
     * @param tabletIds selected tablet ids.
     * @param context Statement executor context.
     * @return Pair of result of the sql and the factor of sample rate. (factor = 1 / sample percentage).
     */
    private Pair<ColStatsData, Double> sampleOnce(double scaleFactor, List<Long> tabletIds,
                                                  AutoCloseConnectContext context, ResultRow basicStats) {
        String tabletStr = tabletIds.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", String.valueOf(info.indexId));
        params.put("colId", String.valueOf(info.colName));
        params.put("dataSizeFunction", getDataSizeFunction(col));
        params.put("dbName", db.getFullName());
        params.put("colName", info.colName);
        params.put("tblName", tbl.getName());
        params.put("scaleFactor", String.valueOf(scaleFactor));
        params.put("tablets", tabletStr.isEmpty() ? "" : String.format("TABLET(%s)", tabletStr));
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        stmtExecutor = new StmtExecutor(context.connectContext,
            stringSubstitutor.replace(SAMPLE_COLUMN_SQL_TEMPLATE));
        // Scalar query only return one row
        return Pair.of(new ColStatsData(basicStats, stmtExecutor.executeInternalQuery().get(0)), scaleFactor);
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
                            catalog.getName(), db.getFullName(), info.colName, partitionCollectSQL,
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

    /**
     * Get sample tablets id and actual row count to sample.
     * @param tableSample Input sample rate.
     * @param addExtraTablet True to add one more tablet to the sample tablet list. This is used for two time sample.
     *                       When the given sample rate is too low, the result will contain only one tablet.
     *                       in this case, need to add one more tablet to the sample tablet list to improve accuracy.
     *                       False to ignore this. The second sample time doesn't need to add extra tablet.
     * @return Tablet list and actual row count to sample.
     */
    protected Pair<List<Long>, Long> calcActualSampleTablets(TableSample tableSample, boolean addExtraTablet) {
        // Below code copied from OlapScanNode.java
        long sampleRows; // The total number of sample rows
        long totalRows = 0; // The total number of partition rows hit
        long totalTablet = 0; // The total number of tablets in the hit partition
        OlapTable olapTable = (OlapTable) tbl;
        if (tableSample.isPercent()) {
            sampleRows = (long) Math.max(olapTable.getRowCount() * (tableSample.getSampleValue() / 100.0), 1);
        } else {
            sampleRows = Math.max(tableSample.getSampleValue(), 1);
        }

        // calculate the number of tablets by each partition
        long avgRowsPerPartition = sampleRows / Math.max(olapTable.getPartitions().size(), 1);
        List<Long> sampleTabletIds = new ArrayList<>();
        long actualSampledRowCount = 0;
        // If sampled only one tablet, add this extra tablet to the sample list to improve accuracy.
        long extraTabletId = -1;
        long extraSampledRowCount = 0;
        for (Partition p : olapTable.getPartitions()) {
            List<Long> ids = p.getBaseIndex().getTabletIdsInOrder();

            if (ids.isEmpty()) {
                continue;
            }

            // Skip partitions with row count < row count / 2 expected to be sampled per partition.
            // It can be expected to sample a smaller number of partitions to avoid uneven distribution
            // of sampling results.
            if (p.getBaseIndex().getRowCount() < (avgRowsPerPartition / 2)) {
                continue;
            }
            MaterializedIndex baseIndex = p.getBaseIndex();
            long avgRowsPerTablet = Math.max(baseIndex.getRowCount() / ids.size(), 1);
            long tabletCounts = Math.max(
                    avgRowsPerPartition / avgRowsPerTablet + (avgRowsPerPartition % avgRowsPerTablet != 0 ? 1 : 0), 1);
            tabletCounts = Math.min(tabletCounts, ids.size());
            long seek = tableSample.getSeek() != -1
                    ? tableSample.getSeek() : (long) (new SecureRandom().nextDouble() * ids.size());
            for (int i = 0; i < tabletCounts; i++) {
                int seekTid = (int) ((i + seek) % ids.size());
                long tabletId = ids.get(seekTid);
                sampleTabletIds.add(tabletId);
                actualSampledRowCount += baseIndex.getTablet(tabletId).getRowCount(true);
            }
            if (tabletCounts < ids.size()) {
                extraTabletId = ids.get((int) ((tabletCounts + seek) % ids.size()));
                extraSampledRowCount = baseIndex.getTablet(extraTabletId).getRowCount(true);
            }

            totalRows += p.getBaseIndex().getRowCount();
            totalTablet += ids.size();
        }

        // all hit, direct full
        if (totalRows < sampleRows) {
            // can't fill full sample rows
            sampleTabletIds.clear();
        } else if (sampleTabletIds.size() == totalTablet) {
            // TODO add limit
            sampleTabletIds.clear();
        } else if (!sampleTabletIds.isEmpty()) {
            // TODO add limit
            // might happen if row count in fe metadata hasn't been updated yet
            if (actualSampledRowCount == 0) {
                sampleTabletIds.clear();
            }
        }
        // When only sample one tablet, add an extra tablet to the sample list.
        if (addExtraTablet && sampleTabletIds.size() == 1 && extraTabletId != -1) {
            sampleTabletIds.add(extraTabletId);
            actualSampledRowCount += extraSampledRowCount;
        }
        return Pair.of(sampleTabletIds, actualSampledRowCount);
    }
}

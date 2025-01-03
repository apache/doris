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

import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.text.StringSubstitutor;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Each task analyze one column.
 */
public class OlapAnalysisTask extends BaseAnalysisTask {

    private static final String BASIC_STATS_TEMPLATE = "SELECT "
            + "SUBSTRING(CAST(MIN(`${colName}`) AS STRING), 1, 1024) as min, "
            + "SUBSTRING(CAST(MAX(`${colName}`) AS STRING), 1, 1024) as max "
            + "FROM `${dbName}`.`${tblName}` ${index}";

    @VisibleForTesting
    public OlapAnalysisTask() {
    }

    public OlapAnalysisTask(AnalysisInfo info) {
        super(info);
    }

    public void doExecute() throws Exception {
        if (killed) {
            return;
        }
        List<Pair<String, String>> columnList = info.jobColumns;
        if (columnList == null || columnList.isEmpty()) {
            LOG.warn("Table {}.{}.{}, jobColumns is null or empty.", info.catalogId, info.dbId, info.tblId);
            throw new RuntimeException();
        }
        if (StatisticsUtil.isEmptyTable(tbl, info.analysisMethod)) {
            StatsId statsId = new StatsId(concatColumnStatsId(), info.catalogId, info.dbId,
                    info.tblId, info.indexId, info.colName, null);
            ColStatsData colStatsData = new ColStatsData(statsId);
            Env.getCurrentEnv().getStatisticsCache().syncColStats(colStatsData);
            job.appendBuf(this, Collections.singletonList(colStatsData));
            return;
        }
        if (tableSample != null) {
            doSample();
        } else {
            doFull();
        }
    }

    /**
     * 1. Get col stats in sample ways
     * 2. estimate partition stats
     * 3. insert col stats and partition stats
     */
    protected void doSample() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Will do sample collection for column {}", col.getName());
        }
        Pair<List<Long>, Long> pair = calcActualSampleTablets(tbl.isPartitionColumn(col.getName()));
        LOG.info("Number of tablets selected {}, rows in tablets {}", pair.first.size(), pair.second);
        List<Long> tabletIds = pair.first;
        long totalRowCount = info.indexId == -1
                ? tbl.getRowCount()
                : ((OlapTable) tbl).getRowCountForIndex(info.indexId, false);
        double scaleFactor = (double) totalRowCount / (double) pair.second;
        // might happen if row count in fe metadata hasn't been updated yet
        if (Double.isInfinite(scaleFactor) || Double.isNaN(scaleFactor)) {
            scaleFactor = 1;
            tabletIds = Collections.emptyList();
            pair.second = totalRowCount;
        }
        String tabletStr = tabletIds.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        // Get basic stats, including min and max.
        ResultRow basicStats = collectBasicStat();
        String min = StatisticsUtil.escapeSQL(basicStats != null && basicStats.getValues().size() > 0
                ? basicStats.get(0) : null);
        String max = StatisticsUtil.escapeSQL(basicStats != null && basicStats.getValues().size() > 1
                ? basicStats.get(1) : null);

        boolean limitFlag = false;
        long rowsToSample = pair.second;
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("catalogName", catalog.getName());
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", String.valueOf(info.indexId));
        params.put("colId", StatisticsUtil.escapeSQL(String.valueOf(info.colName)));
        params.put("dataSizeFunction", getDataSizeFunction(col, false));
        params.put("dbName", db.getFullName());
        params.put("colName", StatisticsUtil.escapeColumnName(String.valueOf(info.colName)));
        params.put("tblName", String.valueOf(tbl.getName()));
        params.put("scaleFactor", String.valueOf(scaleFactor));
        params.put("sampleHints", tabletStr.isEmpty() ? "" : String.format("TABLET(%s)", tabletStr));
        params.put("ndvFunction", getNdvFunction(String.valueOf(totalRowCount)));
        params.put("min", StatisticsUtil.quote(min));
        params.put("max", StatisticsUtil.quote(max));
        params.put("rowCount", String.valueOf(totalRowCount));
        params.put("type", col.getType().toString());
        params.put("limit", "");
        params.put("index", getIndex());
        if (needLimit()) {
            // If the tablets to be sampled are too large, use limit to control the rows to read, and re-calculate
            // the scaleFactor.
            rowsToSample = Math.min(getSampleRows(), pair.second);
            // Empty table doesn't need to limit.
            if (rowsToSample > 0) {
                limitFlag = true;
                params.put("limit", "limit " + rowsToSample);
                params.put("scaleFactor", String.valueOf(scaleFactor * (double) pair.second / rowsToSample));
            }
        }
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql;
        if (useLinearAnalyzeTemplate()) {
            // For single unique key, use count as ndv.
            if (isSingleUniqueKey()) {
                params.put("ndvFunction", String.valueOf(totalRowCount));
            } else {
                params.put("ndvFunction", "ROUND(NDV(`${colName}`) * ${scaleFactor})");
            }
            sql = stringSubstitutor.replace(LINEAR_ANALYZE_TEMPLATE);
        } else {
            params.put("dataSizeFunction", getDataSizeFunction(col, true));
            params.put("subStringColName", getStringTypeColName(col));
            sql = stringSubstitutor.replace(DUJ1_ANALYZE_TEMPLATE);
        }
        LOG.info("Sample for column [{}]. Total rows [{}], rows to sample [{}], scale factor [{}], "
                + "limited [{}], distribute column [{}], partition column [{}], key column [{}], "
                + "is single unique key [{}]",
                col.getName(), params.get("rowCount"), rowsToSample, params.get("scaleFactor"),
                limitFlag, tbl.isDistributionColumn(col.getName()),
                tbl.isPartitionColumn(col.getName()), col.isKey(), isSingleUniqueKey());
        runQuery(sql);
    }

    protected ResultRow collectBasicStat() {
        // Agg table value columns has no zone map.
        // For these columns, skip collecting min and max value to avoid scan whole table.
        if (((OlapTable) tbl).getKeysType().equals(KeysType.AGG_KEYS) && !col.isKey()) {
            LOG.info("Aggregation table {} column {} is not a key column, skip collecting min and max.",
                    tbl.getName(), col.getName());
            return null;
        }
        long startTime = System.currentTimeMillis();
        Map<String, String> params = new HashMap<>();
        params.put("dbName", db.getFullName());
        params.put("colName", StatisticsUtil.escapeColumnName(String.valueOf(info.colName)));
        params.put("tblName", String.valueOf(tbl.getName()));
        params.put("index", getIndex());
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(BASIC_STATS_TEMPLATE);
        ResultRow resultRow;
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(false)) {
            stmtExecutor = new StmtExecutor(r.connectContext, sql);
            resultRow = stmtExecutor.executeInternalQuery().get(0);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cost time in millisec: " + (System.currentTimeMillis() - startTime) + " Min max SQL: "
                        + sql + " QueryId: " + DebugUtil.printId(stmtExecutor.getContext().queryId()));
            }
            // Release the reference to stmtExecutor, reduce memory usage.
            stmtExecutor = null;
        }
        return resultRow;
    }

    /**
     * 1. Get stats of each partition
     * 2. insert partition in batch
     * 3. calculate column stats based on partition stats
     */
    protected void doFull() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Will do full collection for column {}", col.getName());
        }
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", String.valueOf(info.indexId));
        params.put("colId", StatisticsUtil.escapeSQL(String.valueOf(info.colName)));
        params.put("dataSizeFunction", getDataSizeFunction(col, false));
        params.put("catalogName", catalog.getName());
        params.put("dbName", db.getFullName());
        params.put("colName", StatisticsUtil.escapeColumnName(String.valueOf(info.colName)));
        params.put("tblName", String.valueOf(tbl.getName()));
        params.put("index", getIndex());
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String collectColStats = stringSubstitutor.replace(COLLECT_COL_STATISTICS);
        runQuery(collectColStats);
    }

    protected String getIndex() {
        if (info.indexId == -1) {
            return "";
        } else {
            OlapTable olapTable = (OlapTable) this.tbl;
            return "index `" + olapTable.getIndexNameById(info.indexId) + "`";
        }
    }

    // Get sample tablets id and sample row count
    protected Pair<List<Long>, Long> calcActualSampleTablets(boolean forPartitionColumn) {
        // Below code copied from OlapScanNode.java
        long sampleRows; // The total number of sample rows
        long totalRows = 0; // The total number of partition rows hit
        long totalTablet = 0; // The total number of tablets in the hit partition
        OlapTable olapTable = (OlapTable) tbl;
        sampleRows = getSampleRows();

        // calculate the number of tablets by each partition
        long avgRowsPerPartition = sampleRows / Math.max(olapTable.getPartitions().size(), 1);
        List<Long> sampleTabletIds = new ArrayList<>();
        long actualSampledRowCount = 0;
        boolean enough = false;
        List<Partition> sortedPartitions = olapTable.getPartitions().stream().sorted(
                Comparator.comparing(Partition::getName)).collect(Collectors.toList());
        for (Partition p : sortedPartitions) {
            MaterializedIndex materializedIndex = info.indexId == -1 ? p.getBaseIndex() : p.getIndex(info.indexId);
            if (materializedIndex == null) {
                continue;
            }
            List<Long> ids = materializedIndex.getTabletIdsInOrder();
            if (ids.isEmpty()) {
                continue;
            }

            // Skip partitions with row count < row count / 2 expected to be sampled per partition.
            // It can be expected to sample a smaller number of partitions to avoid uneven distribution
            // of sampling results.
            if (materializedIndex.getRowCount() < (avgRowsPerPartition / 2) && !forPartitionColumn) {
                continue;
            }
            long avgRowsPerTablet = Math.max(materializedIndex.getRowCount() / ids.size(), 1);
            long tabletCounts = Math.max(
                    avgRowsPerPartition / avgRowsPerTablet + (avgRowsPerPartition % avgRowsPerTablet != 0 ? 1 : 0), 1);
            tabletCounts = Math.min(tabletCounts, ids.size());
            long seek = tableSample.getSeek() != -1
                    ? tableSample.getSeek() : (long) (new SecureRandom().nextDouble() * ids.size());
            for (int i = 0; i < tabletCounts; i++) {
                int seekTid = (int) ((i + seek) % ids.size());
                long tabletId = ids.get(seekTid);
                sampleTabletIds.add(tabletId);
                actualSampledRowCount += materializedIndex.getTablet(tabletId)
                        .getMinReplicaRowCount(p.getVisibleVersion());
                if (actualSampledRowCount >= sampleRows && !forPartitionColumn) {
                    enough = true;
                    break;
                }
            }
            totalRows += materializedIndex.getRowCount();
            totalTablet += ids.size();
            if (enough) {
                break;
            }
        }

        // all hit, direct full
        if (totalRows < sampleRows) {
            // can't fill full sample rows
            sampleTabletIds.clear();
            actualSampledRowCount = 0;
        } else if (sampleTabletIds.size() == totalTablet && !enough) {
            sampleTabletIds.clear();
            actualSampledRowCount = 0;
        }
        return Pair.of(sampleTabletIds, actualSampledRowCount);
    }

    /**
     * For ordinary column (neither key column nor partition column), need to limit sample size to user specified value.
     * @return Return true when need to limit.
     */
    protected boolean needLimit() {
        // Key column is sorted, use limit will cause the ndv not accurate enough, so skip key columns.
        if (col.isKey()) {
            return false;
        }
        // Partition column need to scan tablets from all partitions.
        if (tbl.isPartitionColumn(col.getName())) {
            return false;
        }
        return true;
    }

    /**
     * Calculate rows to sample based on user given sample value.
     * @return Rows to sample.
     */
    protected long getSampleRows() {
        long sampleRows;
        if (tableSample.isPercent()) {
            sampleRows = (long) Math.max(tbl.getRowCount() * (tableSample.getSampleValue() / 100.0), 1);
        } else {
            sampleRows = Math.max(tableSample.getSampleValue(), 1);
        }
        return sampleRows;
    }

    /**
     * Check if the task should use linear analyze template.
     * @return True for single unique key column and single distribution column.
     */
    protected boolean useLinearAnalyzeTemplate() {
        if (isSingleUniqueKey()) {
            return true;
        }
        String columnName = col.getName();
        if (columnName.startsWith(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX)) {
            columnName = columnName.substring(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX.length());
        }
        Set<String> distributionColumns = tbl.getDistributionColumnNames();
        return distributionColumns.size() == 1 && distributionColumns.contains(columnName.toLowerCase());
    }

    /**
     * Check if the olap table has a single unique key.
     * @return True if the table has a single unique/agg key. False otherwise.
     */
    protected boolean isSingleUniqueKey() {
        OlapTable olapTable = (OlapTable) this.tbl;
        List<Column> schema;
        KeysType keysType;
        if (info.indexId == -1) {
            schema = olapTable.getBaseSchema();
            keysType = olapTable.getKeysType();
        } else {
            MaterializedIndexMeta materializedIndexMeta = olapTable.getIndexIdToMeta().get(info.indexId);
            schema = materializedIndexMeta.getSchema();
            keysType = materializedIndexMeta.getKeysType();
        }

        int keysNum = 0;
        for (Column column : schema) {
            if (column.isKey()) {
                keysNum += 1;
            }
        }

        return col.isKey()
            && keysNum == 1
            && (keysType.equals(KeysType.UNIQUE_KEYS) || keysType.equals(KeysType.AGG_KEYS));
    }

    protected String concatColumnStatsId() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(info.tblId);
        stringBuilder.append("-");
        stringBuilder.append(info.indexId);
        stringBuilder.append("-");
        stringBuilder.append(info.colName);
        return stringBuilder.toString();
    }
}

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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
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

    private boolean keyColumnSampleTooManyRows = false;
    private boolean partitionColumnSampleTooManyRows = false;
    private boolean scanFullTable = false;
    private static final long MAXIMUM_SAMPLE_ROWS = 1_000_000_000;
    private static final int PARTITION_COUNT_TO_SAMPLE = 5;

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
        // For empty table, write empty result directly, no need to run SQL to collect stats.
        if (info.rowCount == 0 && tableSample != null) {
            StatsId statsId = new StatsId(concatColumnStatsId(), info.catalogId, info.dbId,
                    info.tblId, info.indexId, info.colName, null);
            job.appendBuf(this, Collections.singletonList(new ColStatsData(statsId)));
            return;
        }
        if (tableSample != null) {
            doSample();
        } else {
            doFull();
        }
        LOG.info("AnalysisTask Done {}", this.toString());
    }

    /**
     * 1. Get col stats in sample ways
     * 2. estimate partition stats
     * 3. insert col stats and partition stats
     */
    @Override
    protected void doSample() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Will do sample collection for column {}", col.getName());
        }
        // Get basic stats, including min and max.
        ResultRow minMax = collectMinMax();
        String min = StatisticsUtil.escapeSQL(minMax != null && minMax.getValues().size() > 0
                ? minMax.get(0) : null);
        String max = StatisticsUtil.escapeSQL(minMax != null && minMax.getValues().size() > 1
                ? minMax.get(1) : null);

        Map<String, String> params = buildSqlParams();
        params.put("min", StatisticsUtil.quote(min));
        params.put("max", StatisticsUtil.quote(max));
        long tableRowCount = info.indexId == -1
                ? tbl.getRowCount()
                : ((OlapTable) tbl).getRowCountForIndex(info.indexId, false);
        getSampleParams(params, tableRowCount);
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql;
        if (useLinearAnalyzeTemplate()) {
            sql = stringSubstitutor.replace(LINEAR_ANALYZE_TEMPLATE);
        } else {
            sql = stringSubstitutor.replace(DUJ1_ANALYZE_TEMPLATE);
        }
        LOG.info("Analyze param: scanFullTable {}, partitionColumnTooMany {}, keyColumnTooMany {}",
                scanFullTable, partitionColumnSampleTooManyRows, keyColumnSampleTooManyRows);
        LOG.debug(sql);
        runQuery(sql);
    }

    protected ResultRow collectMinMax() {
        // Agg table value columns has no zone map.
        // For these columns, skip collecting min and max value to avoid scan whole table.
        if (((OlapTable) tbl).getKeysType().equals(KeysType.AGG_KEYS) && !col.isKey()) {
            LOG.info("Aggregation table {} column {} is not a key column, skip collecting min and max.",
                    tbl.getName(), col.getName());
            return null;
        }
        long startTime = System.currentTimeMillis();
        Map<String, String> params = buildSqlParams();
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
        } catch (Exception e) {
            LOG.info("Failed to collect basic stat {}. Reason {}", sql, e.getMessage());
            throw e;
        }
        return resultRow;
    }

    /**
     * Select the tablets to read.
     * @return Pair of tablet id list and how many rows are going to read.
     */
    protected Pair<List<Long>, Long> getSampleTablets() {
        long targetSampleRows = getSampleRows();
        OlapTable olapTable = (OlapTable) tbl;
        boolean forPartitionColumn = tbl.isPartitionColumn(col.getName());
        long avgTargetRowsPerPartition = targetSampleRows / Math.max(olapTable.getPartitions().size(), 1);
        List<Long> sampleTabletIds = new ArrayList<>();
        long selectedRows = 0;
        boolean enough = false;
        // Sort the partitions to get stable result.
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
            long avgRowsPerTablet = Math.max(materializedIndex.getRowCount() / ids.size(), 1);
            long tabletCounts = Math.max(avgTargetRowsPerPartition / avgRowsPerTablet
                    + (avgTargetRowsPerPartition % avgRowsPerTablet != 0 ? 1 : 0), 1);
            tabletCounts = Math.min(tabletCounts, ids.size());
            long seek = tableSample.getSeek() != -1 ? tableSample.getSeek()
                    : (long) (new SecureRandom().nextDouble() * ids.size());
            for (int i = 0; i < tabletCounts; i++) {
                int seekTid = (int) ((i + seek) % ids.size());
                long tabletId = ids.get(seekTid);
                sampleTabletIds.add(tabletId);
                long tabletRows = materializedIndex.getTablet(tabletId).getMinReplicaRowCount(p.getVisibleVersion());
                if (tabletRows > 0) {
                    selectedRows += tabletRows;
                    // For regular column, will stop adding more tablets when selected tablets'
                    // row count is more than the target sample rows.
                    // But for partition columns, will not stop adding. For ndv sample accuracy,
                    // better to choose at least one tablet in each partition.
                    if (selectedRows >= targetSampleRows && !forPartitionColumn) {
                        enough = true;
                        break;
                    }
                }
            }
            if (enough) {
                break;
            }
        }
        if (selectedRows < targetSampleRows) {
            scanFullTable = true;
        } else if (forPartitionColumn && selectedRows > MAXIMUM_SAMPLE_ROWS) {
            // If the selected tablets for partition column contain too many rows, change to linear sample.
            partitionColumnSampleTooManyRows = true;
            sampleTabletIds.clear();
            Collections.shuffle(sortedPartitions);
            selectedRows = pickSamplePartition(sortedPartitions, sampleTabletIds);
        } else if (col.isKey() && selectedRows > MAXIMUM_SAMPLE_ROWS) {
            // For key column, if a single tablet contains too many rows, need to use limit to control rows to read.
            // In most cases, a single tablet shouldn't contain more than MAXIMUM_SAMPLE_ROWS, in this case, we
            // don't use limit for key column for ndv accuracy reason.
            keyColumnSampleTooManyRows = true;
        }
        return Pair.of(sampleTabletIds, selectedRows);
    }

    /**
     * Get the sql params for this sample task.
     * @param params Sql params to use in analyze task.
     * @param tableRowCount BE reported table/index row count.
     */
    protected void getSampleParams(Map<String, String> params, long tableRowCount) {
        long targetSampleRows = getSampleRows();
        params.put("rowCount", String.valueOf(tableRowCount));
        params.put("type", col.getType().toString());
        params.put("limit", "");

        // If table row count is less than the target sample row count, simple scan the full table.
        if (tableRowCount <= targetSampleRows) {
            params.put("scaleFactor", "1");
            params.put("sampleHints", "");
            params.put("ndvFunction", "ROUND(NDV(`${colName}`) * ${scaleFactor})");
            scanFullTable = true;
            return;
        }
        Pair<List<Long>, Long> sampleTabletsInfo = getSampleTablets();
        String tabletStr = sampleTabletsInfo.first.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        String sampleHints = scanFullTable ? "" : String.format("TABLET(%s)", tabletStr);
        params.put("sampleHints", sampleHints);
        long selectedRows = sampleTabletsInfo.second;
        long finalScanRows = selectedRows;
        double scaleFactor = scanFullTable ? 1 : (double) tableRowCount / finalScanRows;
        params.put("scaleFactor", String.valueOf(scaleFactor));

        // If the tablets to be sampled are too large, use limit to control the rows to read, and re-calculate
        // the scaleFactor.
        if (needLimit()) {
            finalScanRows = Math.min(targetSampleRows, selectedRows);
            if (col.isKey() && keyColumnSampleTooManyRows) {
                finalScanRows = MAXIMUM_SAMPLE_ROWS;
            }
            // Empty table doesn't need to limit.
            if (finalScanRows > 0) {
                scaleFactor = (double) tableRowCount / finalScanRows;
                params.put("limit", "limit " + finalScanRows);
                params.put("scaleFactor", String.valueOf(scaleFactor));
            }
        }
        // Set algorithm related params.
        if (useLinearAnalyzeTemplate()) {
            // For single unique key, use count as ndv.
            if (isSingleUniqueKey()) {
                params.put("ndvFunction", String.valueOf(tableRowCount));
            } else {
                params.put("ndvFunction", "ROUND(NDV(`${colName}`) * ${scaleFactor})");
            }
        } else {
            params.put("ndvFunction", getNdvFunction(String.valueOf(tableRowCount)));
            params.put("dataSizeFunction", getDataSizeFunction(col, true));
            params.put("subStringColName", getStringTypeColName(col));
        }
    }

    protected void doFull() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Will do full collection for column {}", col.getName());
        }
        if (StatisticsUtil.enablePartitionAnalyze() && tbl.isPartitionedTable()) {
            doPartitionTable();
        } else {
            StringSubstitutor stringSubstitutor = new StringSubstitutor(buildSqlParams());
            runQuery(stringSubstitutor.replace(FULL_ANALYZE_TEMPLATE));
        }
    }

    @Override
    protected void deleteNotExistPartitionStats(AnalysisInfo jobInfo) throws DdlException {
        TableStatsMeta tableStats = Env.getServingEnv().getAnalysisManager().findTableStatsStatus(tbl.getId());
        if (tableStats == null) {
            return;
        }
        OlapTable table = (OlapTable) tbl;
        String indexName = info.indexId == -1 ? table.getName() : table.getIndexNameById(info.indexId);
        ColStatsMeta columnStats = tableStats.findColumnStatsMeta(indexName, info.colName);
        if (columnStats == null || columnStats.partitionUpdateRows == null
                || columnStats.partitionUpdateRows.isEmpty()) {
            return;
        }
        // When a partition was dropped, partitionChanged will be set to true.
        // So we don't need to check dropped partition if partitionChanged is false.
        if (!tableStats.partitionChanged.get()
                && columnStats.partitionUpdateRows.size() == table.getPartitions().size()) {
            return;
        }
        Set<Long> expiredPartition = Sets.newHashSet();
        String columnCondition = "AND col_id = " + StatisticsUtil.quote(col.getName());
        for (long partId : columnStats.partitionUpdateRows.keySet()) {
            Partition partition = table.getPartition(partId);
            if (partition == null) {
                columnStats.partitionUpdateRows.remove(partId);
                tableStats.partitionUpdateRows.remove(partId);
                jobInfo.partitionUpdateRows.remove(partId);
                expiredPartition.add(partId);
                if (expiredPartition.size() == Config.max_allowed_in_element_num_of_delete) {
                    String partitionCondition = " AND part_id in (" + Joiner.on(", ").join(expiredPartition) + ")";
                    StatisticsRepository.dropPartitionsColumnStatistics(info.catalogId, info.dbId, info.tblId,
                            columnCondition, partitionCondition);
                    expiredPartition.clear();
                }
            }
        }
        if (expiredPartition.size() > 0) {
            String partitionCondition = " AND part_id in (" + Joiner.on(", ").join(expiredPartition) + ")";
            StatisticsRepository.dropPartitionsColumnStatistics(info.catalogId, info.dbId, info.tblId,
                    columnCondition, partitionCondition);
        }
    }

    @Override
    protected String getPartitionInfo(String partitionName) {
        return "partition " + partitionName;
    }

    @Override
    protected Map<String, String> buildSqlParams() {
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.TABLE_STATISTIC_TBL_NAME);
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
        return params;
    }

    protected String getIndex() {
        if (info.indexId == -1) {
            return "";
        } else {
            OlapTable olapTable = (OlapTable) this.tbl;
            return "index `" + olapTable.getIndexNameById(info.indexId) + "`";
        }
    }

    protected long pickSamplePartition(List<Partition> partitions, List<Long> pickedTabletIds) {
        long averageRowsPerPartition = tbl.getRowCount() / partitions.size();
        long indexId = info.indexId == -1 ? ((OlapTable) tbl).getBaseIndexId() : info.indexId;
        long pickedRows = 0;
        int pickedPartitionCount = 0;
        for (Partition p : partitions) {
            long partitionRowCount = p.getRowCount();
            if (partitionRowCount >= averageRowsPerPartition) {
                pickedRows += partitionRowCount;
                pickedPartitionCount++;
                MaterializedIndex materializedIndex = p.getIndex(indexId);
                pickedTabletIds.addAll(materializedIndex.getTabletIdsInOrder());
            }
            if (pickedRows >= MAXIMUM_SAMPLE_ROWS || pickedPartitionCount > PARTITION_COUNT_TO_SAMPLE) {
                break;
            }
        }
        return pickedRows;
    }

    @VisibleForTesting
    protected void setTable(OlapTable table) {
        tbl = table;
    }

    /**
     * For ordinary column (neither key column nor partition column), need to limit sample size to user specified value.
     * @return Return true when need to limit.
     */
    protected boolean needLimit() {
        if (scanFullTable) {
            return false;
        }
        // Key column is sorted, use limit will cause the ndv not accurate enough, so skip key columns.
        if (col.isKey() && !keyColumnSampleTooManyRows) {
            return false;
        }
        // Partition column need to scan tablets from all partitions.
        return !tbl.isPartitionColumn(col.getName());
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
        if (partitionColumnSampleTooManyRows || scanFullTable) {
            return true;
        }
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
        return info.tblId + "-" + info.indexId + "-" + info.colName;
    }

    @VisibleForTesting
    public void setKeyColumnSampleTooManyRows(boolean value) {
        keyColumnSampleTooManyRows = value;
    }

    @VisibleForTesting
    public void setPartitionColumnSampleTooManyRows(boolean value) {
        partitionColumnSampleTooManyRows = value;
    }

    @VisibleForTesting
    public void setScanFullTable(boolean value) {
        scanFullTable = value;
    }

    @VisibleForTesting
    public boolean scanFullTable() {
        return scanFullTable;
    }
}

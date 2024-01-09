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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.external.hive.util.HiveUtil;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Sets;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class HMSAnalysisTask extends BaseAnalysisTask {
    private static final Logger LOG = LogManager.getLogger(HMSAnalysisTask.class);

    private static final String ANALYZE_TABLE_COUNT_TEMPLATE = "SELECT ROUND(COUNT(1) * ${scaleFactor}) as rowCount "
            + "FROM `${catalogName}`.`${dbName}`.`${tblName}` ${sampleHints}";
    private boolean isTableLevelTask;
    private boolean isPartitionOnly;
    private HMSExternalTable table;

    public HMSAnalysisTask() {
    }

    public HMSAnalysisTask(AnalysisInfo info) {
        super(info);
        isTableLevelTask = info.externalTableLevelTask;
        isPartitionOnly = info.partitionOnly;
        table = (HMSExternalTable) tbl;
    }

    public void doExecute() throws Exception {
        if (isTableLevelTask) {
            getTableStats();
        } else {
            getTableColumnStats();
        }
    }

    // For test
    protected void setTable(HMSExternalTable table) {
        this.table = table;
    }

    /**
     * Get table row count
     */
    private void getTableStats() throws Exception {
        Map<String, String> params = buildStatsParams(null);
        List<ResultRow> columnResult =
                StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
                        .replace(ANALYZE_TABLE_COUNT_TEMPLATE));
        String rowCount = columnResult.get(0).get(0);
        Env.getCurrentEnv().getAnalysisManager()
                .updateTableStatsStatus(
                        new TableStatsMeta(Long.parseLong(rowCount), info, tbl));
        job.rowCountDone(this);
    }

    /**
     * Get column statistics and insert the result to __internal_schema.column_statistics
     */
    protected void getTableColumnStats() throws Exception {
        if (!info.usingSqlForPartitionColumn && isPartitionColumn()) {
            try {
                getPartitionColumnStats();
            } catch (Exception e) {
                LOG.warn("Failed to collect stats for partition col {} using metadata, "
                        + "fallback to normal collection", col.getName(), e);
                getOrdinaryColumnStats();
            }
        } else {
            getOrdinaryColumnStats();
        }
    }

    private boolean isPartitionColumn() {
        return table.getPartitionColumns().stream().anyMatch(c -> c.getName().equals(col.getName()));
    }

    // Get ordinary column stats. Ordinary column means not partition column.
    private void getOrdinaryColumnStats() throws Exception {
        StringBuilder sb = new StringBuilder();
        Map<String, String> params = buildStatsParams("NULL");
        params.put("min", getMinFunction());
        params.put("max", getMaxFunction());
        params.put("dataSizeFunction", getDataSizeFunction(col, false));
        Pair<Double, Long> sampleInfo = getSampleInfo();
        params.put("scaleFactor", String.valueOf(sampleInfo.first));
        StringSubstitutor stringSubstitutor;
        if (tableSample == null) {
            // Do full analyze
            LOG.debug("Will do full collection for column {}", col.getName());
            sb.append(COLLECT_COL_STATISTICS);
        } else {
            // Do sample analyze
            LOG.debug("Will do sample collection for column {}", col.getName());
            boolean limitFlag = false;
            boolean bucketFlag = false;
            // If sample size is too large, use limit to control the sample size.
            if (needLimit(sampleInfo.second, sampleInfo.first)) {
                limitFlag = true;
                long columnSize = 0;
                for (Column column : table.getFullSchema()) {
                    columnSize += column.getDataType().getSlotSize();
                }
                double targetRows = (double) sampleInfo.second / columnSize;
                // Estimate the new scaleFactor based on the schema.
                if (targetRows > StatisticsUtil.getHugeTableSampleRows()) {
                    params.put("limit", "limit " + StatisticsUtil.getHugeTableSampleRows());
                    params.put("scaleFactor",
                            String.valueOf(sampleInfo.first * targetRows / StatisticsUtil.getHugeTableSampleRows()));
                }
            }
            // Single distribution column is not fit for DUJ1 estimator, use linear estimator.
            Set<String> distributionColumns = tbl.getDistributionColumnNames();
            if (distributionColumns.size() == 1 && distributionColumns.contains(col.getName().toLowerCase())) {
                bucketFlag = true;
                sb.append(LINEAR_ANALYZE_TEMPLATE);
                params.put("ndvFunction", "ROUND(NDV(`${colName}`) * ${scaleFactor})");
                params.put("rowCount", "ROUND(count(1) * ${scaleFactor})");
            } else {
                sb.append(DUJ1_ANALYZE_TEMPLATE);
                params.put("dataSizeFunction", getDataSizeFunction(col, true));
                params.put("ndvFunction", getNdvFunction("ROUND(SUM(t1.count) * ${scaleFactor})"));
                params.put("rowCount", "ROUND(SUM(t1.count) * ${scaleFactor})");
            }
            LOG.info("Sample for column [{}]. Scale factor [{}], "
                    + "limited [{}], is distribute column [{}]",
                    col.getName(), params.get("scaleFactor"), limitFlag, bucketFlag);
        }
        stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(sb.toString());
        runQuery(sql);
    }

    // Collect the partition column stats through HMS metadata.
    // Get all the partition values and calculate the stats based on the values.
    private void getPartitionColumnStats() throws Exception {
        Set<String> partitionNames = table.getPartitionNames();
        Set<String> ndvPartValues = Sets.newHashSet();
        long numNulls = 0;
        long dataSize = 0;
        String min = null;
        String max = null;
        for (String names : partitionNames) {
            // names is like "date=20230101" for one level partition
            // and like "date=20230101/hour=12" for two level partition
            String[] parts = names.split("/");
            for (String part : parts) {
                if (part.startsWith(col.getName())) {
                    String value = HiveUtil.getHivePartitionValue(part);
                    // HIVE_DEFAULT_PARTITION hive partition value when the partition name is not specified.
                    if (value == null || value.isEmpty() || value.equals(HiveMetaStoreCache.HIVE_DEFAULT_PARTITION)) {
                        numNulls += 1;
                        continue;
                    }
                    ndvPartValues.add(value);
                    dataSize += col.getType().isStringType() ? value.length() : col.getType().getSlotSize();
                    min = updateMinValue(min, value);
                    max = updateMaxValue(max, value);
                }
            }
        }
        // Estimate the row count. This value is inaccurate if the table stats is empty.
        TableStatsMeta tableStatsStatus = Env.getCurrentEnv().getAnalysisManager().findTableStatsStatus(table.getId());
        long count = tableStatsStatus == null ? table.estimatedRowCount() : tableStatsStatus.rowCount;
        dataSize = dataSize * count / partitionNames.size();
        numNulls = numNulls * count / partitionNames.size();
        int ndv = ndvPartValues.size();

        Map<String, String> params = buildStatsParams("NULL");
        params.put("row_count", String.valueOf(count));
        params.put("ndv", String.valueOf(ndv));
        params.put("null_count", String.valueOf(numNulls));
        params.put("min", StatisticsUtil.quote(min));
        params.put("max", StatisticsUtil.quote(max));
        params.put("data_size", String.valueOf(dataSize));
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(ANALYZE_PARTITION_COLUMN_TEMPLATE);
        runQuery(sql);
    }

    private String updateMinValue(String currentMin, String value) {
        if (currentMin == null) {
            return value;
        }
        if (col.getType().isFixedPointType()) {
            if (Long.parseLong(value) < Long.parseLong(currentMin)) {
                return value;
            } else {
                return currentMin;
            }
        }
        if (col.getType().isFloatingPointType() || col.getType().isDecimalV2() || col.getType().isDecimalV3()) {
            if (Double.parseDouble(value) < Double.parseDouble(currentMin)) {
                return value;
            } else {
                return currentMin;
            }
        }
        return value.compareTo(currentMin) < 0 ? value : currentMin;
    }

    private String updateMaxValue(String currentMax, String value) {
        if (currentMax == null) {
            return value;
        }
        if (col.getType().isFixedPointType()) {
            if (Long.parseLong(value) > Long.parseLong(currentMax)) {
                return value;
            } else {
                return currentMax;
            }
        }
        if (col.getType().isFloatingPointType() || col.getType().isDecimalV2() || col.getType().isDecimalV3()) {
            if (Double.parseDouble(value) > Double.parseDouble(currentMax)) {
                return value;
            } else {
                return currentMax;
            }
        }
        return value.compareTo(currentMax) > 0 ? value : currentMax;
    }

    private Map<String, String> buildStatsParams(String partId) {
        Map<String, String> commonParams = new HashMap<>();
        String id = StatisticsUtil.constructId(tbl.getId(), -1);
        if (partId == null) {
            commonParams.put("partId", "NULL");
        } else {
            id = StatisticsUtil.constructId(id, partId);
            commonParams.put("partId", "\'" + partId + "\'");
        }
        commonParams.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        commonParams.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        commonParams.put("id", id);
        commonParams.put("catalogId", String.valueOf(catalog.getId()));
        commonParams.put("dbId", String.valueOf(db.getId()));
        commonParams.put("tblId", String.valueOf(tbl.getId()));
        commonParams.put("indexId", "-1");
        commonParams.put("idxId", "-1");
        commonParams.put("colName", info.colName);
        commonParams.put("colId", info.colName);
        commonParams.put("catalogName", catalog.getName());
        commonParams.put("dbName", db.getFullName());
        commonParams.put("tblName", tbl.getName());
        commonParams.put("sampleHints", getSampleHint());
        commonParams.put("limit", "");
        commonParams.put("scaleFactor", "1");
        if (col != null) {
            commonParams.put("type", col.getType().toString());
        }
        commonParams.put("lastAnalyzeTimeInMs", String.valueOf(System.currentTimeMillis()));
        return commonParams;
    }

    protected String getSampleHint() {
        if (tableSample == null) {
            return "";
        }
        if (tableSample.isPercent()) {
            return String.format("TABLESAMPLE(%d PERCENT)", tableSample.getSampleValue());
        } else {
            return String.format("TABLESAMPLE(%d ROWS)", tableSample.getSampleValue());
        }
    }

    /**
     * Get the pair of sample scale factor and the file size going to sample.
     * While analyzing, the result of count, null count and data size need to
     * multiply this scale factor to get more accurate result.
     * @return Pair of sample scale factor and the file size going to sample.
     */
    protected Pair<Double, Long> getSampleInfo() {
        if (tableSample == null) {
            return Pair.of(1.0, 0L);
        }
        long target;
        // Get list of all files' size in this HMS table.
        List<Long> chunkSizes = table.getChunkSizes();
        Collections.shuffle(chunkSizes, new Random(tableSample.getSeek()));
        long total = 0;
        // Calculate the total size of this HMS table.
        for (long size : chunkSizes) {
            total += size;
        }
        if (total == 0) {
            return Pair.of(1.0, 0L);
        }
        // Calculate the sample target size for percent and rows sample.
        if (tableSample.isPercent()) {
            target = total * tableSample.getSampleValue() / 100;
        } else {
            int columnSize = 0;
            for (Column column : table.getFullSchema()) {
                columnSize += column.getDataType().getSlotSize();
            }
            target = columnSize * tableSample.getSampleValue();
        }
        // Calculate the actual sample size (cumulate).
        long cumulate = 0;
        for (long size : chunkSizes) {
            cumulate += size;
            if (cumulate >= target) {
                break;
            }
        }
        return Pair.of(Math.max(((double) total) / cumulate, 1), cumulate);
    }

    @Override
    protected void afterExecution() {
        // Table level task doesn't need to sync any value to sync stats, it stores the value in metadata.
        // Partition only task doesn't need to refresh cached.
        if (isTableLevelTask || isPartitionOnly) {
            return;
        }
        Env.getCurrentEnv().getStatisticsCache().syncLoadColStats(tbl.getId(), -1, col.getName());
    }

    /**
     * If the size to sample is larger than LIMIT_SIZE (1GB)
     * and is much larger (1.2*) than the size user want to sample,
     * use limit to control the total sample size.
     * @param sizeToRead The file size to sample.
     * @param factor sizeToRead * factor = Table total size.
     * @return True if need to limit.
     */
    protected boolean needLimit(long sizeToRead, double factor) {
        long total = (long) (sizeToRead * factor);
        long target;
        if (tableSample.isPercent()) {
            target = total * tableSample.getSampleValue() / 100;
        } else {
            int columnSize = 0;
            for (Column column : table.getFullSchema()) {
                columnSize += column.getDataType().getSlotSize();
            }
            target = columnSize * tableSample.getSampleValue();
        }
        if (sizeToRead > LIMIT_SIZE && sizeToRead > target * LIMIT_FACTOR) {
            return true;
        }
        return false;
    }
}

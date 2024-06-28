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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HiveUtil;
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

public class HMSAnalysisTask extends ExternalAnalysisTask {
    private static final Logger LOG = LogManager.getLogger(HMSAnalysisTask.class);
    private HMSExternalTable hmsExternalTable;

    // for test
    public HMSAnalysisTask() {
    }

    public HMSAnalysisTask(AnalysisInfo info) {
        super(info);
        hmsExternalTable = (HMSExternalTable) tbl;
    }

    private boolean isPartitionColumn() {
        return hmsExternalTable.getPartitionColumns().stream().anyMatch(c -> c.getName().equals(col.getName()));
    }

    // For test
    protected void setTable(HMSExternalTable table) {
        setTable((ExternalTable) table);
        this.hmsExternalTable = table;
    }

    @Override
    public void doExecute() throws Exception {
        if (killed) {
            return;
        }
        if (info.usingSqlForExternalTable) {
            // If user specify with sql in analyze statement, using sql to collect stats.
            super.doExecute();
        } else {
            // By default, using HMS stats and partition info to collect stats.
            try {
                if (StatisticsUtil.enablePartitionAnalyze() && tbl.isPartitionedTable()) {
                    throw new RuntimeException("HMS doesn't support fetch partition level stats.");
                }
                if (isPartitionColumn()) {
                    getPartitionColumnStats();
                } else {
                    getHmsColumnStats();
                }
            } catch (Exception e) {
                LOG.info("Failed to collect stats for {}col {} using metadata, "
                        + "fallback to normal collection. Because {}",
                        isPartitionColumn() ? "partition " : "", col.getName(), e.getMessage());
                /* retry using sql way! */
                super.doExecute();
            }
        }
    }

    // Collect the partition column stats through HMS metadata.
    // Get all the partition values and calculate the stats based on the values.
    private void getPartitionColumnStats() {
        Set<String> partitionNames = hmsExternalTable.getPartitionNames();
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
        // getRowCount may return 0 if cache is empty, in this case, call fetchRowCount.
        long count = hmsExternalTable.getRowCount();
        if (count == 0) {
            count = hmsExternalTable.fetchRowCount();
        }
        dataSize = dataSize * count / partitionNames.size();
        numNulls = numNulls * count / partitionNames.size();
        int ndv = ndvPartValues.size();

        Map<String, String> params = buildSqlParams();
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

    // Collect the spark analyzed column stats through HMS metadata.
    private void getHmsColumnStats() throws Exception {
        // getRowCount may return 0 if cache is empty, in this case, call fetchRowCount.
        long count = hmsExternalTable.getRowCount();
        if (count == 0) {
            count = hmsExternalTable.fetchRowCount();
        }

        Map<String, String> params = buildSqlParams();
        Map<StatsType, String> statsParams = new HashMap<>();
        statsParams.put(StatsType.NDV, "ndv");
        statsParams.put(StatsType.NUM_NULLS, "null_count");
        statsParams.put(StatsType.MIN_VALUE, "min");
        statsParams.put(StatsType.MAX_VALUE, "max");
        statsParams.put(StatsType.AVG_SIZE, "avg_len");

        if (!hmsExternalTable.fillColumnStatistics(info.colName, statsParams, params)) {
            throw new AnalysisException("some column stats not available");
        }

        long dataSize = Long.parseLong(params.get("avg_len")) * count;
        params.put("row_count", String.valueOf(count));
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

    @Override
    protected void doSample() {
        StringBuilder sb = new StringBuilder();
        Map<String, String> params = buildSqlParams();
        params.put("min", getMinFunction());
        params.put("max", getMaxFunction());
        params.put("dataSizeFunction", getDataSizeFunction(col, false));
        Pair<Double, Long> sampleInfo = getSampleInfo();
        params.put("scaleFactor", String.valueOf(sampleInfo.first));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Will do sample collection for column {}", col.getName());
        }
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
            if (col.getType().isStringType()) {
                sb.append(DUJ1_ANALYZE_STRING_TEMPLATE);
            } else {
                sb.append(DUJ1_ANALYZE_TEMPLATE);
            }
            params.put("dataSizeFunction", getDataSizeFunction(col, true));
            params.put("ndvFunction", getNdvFunction("ROUND(SUM(t1.count) * ${scaleFactor})"));
            params.put("rowCount", "ROUND(SUM(t1.count) * ${scaleFactor})");
        }
        LOG.info("Sample for column [{}]. Scale factor [{}], "
                + "limited [{}], is distribute column [{}]",
                col.getName(), params.get("scaleFactor"), limitFlag, bucketFlag);
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(sb.toString());
        runQuery(sql);
    }

    @Override
    protected void doFull() throws Exception {
        if (StatisticsUtil.enablePartitionAnalyze() && tbl.isPartitionedTable()) {
            doPartitionTable();
        } else {
            super.doFull();
        }
    }

    @Override
    protected void deleteNotExistPartitionStats(AnalysisInfo jobInfo) throws DdlException {
        TableStatsMeta tableStats = Env.getServingEnv().getAnalysisManager().findTableStatsStatus(tbl.getId());
        if (tableStats == null) {
            return;
        }
        String indexName = table.getName();
        ColStatsMeta columnStats = tableStats.findColumnStatsMeta(indexName, info.colName);
        if (columnStats == null) {
            return;
        }
        // For external table, simply remove all partition stats for the given column and re-analyze it again.
        String columnCondition = "AND col_id = " + StatisticsUtil.quote(col.getName());
        StatisticsRepository.dropPartitionsColumnStatistics(info.catalogId, info.dbId, info.tblId,
                columnCondition, "");
    }

    @Override
    protected String getPartitionInfo(String partitionName) {
        // partitionName is like "date=20230101" for one level partition
        // and like "date=20230101/hour=12" for two level partition
        String[] parts = partitionName.split("/");
        if (parts.length == 0) {
            throw new RuntimeException("Invalid partition name " + partitionName);
        }
        StringBuilder sb = new StringBuilder();
        sb.append(" WHERE ");
        for (int i = 0; i < parts.length; i++) {
            String[] split = parts[i].split("=");
            if (split.length != 2 || split[0].isEmpty() || split[1].isEmpty()) {
                throw new RuntimeException("Invalid partition name " + partitionName);
            }
            sb.append("`");
            sb.append(split[0]);
            sb.append("`");
            sb.append(" = ");
            sb.append("'");
            sb.append(split[1]);
            sb.append("'");
            if (i < parts.length - 1) {
                sb.append(" AND ");
            }
        }
        return sb.toString();
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
        return sizeToRead > LIMIT_SIZE && sizeToRead > target * LIMIT_FACTOR;
    }
}

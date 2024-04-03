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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class ExternalAnalysisTask extends BaseAnalysisTask {
    private static final Logger LOG = LogManager.getLogger(ExternalAnalysisTask.class);

    private static final String ANALYZE_TABLE_COUNT_TEMPLATE = "SELECT ROUND(COUNT(1) * ${scaleFactor}) as rowCount "
            + "FROM `${catalogName}`.`${dbName}`.`${tblName}` ${sampleHints}";
    private boolean isTableLevelTask;
    private boolean isPartitionOnly;
    private ExternalTable table;

    // For test
    public ExternalAnalysisTask() {
    }

    public ExternalAnalysisTask(AnalysisInfo info) {
        super(info);
        isTableLevelTask = info.externalTableLevelTask;
        isPartitionOnly = info.partitionOnly;
        table = (ExternalTable) tbl;
    }

    public void doExecute() throws Exception {
        if (isTableLevelTask) {
            getTableStats();
        } else {
            getColumnStats();
        }
    }

    // For test
    protected void setTable(ExternalTable table) {
        this.table = table;
    }

    /**
     * Get table row count
     */
    private void getTableStats() {
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

    // Get column stats
    protected void getColumnStats() throws Exception {
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Will do full collection for column {}", col.getName());
            }
            sb.append(COLLECT_COL_STATISTICS);
        } else {
            // Do sample analyze
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
        }
        stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(sb.toString());
        runQuery(sql);
    }

    protected Map<String, String> buildStatsParams(String partId) {
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
        commonParams.put("index", "");
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

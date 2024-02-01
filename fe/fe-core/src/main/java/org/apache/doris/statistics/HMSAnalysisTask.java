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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.external.hive.util.HiveUtil;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Sets;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
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
    protected void getOrdinaryColumnStats() throws Exception {
        if (!info.usingSqlForPartitionColumn) {
            try {
                if (isPartitionColumn()) {
                    getPartitionColumnStats();
                } else {
                    getHmsColumnStats();
                }
            } catch (Exception e) {
                LOG.warn("Failed to collect stats for {}col {} using metadata, "
                        + "fallback to normal collection",
                        isPartitionColumn() ? "partition " : "", col.getName(), e);
                /* retry using sql way! */
                super.getOrdinaryColumnStats();
            }
        } else {
            super.getOrdinaryColumnStats();
        }
    }

    // Collect the partition column stats through HMS metadata.
    // Get all the partition values and calculate the stats based on the values.
    private void getPartitionColumnStats() throws Exception {
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
        // Estimate the row count. This value is inaccurate if the table stats is empty.
        TableStatsMeta tableStatsStatus = Env.getCurrentEnv().getAnalysisManager()
                .findTableStatsStatus(hmsExternalTable.getId());
        long count = tableStatsStatus == null ? hmsExternalTable.estimatedRowCount() : tableStatsStatus.rowCount;
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

    // Collect the spark analyzed column stats through HMS metadata.
    private void getHmsColumnStats() throws Exception {
        TableStatsMeta tableStatsStatus = Env.getCurrentEnv().getAnalysisManager()
                .findTableStatsStatus(hmsExternalTable.getId());
        long count = tableStatsStatus == null ? hmsExternalTable.estimatedRowCount() : tableStatsStatus.rowCount;

        Map<String, String> params = buildStatsParams("NULL");
        Map<StatsType, String> statsParams = new HashMap<>();
        statsParams.put(StatsType.NDV, "ndv");
        statsParams.put(StatsType.NUM_NULLS, "null_count");
        statsParams.put(StatsType.MIN_VALUE, "min");
        statsParams.put(StatsType.MAX_VALUE, "max");
        statsParams.put(StatsType.AVG_SIZE, "avg_len");

        if (hmsExternalTable.fillColumnStatistics(info.colName, statsParams, params)) {
            throw new AnalysisException("some column stats not available");
        }

        long dataSize = Long.valueOf(params.get("avg_len")) * count;
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
}

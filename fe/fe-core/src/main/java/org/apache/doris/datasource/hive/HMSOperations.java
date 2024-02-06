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

package org.apache.doris.datasource.hive;

import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class HMSOperations {
    private static final String TRANSIENT_LAST_DDL_TIME = "transient_lastDdlTime";
    private static final String ROW_COUNT = "numRows";
    private static final String TOTAL_SIZE = "totalSize";
    private static final String NUM_FILES = "numFiles";
    private static final Set<String> STATS_PROPERTIES = ImmutableSet.of(ROW_COUNT, TOTAL_SIZE, NUM_FILES);
    private final HMSCachedClient client;

    public HMSOperations(HMSCachedClient client) {
        this.client = Preconditions.checkNotNull(client, "client is null");
    }

    public void updateTableStatistics(String dbName, String tableName,
                                      Function<HivePartitionStats, HivePartitionStats> updateFunc) {
        org.apache.hadoop.hive.metastore.api.Table originTable = client.getTable(dbName, tableName);
        if (originTable == null) {
            throw new HiveCommitException("Table '%s.%s' not found", dbName, tableName);
        }

        org.apache.hadoop.hive.metastore.api.Table newTable = originTable.deepCopy();
        ColumnStatistic curStats = StatisticsUtil.getColumnStatsFromHiveTable(originTable);
        HivePartitionStats curPartitionStats = new HivePartitionStats(curStats);
        HivePartitionStats updatePartitionStats = updateFunc.apply(curPartitionStats);
        ColumnStatistic updateStats = updatePartitionStats.getTableStats();
        Map<String, String> originParams = newTable.getParameters();
        originParams.put(TRANSIENT_LAST_DDL_TIME, String.valueOf(System.currentTimeMillis() / 1000));
        newTable.setParameters(updateStatisticsParams(originParams, updateStats));
        client.alterTable(dbName, tableName, newTable);
    }

    private static Map<String, String> updateStatisticsParams(Map<String, String> parameters,
                                                              ColumnStatistic statistics) {
        ImmutableMap.Builder<String, String> result = ImmutableMap.builder();

        parameters.forEach((key, value) -> {
            if (!(STATS_PROPERTIES.contains(key))) {
                result.put(key, value);
            }
        });

        result.put(ROW_COUNT, String.valueOf((long) statistics.count));
        result.put(TOTAL_SIZE, String.valueOf((long) statistics.dataSize));

        if (!parameters.containsKey("STATS_GENERATED_VIA_STATS_TASK")) {
            result.put("STATS_GENERATED_VIA_STATS_TASK", "workaround for potential lack of HIVE-12730");
        }

        return result.buildOrThrow();
    }
}

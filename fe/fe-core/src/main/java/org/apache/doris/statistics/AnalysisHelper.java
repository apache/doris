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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Check the statistics and if they need to be collected again based on their health.
 *
 * Health = 100 * (1 - 1.0 * updateRows / RowCount), it ranges from 0 to 100,
 * with higher values indicating better health.
 */
public class AnalysisHelper extends MasterDaemon {

    private static final String ID_DELIMITER = "-";
    private static final String VALUES_DELIMITER = ",";
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final Logger LOG = LogManager.getLogger(AnalysisHelper.class);

    private static final String COLUMN_STATS_FULL_NAME =
            FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.COL_STATISTIC_TBL_NAME;

    private static final String TABLES_STATS_FULL_NAME =
            FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.TBL_STATISTIC_TBL_NAME;

    private static final String FETCH_LATEST_PART_STATISTICS = "SELECT\n"
            + "  part_id,\n"
            + "  MAX(count) AS row_count,\n"
            + "  MAX(update_time) AS update_time\n"
            + "FROM\n"
            + "  ${tableFullName}\n"
            + "WHERE\n"
            + "  catalog_id = '${catalogId}'\n"
            + "  AND db_id = '${dbId}'\n"
            + "  AND tbl_id = '${tblId}'\n"
            + "  AND idx_id = '${idxId}'\n"
            + "  AND part_id IS NOT NULL\n"
            + "GROUP BY\n"
            + "  part_id";

    private static final String INSERT_TABLE_STATISTICS_TEMPLATE =
            "INSERT INTO ${tableFullName} (id, catalog_id, db_id, tbl_id, idx_id, part_id, row_count, update_rows, "
                    + "healthy, data_size_in_bytes, update_time, last_analyze_time) VALUES ${values}";

    private static final String TABLE_STATISTICS_VALUES_TEMPLATE =
            "('${id}', '${catalogId}', '${dbId}', '${tblId}', '${idxId}', '${partId}', '${rowCount}', "
                    + "'${updateRows}', '${healthy}', '${dataSize}', '${updateTime}', '${lastAnalyzeTime}')";

    private final List<String> excludedDbs = Lists.newArrayList(
            InfoSchemaDb.DATABASE_NAME,
            FeConstants.INTERNAL_DB_NAME
    );

    public AnalysisHelper() {
        super("Analysis Helper", TimeUnit.MINUTES.toMillis(Config.auto_check_statistics_in_min));
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.enable_auto_collect_statistics) {
            return;
        }

        CatalogMgr catalogMgr = Env.getCurrentEnv().getCatalogMgr();
        catalogMgr.getCatalogIds().forEach(catalogId -> {
            CatalogIf catalog = catalogMgr.getCatalog(catalogId);

            if (catalog != null) {
                catalog.getDbIds().forEach(obj -> {
                    long dbId = (long) obj;
                    DatabaseIf database = catalog.getDbNullable(dbId);

                    if (database != null) {
                        String dbName = ClusterNamespace.getNameFromFullName(database.getFullName());

                        if (!excludedDbs.contains(dbName)) {
                            database.getTables().forEach(tableObj -> {
                                TableIf table = (Table) tableObj;
                                try {
                                    updateTableStatistics(catalogId, dbId, table);
                                } catch (Exception e) {
                                    LOG.info("Failed to update table granularity statistics.");
                                    e.printStackTrace();
                                }
                            });
                        }
                    }
                });
            }
        });

        // TODO start creating analytics tasks to collect statistics based on table health
    }

    private void updateTableStatistics(long catalogId, long dbId, TableIf table)
            throws Exception {
        // 1. Update table-level statistics for the table
        // which statistics have been collected
        if (existTableStatsCache(table)) {
            long tableId = table.getId();
            // 2. Get the latest statistics for the table that has been analyzed
            List<ResultRow> statsTableInfo = getStatsTableInfo(catalogId, dbId, tableId);
            Map<Long, Partition> idToPartition = getIdToPartitionMap(table);

            List<String> values = Lists.newArrayList();
            List<Long> analyzedPartIds = Lists.newArrayList();

            long tableUpdateRowcount = 0L;
            long tableRelatedRowcount = 0L;
            long tableLatestAnalyzeTime = 0L;

            // 2.1 Process old (statistics already collected) partitions
            for (ResultRow row : statsTableInfo) {
                long partId = Long.parseLong(row.getColumnValue("part_id"));

                if (idToPartition.containsKey(partId)) {
                    String updateTimeStr = row.getColumnValue("update_time");
                    long statUpdateTimestamp = getTimestampFromStr(updateTimeStr);
                    Partition partition = idToPartition.get(partId);
                    long ptVisibleTime = partition.getVisibleVersionTime();
                    MaterializedIndex baseIndex = partition.getBaseIndex();
                    long ptRowCount = baseIndex.getRowCount();
                    long ptDataSize = baseIndex.getDataSize();

                    Map<String, String> valueParams = getCommonParams(catalogId, dbId, tableId);
                    valueParams.put("id", constructId(tableId, -1, partId));
                    valueParams.put("partId", String.valueOf(partId));
                    valueParams.put("rowCount", String.valueOf(ptRowCount));
                    valueParams.put("dataSize", String.valueOf(ptDataSize));
                    valueParams.put("updateTime", getDateStrFromTimestamp(ptVisibleTime));
                    valueParams.put("lastAnalyzeTime", updateTimeStr);

                    long updateRows = 0;

                    if (statUpdateTimestamp >= ptVisibleTime) {
                        valueParams.put("healthy", "100");
                        valueParams.put("updateRows", String.valueOf(updateRows));
                    } else {
                        // If partition changes after statistics are collected,
                        // it needs to be updated, otherwise we consider the stats to be "healthy".
                        long rowCount = Long.parseLong(row.getColumnValue("row_count"));
                        updateRows = ptRowCount - rowCount;

                        int healthy = 100;
                        if (updateRows > 0) {
                            // updateRows > 0 increased the number of lines
                            healthy = (int) (100 * (1 - 1.0 * updateRows / ptRowCount));
                        } else if (updateRows < 0) {
                            // updateRows < 0 decreased the number of lines
                            healthy = (int) (100 * (1 - 1.0 * Math.abs(updateRows) / rowCount));
                        }

                        // TODO updateRows = 0 it may be update-operation,
                        //  We should monitor the number of updated rows for the partition.

                        valueParams.put("healthy", String.valueOf(healthy));
                        valueParams.put("updateRows", String.valueOf(Math.abs(updateRows)));

                        tableUpdateRowcount += Math.abs(updateRows);
                        tableRelatedRowcount += Math.max(ptRowCount, rowCount);
                    }

                    StringSubstitutor stringSubstitutor = new StringSubstitutor(valueParams);
                    values.add(stringSubstitutor.replace(TABLE_STATISTICS_VALUES_TEMPLATE));
                    analyzedPartIds.add(partId);

                    tableLatestAnalyzeTime = Math.max(tableLatestAnalyzeTime, statUpdateTimestamp);
                }
            }

            // 2.2 Handle new (no stats collected and newer update time) partitions
            long finalPartLatestAnalyzeTime = tableLatestAnalyzeTime;
            idToPartition.entrySet().stream()
                    .filter(p -> !analyzedPartIds.contains(p.getKey())
                            && p.getValue().getVisibleVersionTime() > finalPartLatestAnalyzeTime)
                    .collect(Collectors.toList())
                    .forEach(idPartition -> {
                        long partId = idPartition.getKey();
                        Partition partition = idPartition.getValue();
                        long ptVisibleTime = partition.getVisibleVersionTime();
                        MaterializedIndex baseIndex = partition.getBaseIndex();

                        Map<String, String> valueParams = getCommonParams(catalogId, dbId, tableId);
                        valueParams.put("id", constructId(tableId, -1, partId));
                        valueParams.put("partId", String.valueOf(partId));
                        valueParams.put("rowCount", String.valueOf(baseIndex.getRowCount()));
                        valueParams.put("dataSize", String.valueOf(baseIndex.getDataSize()));
                        valueParams.put("updateTime", getDateStrFromTimestamp(ptVisibleTime));
                        valueParams.put("updateRows", "0");
                        valueParams.put("healthy", "0");
                        valueParams.put("lastAnalyzeTime", "NULL");

                        StringSubstitutor stringSubstitutor = new StringSubstitutor(valueParams);
                        values.add(stringSubstitutor.replace(TABLE_STATISTICS_VALUES_TEMPLATE));
                    });

            // 3. Updating Statistics at Table Granularity
            if (!values.isEmpty()) {
                Map<String, String> insertParams = getCommonParams(catalogId, dbId, tableId);
                Map<String, String> tblValueParams = getCommonParams(catalogId, dbId, tableId);

                tblValueParams.put("id", constructId(tableId, -1));
                tblValueParams.put("partId", "NULL");

                long rowCount = table.getRowCount();
                tblValueParams.put("rowCount", String.valueOf(rowCount));

                long dataSize = table.getDataLength();
                tblValueParams.put("dataSize", String.valueOf(dataSize));

                if (tableLatestAnalyzeTime == 0) {
                    tblValueParams.put("lastAnalyzeTime", "NULL");
                } else {
                    tblValueParams.put("lastAnalyzeTime", getDateStrFromTimestamp(tableLatestAnalyzeTime));
                }

                if (tableUpdateRowcount == 0) {
                    tblValueParams.put("healthy", "100");
                } else {
                    int healthy = (int) (100 * (1 - 1.0 * tableUpdateRowcount / tableRelatedRowcount));
                    tblValueParams.put("healthy", String.valueOf(healthy));
                }

                tblValueParams.put("updateTime", getDateStrFromTimestamp(table.getUpdateTime()));
                tblValueParams.put("updateRows", String.valueOf(tableUpdateRowcount));

                StringSubstitutor stringSubstitutor = new StringSubstitutor(tblValueParams);
                values.add(stringSubstitutor.replace(TABLE_STATISTICS_VALUES_TEMPLATE));

                String valuesStr = getCommaJoinerStr(values, VALUES_DELIMITER);
                insertParams.put("values", valuesStr);

                insertParams.put("tableFullName", TABLES_STATS_FULL_NAME);
                StatisticsUtil.execUpdate(INSERT_TABLE_STATISTICS_TEMPLATE, insertParams);
            }
        }

        // 4. Else the table has not collected statistics, skip them
    }

    private List<ResultRow> getStatsTableInfo(long catalogId, long dbId, long tableId) {
        Map<String, String> queryParams = getCommonParams(catalogId, dbId, tableId);
        queryParams.put("tableFullName", COLUMN_STATS_FULL_NAME);
        return StatisticsUtil.executeQuery(FETCH_LATEST_PART_STATISTICS, queryParams);
    }

    private String constructId(long... items) {
        if (items == null || items.length == 0) {
            return "";
        }
        List<String> idElements = Arrays.stream(items)
                .mapToObj(String::valueOf)
                .collect(Collectors.toList());
        return getCommaJoinerStr(idElements, ID_DELIMITER);
    }

    private Map<String, String> getCommonParams(long catalogId, long dbId, long tableId) {
        return new HashMap<String, String>() {
            {
                put("catalogId", String.valueOf(catalogId));
                put("dbId", String.valueOf(dbId));
                put("tblId", String.valueOf(tableId));
                put("idxId", "-1");
            }
        };
    }

    private Map<Long, Partition> getIdToPartitionMap(TableIf table) {
        return table.getPartitionNames().stream()
                .map(table::getPartition)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Partition::getId, Function.identity()));
    }

    private String getCommaJoinerStr(List<String> values, String delimiter) {
        StringJoiner builder = new StringJoiner(delimiter);
        values.forEach(builder::add);
        return builder.toString();
    }

    private static long getTimestampFromStr(String dateString) {
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        try {
            Date date = format.parse(dateString);
            return date.getTime();
        } catch (ParseException e) {
            LOG.warn("Fail to parse Timestamp from {}, {}", dateString, e.getMessage());
            return 0;
        }
    }

    private static String getDateStrFromTimestamp(long timestamp) {
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return format.format(new Date(timestamp));
    }

    private boolean existTableStatsCache(TableIf table) {
        StatisticsCache statisticsCache = Env.getCurrentEnv().getStatisticsCache();
        return table.getColumns()
                .stream()
                .anyMatch(column -> {
                    ColumnLevelStatisticCache colStatCache = statisticsCache
                            .getColumnStatistics(table.getId(), -1, column.getName());
                    return colStatCache != null && colStatCache.columnStatistic != null;
                });
    }
}

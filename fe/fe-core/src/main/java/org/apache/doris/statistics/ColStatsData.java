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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.StringJoiner;

/**
 * Used to convert data from ResultRow.
 * 0: id
 * 1: catalog_id
 * 2: db_id
 * 3: tbl_id
 * 4: idx_id
 * 5: col_id
 * 6: part_id
 * 7: count
 * 8: ndv
 * 9: null_count
 * 10: min
 * 11: max
 * 12: data_size_in_bytes
 * 13: update_time
 */
public class ColStatsData {
    private static final Logger LOG = LogManager.getLogger(ColStatsData.class);

    @SerializedName("statsId")
    public final StatsId statsId;
    @SerializedName("count")
    public final long count;
    @SerializedName("ndv")
    public final long ndv;
    @SerializedName("nullCount")
    public final long nullCount;
    @SerializedName("minLit")
    public final String minLit;
    @SerializedName("maxLit")
    public final String maxLit;
    @SerializedName("dataSizeInBytes")
    public final long dataSizeInBytes;
    @SerializedName("updateTime")
    public final String updateTime;

    @VisibleForTesting
    public ColStatsData() {
        statsId = new StatsId();
        count = 0;
        ndv = 0;
        nullCount = 0;
        minLit = null;
        maxLit = null;
        dataSizeInBytes = 0;
        updateTime = null;
    }

    public ColStatsData(StatsId statsId) {
        this.statsId = statsId;
        count = 0;
        ndv = 0;
        nullCount = 0;
        minLit = null;
        maxLit = null;
        dataSizeInBytes = 0;
        updateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public ColStatsData(ResultRow row) {
        this.statsId = new StatsId(row);
        this.count = (long) Double.parseDouble(row.getWithDefault(7, "0"));
        this.ndv = (long) Double.parseDouble(row.getWithDefault(8, "0"));
        this.nullCount = (long) Double.parseDouble(row.getWithDefault(9, "0"));
        this.minLit = row.get(10);
        this.maxLit = row.get(11);
        this.dataSizeInBytes = (long) Double.parseDouble(row.getWithDefault(12, "0"));
        this.updateTime = row.get(13);
    }

    public ColStatsData(String id, long catalogId, long dbId, long tblId, long idxId, String colId, String partId,
                        ColumnStatistic columnStatistic) {
        this.statsId = new StatsId(id, catalogId, dbId, tblId, idxId, colId, partId);
        this.count = Math.round(columnStatistic.count);
        this.ndv = Math.round(columnStatistic.ndv);
        this.nullCount = Math.round(columnStatistic.numNulls);
        this.minLit = columnStatistic.minExpr == null ? null : columnStatistic.minExpr.getStringValue();
        this.maxLit = columnStatistic.maxExpr == null ? null : columnStatistic.maxExpr.getStringValue();
        this.dataSizeInBytes = Math.round(columnStatistic.dataSize);
        this.updateTime = columnStatistic.updatedTime;
    }

    public String toSQL(boolean roundByParentheses) {
        StringJoiner sj = null;
        if (roundByParentheses) {
            sj = new StringJoiner(",", "(" + statsId.toSQL() + ",", ")");
        } else {
            sj = new StringJoiner(",", statsId.toSQL(), "");
        }
        sj.add(String.valueOf(count));
        sj.add(String.valueOf(ndv));
        sj.add(String.valueOf(nullCount));
        sj.add(minLit == null ? "NULL" : "'" + StatisticsUtil.escapeSQL(minLit) + "'");
        sj.add(maxLit == null ? "NULL" : "'" + StatisticsUtil.escapeSQL(maxLit) + "'");
        sj.add(String.valueOf(dataSizeInBytes));
        sj.add(StatisticsUtil.quote(updateTime));
        return sj.toString();
    }

    public ColumnStatistic toColumnStatistic() {
        // For non-empty table, return UNKNOWN if we can't collect ndv value.
        // Because inaccurate ndv is very misleading.
        if (count > 0 && ndv == 0 && count != nullCount) {
            return ColumnStatistic.UNKNOWN;
        }
        try {
            ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder();
            columnStatisticBuilder.setCount(count);
            columnStatisticBuilder.setNdv(ndv);
            columnStatisticBuilder.setNumNulls(nullCount);
            columnStatisticBuilder.setDataSize(dataSizeInBytes);
            columnStatisticBuilder.setAvgSizeByte(count == 0 ? 0 : ((double) dataSizeInBytes) / count);
            if (statsId == null) {
                return ColumnStatistic.UNKNOWN;
            }
            long catalogId = statsId.catalogId;
            long idxId = statsId.idxId;
            long dbID = statsId.dbId;
            long tblId = statsId.tblId;
            String colName = statsId.colId;
            Column col = StatisticsUtil.findColumn(catalogId, dbID, tblId, idxId, colName);
            if (col == null) {
                return ColumnStatistic.UNKNOWN;
            }
            String min = minLit;
            String max = maxLit;
            if (min != null && !min.equalsIgnoreCase("NULL")) {
                try {
                    columnStatisticBuilder.setMinValue(StatisticsUtil.convertToDouble(col.getType(), min));
                    columnStatisticBuilder.setMinExpr(StatisticsUtil.readableValue(col.getType(), min));
                } catch (AnalysisException e) {
                    LOG.warn("Failed to process column {} min value {}.", col, min, e);
                    columnStatisticBuilder.setMinValue(Double.NEGATIVE_INFINITY);
                }
            } else {
                columnStatisticBuilder.setMinValue(Double.NEGATIVE_INFINITY);
            }
            if (max != null && !max.equalsIgnoreCase("NULL")) {
                try {
                    columnStatisticBuilder.setMaxValue(StatisticsUtil.convertToDouble(col.getType(), max));
                    columnStatisticBuilder.setMaxExpr(StatisticsUtil.readableValue(col.getType(), max));
                } catch (AnalysisException e) {
                    LOG.warn("Failed to process column {} max value {}.", col, max, e);
                    columnStatisticBuilder.setMaxValue(Double.POSITIVE_INFINITY);
                }
            } else {
                columnStatisticBuilder.setMaxValue(Double.POSITIVE_INFINITY);
            }
            columnStatisticBuilder.setUpdatedTime(updateTime);
            return columnStatisticBuilder.build();
        } catch (Exception e) {
            LOG.warn("Failed to convert column statistics.", e);
            return ColumnStatistic.UNKNOWN;
        }
    }
}

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

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnStatistic {

    public static final StatsType NDV = StatsType.NDV;
    public static final StatsType AVG_SIZE = StatsType.AVG_SIZE;
    public static final StatsType MAX_SIZE = StatsType.MAX_SIZE;
    public static final StatsType NUM_NULLS = StatsType.NUM_NULLS;
    public static final StatsType MIN_VALUE = StatsType.MIN_VALUE;
    public static final StatsType MAX_VALUE = StatsType.MAX_VALUE;

    private static final Logger LOG = LogManager.getLogger(ColumnStatistic.class);

    public static ColumnStatistic UNKNOWN = new ColumnStatisticBuilder().setAvgSizeByte(1).setNdv(1)
            .setNumNulls(1).setCount(1).setMaxValue(Double.POSITIVE_INFINITY).setMinValue(Double.NEGATIVE_INFINITY)
            .setSelectivity(1.0).setIsUnknown(true)
            .build();

    public static ColumnStatistic ZERO = new ColumnStatisticBuilder().setAvgSizeByte(0).setNdv(0)
            .setNumNulls(0).setCount(0).setMaxValue(Double.NaN).setMinValue(Double.NaN)
            .setSelectivity(0)
            .build();

    public static final Set<Type> UNSUPPORTED_TYPE = new HashSet<>();

    static {
        UNSUPPORTED_TYPE.add(Type.HLL);
        UNSUPPORTED_TYPE.add(Type.BITMAP);
        UNSUPPORTED_TYPE.add(Type.ARRAY);
        UNSUPPORTED_TYPE.add(Type.STRUCT);
        UNSUPPORTED_TYPE.add(Type.MAP);
        UNSUPPORTED_TYPE.add(Type.QUANTILE_STATE);
        UNSUPPORTED_TYPE.add(Type.AGG_STATE);
        UNSUPPORTED_TYPE.add(Type.JSONB);
        UNSUPPORTED_TYPE.add(Type.VARIANT);
        UNSUPPORTED_TYPE.add(Type.TIME);
        UNSUPPORTED_TYPE.add(Type.TIMEV2);
        UNSUPPORTED_TYPE.add(Type.LAMBDA_FUNCTION);
    }

    @SerializedName("count")
    public final double count;
    @SerializedName("ndv")
    public final double ndv;
    @SerializedName("numNulls")
    public final double numNulls;
    @SerializedName("dataSize")
    public final double dataSize;
    @SerializedName("avgSizeByte")
    public final double avgSizeByte;
    @SerializedName("minValue")
    public final double minValue;
    @SerializedName("maxValue")
    public final double maxValue;
    public final boolean isUnKnown;
    /*
    selectivity of Column T1.A:
    if T1.A = T2.B is the inner join condition, for a given `b` in B, b in
    intersection of range(A) and range(B), selectivity means the probability that
    the equation can be satisfied.
    We take tpch as example.
    l_orderkey = o_orderkey and o_orderstatus='o'
        there are 3 distinct o_orderstatus in orders table. filter o_orderstatus='o' reduces orders table by 1/3
        because o_orderkey is primary key, thus the o_orderkey.selectivity = 1/3,
        and after join(l_orderkey = o_orderkey), lineitem is reduced by 1/3.
        But after filter, other columns' selectivity is still 1.0
     */
    public final double selectivity;

    /*
    originalNdv is the ndv in stats of ScanNode. ndv may be changed after filter or join,
    but originalNdv is not. It is used to trace the change of a column's ndv through serials
    of sql operators.
     */
    public final ColumnStatistic original;

    // For display only.
    public final LiteralExpr minExpr;
    public final LiteralExpr maxExpr;

    @SerializedName("histogram")
    // assign value when do stats estimation.
    public final Histogram histogram;

    public final Map<Long, ColumnStatistic> partitionIdToColStats = new HashMap<>();

    public final String updatedTime;

    public ColumnStatistic(double count, double ndv, ColumnStatistic original, double avgSizeByte,
            double numNulls, double dataSize, double minValue, double maxValue,
            double selectivity, LiteralExpr minExpr, LiteralExpr maxExpr, boolean isUnKnown, Histogram histogram,
            String updatedTime) {
        this.count = count;
        this.ndv = ndv;
        this.original = original;
        this.avgSizeByte = avgSizeByte;
        this.numNulls = numNulls;
        this.dataSize = dataSize;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.selectivity = selectivity;
        this.minExpr = minExpr;
        this.maxExpr = maxExpr;
        this.isUnKnown = isUnKnown;
        this.histogram = histogram;
        this.updatedTime = updatedTime;
    }

    public static ColumnStatistic fromResultRow(List<ResultRow> resultRows) {
        Map<Long, ColumnStatistic> partitionIdToColStats = new HashMap<>();
        ColumnStatistic columnStatistic = null;
        try {
            for (ResultRow resultRow : resultRows) {
                String partId = resultRow.getColumnValue("part_id");
                if (partId == null) {
                    columnStatistic = fromResultRow(resultRow);
                } else {
                    partitionIdToColStats.put(Long.parseLong(partId), fromResultRow(resultRow));
                }
            }
        } catch (Throwable t) {
            LOG.debug("Failed to deserialize column stats", t);
            return ColumnStatistic.UNKNOWN;
        }
        // Means last analyze failed or interrupted for some reason.
        if (columnStatistic == null) {
            return ColumnStatistic.UNKNOWN;
        }
        columnStatistic.partitionIdToColStats.putAll(partitionIdToColStats);
        return columnStatistic;
    }

    // TODO: use thrift
    public static ColumnStatistic fromResultRow(ResultRow resultRow) {
        try {
            ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder();
            double count = Double.parseDouble(resultRow.getColumnValueWithDefault("count", "0"));
            columnStatisticBuilder.setCount(count);
            double ndv = Double.parseDouble(resultRow.getColumnValueWithDefault("ndv", "0"));
            if (0.99 * count < ndv && ndv < 1.01 * count) {
                ndv = count;
            }
            columnStatisticBuilder.setNdv(ndv);
            String nullCount = resultRow.getColumnValueWithDefault("null_count", "0");
            columnStatisticBuilder.setNumNulls(Double.parseDouble(nullCount));
            columnStatisticBuilder.setDataSize(Double
                    .parseDouble(resultRow.getColumnValueWithDefault("data_size_in_bytes", "0")));
            columnStatisticBuilder.setAvgSizeByte(columnStatisticBuilder.getCount() == 0
                    ? 0 : columnStatisticBuilder.getDataSize()
                    / columnStatisticBuilder.getCount());
            long catalogId = Long.parseLong(resultRow.getColumnValue("catalog_id"));
            long idxId = Long.parseLong(resultRow.getColumnValue("idx_id"));
            long dbID = Long.parseLong(resultRow.getColumnValue("db_id"));
            long tblId = Long.parseLong(resultRow.getColumnValue("tbl_id"));
            String colName = resultRow.getColumnValue("col_id");
            Column col = StatisticsUtil.findColumn(catalogId, dbID, tblId, idxId, colName);
            if (col == null) {
                LOG.warn("Failed to deserialize column statistics, ctlId: {} dbId: {}"
                        + "tblId: {} column: {} not exists",
                        catalogId, dbID, tblId, colName);
                return ColumnStatistic.UNKNOWN;
            }
            String min = resultRow.getColumnValue("min");
            String max = resultRow.getColumnValue("max");
            if (min != null && !min.equalsIgnoreCase("NULL")) {
                try {
                    columnStatisticBuilder.setMinValue(StatisticsUtil.convertToDouble(col.getType(), min));
                    columnStatisticBuilder.setMinExpr(StatisticsUtil.readableValue(col.getType(), min));
                } catch (AnalysisException e) {
                    LOG.warn("Failed to deserialize column {} min value {}.", col, min, e);
                    columnStatisticBuilder.setMinValue(Double.MIN_VALUE);
                }
            } else {
                columnStatisticBuilder.setMinValue(Double.MIN_VALUE);
            }
            if (max != null && !max.equalsIgnoreCase("NULL")) {
                try {
                    columnStatisticBuilder.setMaxValue(StatisticsUtil.convertToDouble(col.getType(), max));
                    columnStatisticBuilder.setMaxExpr(StatisticsUtil.readableValue(col.getType(), max));
                } catch (AnalysisException e) {
                    LOG.warn("Failed to deserialize column {} max value {}.", col, max, e);
                    columnStatisticBuilder.setMaxValue(Double.MAX_VALUE);
                }
            } else {
                columnStatisticBuilder.setMaxValue(Double.MAX_VALUE);
            }
            columnStatisticBuilder.setSelectivity(1.0);
            Histogram histogram = Env.getCurrentEnv().getStatisticsCache().getHistogram(tblId, idxId, colName)
                    .orElse(null);
            columnStatisticBuilder.setHistogram(histogram);
            columnStatisticBuilder.setUpdatedTime(resultRow.getColumnValue("update_time"));
            return columnStatisticBuilder.build();
        } catch (Exception e) {
            LOG.warn("Failed to deserialize column statistics.", e);
            return ColumnStatistic.UNKNOWN;
        }
    }

    public static boolean isAlmostUnique(double ndv, double rowCount) {
        return rowCount * 0.9 < ndv && ndv < rowCount * 1.1;
    }

    public ColumnStatistic copy() {
        return new ColumnStatisticBuilder().setCount(count).setNdv(ndv).setAvgSizeByte(avgSizeByte)
                .setNumNulls(numNulls).setDataSize(dataSize).setMinValue(minValue)
                .setMaxValue(maxValue).setMinExpr(minExpr).setMaxExpr(maxExpr)
                .setSelectivity(selectivity).setIsUnknown(isUnKnown).build();
    }

    public ColumnStatistic updateByLimit(long limit, double rowCount) {
        double ratio = 0;
        if (rowCount != 0) {
            ratio = limit / rowCount;
        }
        double newNdv = Math.ceil(Math.min(ndv, limit));
        double newSelectivity = selectivity;
        if (newNdv != 0) {
            newSelectivity = newSelectivity * newNdv / ndv;
        } else {
            newSelectivity = 0;
        }
        return new ColumnStatisticBuilder()
                .setCount(Math.ceil(limit))
                .setNdv(newNdv)
                .setAvgSizeByte(Math.ceil(avgSizeByte))
                .setNumNulls(Math.ceil(numNulls * ratio))
                .setDataSize(Math.ceil(dataSize * ratio))
                .setMinValue(minValue)
                .setMaxValue(maxValue)
                .setMinExpr(minExpr)
                .setMaxExpr(maxExpr)
                .setSelectivity(newSelectivity)
                .setIsUnknown(isUnKnown)
                .build();
    }

    public boolean hasIntersect(ColumnStatistic other) {
        return Math.max(this.minValue, other.minValue) <= Math.min(this.maxValue, other.maxValue);
    }

    public ColumnStatistic updateBySelectivity(double selectivity, double rowCount) {
        if (isUnKnown) {
            return UNKNOWN;
        }
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder(this);
        Double rowsAfterFilter = rowCount * selectivity;
        if (isAlmostUnique(ndv, rowCount)) {
            builder.setSelectivity(this.selectivity * selectivity);
            builder.setNdv(ndv * selectivity);
        } else {
            if (ndv > rowsAfterFilter) {
                builder.setSelectivity(this.selectivity * rowsAfterFilter / ndv);
                builder.setNdv(rowsAfterFilter);
            } else {
                builder.setSelectivity(this.selectivity);
                builder.setNdv(this.ndv);
            }
        }
        builder.setNumNulls((long) Math.ceil(numNulls * selectivity));
        return builder.build();
    }

    public double ndvIntersection(ColumnStatistic other) {
        if (isUnKnown) {
            return 1;
        }
        if (maxValue == minValue) {
            if (minValue <= other.maxValue && minValue >= other.minValue) {
                return 1;
            } else {
                return 0;
            }
        }
        double min = Math.max(minValue, other.minValue);
        double max = Math.min(maxValue, other.maxValue);
        if (min < max) {
            return Math.ceil(ndv * (max - min) / (maxValue - minValue));
        } else if (min > max) {
            return 0;
        } else {
            return 1;
        }
    }

    public boolean notEnclosed(ColumnStatistic other) {
        return !enclosed(other);
    }

    /**
     * Return true if range of this is enclosed by another.
     */
    public boolean enclosed(ColumnStatistic other) {
        return this.maxValue >= other.maxValue && this.maxValue <= other.maxValue;
    }

    @Override
    public String toString() {
        return isUnKnown ? "unknown" : String.format("ndv=%.4f, min=%f(%s), max=%f(%s), count=%.4f",
                ndv, minValue, minExpr, maxValue, maxExpr, count);
    }

    public JSONObject toJson() {
        JSONObject statistic = new JSONObject();
        statistic.put("Ndv", ndv);
        if (Double.isInfinite(minValue)) {
            statistic.put("MinValueType", "Infinite");
        } else if (Double.isNaN(minValue)) {
            statistic.put("MinValueType", "Invalid");
        } else {
            statistic.put("MinValueType", "Normal");
            statistic.put("MinValue", minValue);
        }
        if (Double.isInfinite(maxValue)) {
            statistic.put("MaxValueType", "Infinite");
        } else if (Double.isNaN(maxValue)) {
            statistic.put("MaxValueType", "Invalid");
        } else {
            statistic.put("MaxValueType", "Normal");
            statistic.put("MaxValue", maxValue);
        }
        statistic.put("Selectivity", selectivity);
        statistic.put("Count", count);
        statistic.put("AvgSizeByte", avgSizeByte);
        statistic.put("NumNulls", numNulls);
        statistic.put("DataSize", dataSize);
        statistic.put("Selectivity", selectivity);
        statistic.put("MinExpr", minExpr);
        statistic.put("MaxExpr", maxExpr);
        statistic.put("IsUnKnown", isUnKnown);
        statistic.put("Histogram", Histogram.serializeToJson(histogram));
        statistic.put("Original", original);
        return statistic;
    }

    // MinExpr and MaxExpr serialize and deserialize is not complete
    // Histogram is got by other place
    public static ColumnStatistic fromJson(String statJson) {
        JSONObject stat = new JSONObject(statJson);
        Double minValue;
        switch (stat.getString("MinValueType")) {
            case "Infinite":
                minValue = Double.NEGATIVE_INFINITY;
                break;
            case "Invalid":
                minValue = Double.NaN;
                break;
            case "Normal":
                minValue = stat.getDouble("MinValue");
                break;
            default:
                throw new RuntimeException(String.format("Min value does not get anytype"));
        }
        Double maxValue;
        switch (stat.getString("MaxValueType")) {
            case "Infinite":
                maxValue = Double.POSITIVE_INFINITY;
                break;
            case "Invalid":
                maxValue = Double.NaN;
                break;
            case "Normal":
                maxValue = stat.getDouble("MaxValue");
                break;
            default:
                throw new RuntimeException(String.format("Min value does not get anytype"));
        }
        return new ColumnStatistic(
            stat.getDouble("Count"),
            stat.getDouble("Ndv"),
            null,
            stat.getDouble("AvgSizeByte"),
            stat.getDouble("NumNulls"),
            stat.getDouble("DataSize"),
            minValue,
            maxValue,
            stat.getDouble("Selectivity"),
            null,
            null,
            stat.getBoolean("IsUnKnown"),
            Histogram.deserializeFromJson(stat.getString("Histogram")),
            stat.getString("lastUpdatedTine")
        );
    }

    public boolean minOrMaxIsInf() {
        return Double.isInfinite(maxValue) || Double.isInfinite(minValue);
    }

    public boolean hasHistogram() {
        return histogram != null && histogram != Histogram.UNKNOWN;
    }

    public double getOriginalNdv() {
        if (original != null) {
            return original.ndv;
        }
        return ndv;
    }

    // TODO expanded this function to support more cases, help to compute the change of ndv density
    public boolean rangeChanged() {
        return original != null && (minValue != original.minValue || maxValue != original.maxValue);
    }

    public boolean isUnKnown() {
        return isUnKnown;
    }

    public void loadPartitionStats(long tableId, long idxId, String colName) throws DdlException {
        List<ResultRow> resultRows = StatisticsRepository.loadPartStats(tableId, idxId, colName);
        for (ResultRow resultRow : resultRows) {
            String partId = resultRow.getColumnValue("part_id");
            ColumnStatistic columnStatistic = ColumnStatistic.fromResultRow(resultRow);
            partitionIdToColStats.put(Long.parseLong(partId), columnStatistic);
        }
    }
}

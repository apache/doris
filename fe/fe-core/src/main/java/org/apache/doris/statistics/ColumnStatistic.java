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
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnStatistic {

    public static final double STATS_ERROR = 0.1D;

    public static final StatsType NDV = StatsType.NDV;
    public static final StatsType AVG_SIZE = StatsType.AVG_SIZE;
    public static final StatsType MAX_SIZE = StatsType.MAX_SIZE;
    public static final StatsType NUM_NULLS = StatsType.NUM_NULLS;
    public static final StatsType MIN_VALUE = StatsType.MIN_VALUE;
    public static final StatsType MAX_VALUE = StatsType.MAX_VALUE;

    private static final Logger LOG = LogManager.getLogger(ColumnStatistic.class);

    public static ColumnStatistic UNKNOWN = new ColumnStatisticBuilder().setAvgSizeByte(1).setNdv(1)
            .setNumNulls(1).setCount(1).setMaxValue(Double.POSITIVE_INFINITY).setMinValue(Double.NEGATIVE_INFINITY)
            .setIsUnknown(true).setUpdatedTime("")
            .build();

    public static ColumnStatistic ZERO = new ColumnStatisticBuilder().setAvgSizeByte(0).setNdv(0)
            .setNumNulls(0).setCount(0).setMaxValue(Double.NaN).setMinValue(Double.NaN)
            .build();

    public static final Set<Type> UNSUPPORTED_TYPE = Sets.newHashSet(
            Type.HLL, Type.BITMAP, Type.ARRAY, Type.STRUCT, Type.MAP, Type.QUANTILE_STATE, Type.AGG_STATE, Type.JSONB,
            Type.VARIANT, Type.TIME, Type.TIMEV2, Type.LAMBDA_FUNCTION
    );

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
    originalNdv is the ndv in stats of ScanNode. ndv may be changed after filter or join,
    but originalNdv is not. It is used to trace the change of a column's ndv through serials
    of sql operators.
     */
    public final ColumnStatistic original;

    public final LiteralExpr minExpr;
    public final LiteralExpr maxExpr;

    @SerializedName("histogram")
    // assign value when do stats estimation.
    public final Histogram histogram;

    @SerializedName("partitionIdToColStats")
    public final Map<String, ColumnStatistic> partitionIdToColStats = new HashMap<>();

    public final String updatedTime;

    public final PartitionInfo partitionInfo;

    public ColumnStatistic(double count, double ndv, ColumnStatistic original, double avgSizeByte,
            double numNulls, double dataSize, double minValue, double maxValue,
            LiteralExpr minExpr, LiteralExpr maxExpr, boolean isUnKnown, Histogram histogram,
            String updatedTime, PartitionInfo partitionInfo) {
        this.count = count;
        this.ndv = ndv;
        this.original = original;
        this.avgSizeByte = avgSizeByte;
        this.numNulls = numNulls;
        this.dataSize = dataSize;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.minExpr = minExpr;
        this.maxExpr = maxExpr;
        this.isUnKnown = isUnKnown;
        this.histogram = histogram;
        this.updatedTime = updatedTime;
        this.partitionInfo = partitionInfo;
    }

    public static ColumnStatistic fromResultRow(List<ResultRow> resultRows) {
        Map<String, ColumnStatistic> partitionIdToColStats = new HashMap<>();
        ColumnStatistic columnStatistic = null;
        try {
            for (ResultRow resultRow : resultRows) {
                String partId = resultRow.get(6);
                if (partId == null) {
                    columnStatistic = fromResultRow(resultRow);
                } else {
                    partitionIdToColStats.put(partId, fromResultRow(resultRow));
                }
            }
        } catch (Throwable t) {
            LOG.debug("Failed to deserialize column stats", t);
            return ColumnStatistic.UNKNOWN;
        }
        if (columnStatistic == null) {
            return ColumnStatistic.UNKNOWN;
        }
        columnStatistic.partitionIdToColStats.putAll(partitionIdToColStats);
        return columnStatistic;
    }

    // TODO: use thrift
    public static ColumnStatistic fromResultRow(ResultRow row) {
        try {
            ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder();
            double count = Double.parseDouble(row.get(7));
            columnStatisticBuilder.setCount(count);
            double ndv = Double.parseDouble(row.getWithDefault(8, "0"));
            columnStatisticBuilder.setNdv(ndv);
            String nullCount = row.getWithDefault(9, "0");
            columnStatisticBuilder.setNumNulls(Double.parseDouble(nullCount));
            columnStatisticBuilder.setDataSize(Double
                    .parseDouble(row.getWithDefault(12, "0")));
            columnStatisticBuilder.setAvgSizeByte(columnStatisticBuilder.getCount() == 0
                    ? 0 : columnStatisticBuilder.getDataSize()
                    / columnStatisticBuilder.getCount());
            long catalogId = Long.parseLong(row.get(1));
            long idxId = Long.parseLong(row.get(4));
            long dbID = Long.parseLong(row.get(2));
            long tblId = Long.parseLong(row.get(3));
            String colName = row.get(5);
            Column col = StatisticsUtil.findColumn(catalogId, dbID, tblId, idxId, colName);
            if (col == null) {
                LOG.debug("Failed to deserialize column statistics, ctlId: {} dbId: {}"
                                + "tblId: {} column: {} not exists",
                        catalogId, dbID, tblId, colName);
                return ColumnStatistic.UNKNOWN;
            }
            String min = row.get(10);
            String max = row.get(11);
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
            columnStatisticBuilder.setUpdatedTime(row.get(13));
            return columnStatisticBuilder.build();
        } catch (Exception e) {
            LOG.warn("Failed to deserialize column statistics.", e);
            return ColumnStatistic.UNKNOWN;
        }
    }

    public static boolean isAlmostUnique(double ndv, double rowCount) {
        return rowCount * 0.9 < ndv && ndv < rowCount * 1.1;
    }

    public ColumnStatistic updateByLimit(long limit, double rowCount) {
        double ratio = 0;
        if (rowCount != 0) {
            ratio = limit / rowCount;
        }
        double newNdv = Math.ceil(Math.min(ndv, limit));
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
            builder.setNdv(ndv * selectivity);
        } else {
            if (ndv > rowsAfterFilter) {
                builder.setNdv(rowsAfterFilter);
            } else {
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
        return isUnKnown ? "unknown" : String.format("ndv=%.4f, min=%f(%s), max=%f(%s), count=%.4f, avgSizeByte=%f",
                ndv, minValue, minExpr, maxValue, maxExpr, count, avgSizeByte);
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
        statistic.put("Count", count);
        statistic.put("AvgSizeByte", avgSizeByte);
        statistic.put("NumNulls", numNulls);
        statistic.put("DataSize", dataSize);
        statistic.put("MinExpr", minExpr);
        statistic.put("MaxExpr", maxExpr);
        statistic.put("IsUnKnown", isUnKnown);
        statistic.put("Histogram", Histogram.serializeToJson(histogram));
        statistic.put("Original", original);
        statistic.put("LastUpdatedTime", updatedTime);
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
            null,
            null,
            stat.getBoolean("IsUnKnown"),
            Histogram.deserializeFromJson(stat.getString("Histogram")),
            stat.getString("LastUpdatedTime"), null
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

    public void putPartStats(String partId, ColumnStatistic columnStatistic) {
        this.partitionIdToColStats.put(partId, columnStatistic);
    }
}

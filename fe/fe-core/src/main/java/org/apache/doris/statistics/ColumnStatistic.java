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
import org.apache.doris.catalog.Type;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class ColumnStatistic {

    public static final StatsType NDV = StatsType.NDV;
    public static final StatsType AVG_SIZE = StatsType.AVG_SIZE;
    public static final StatsType MAX_SIZE = StatsType.MAX_SIZE;
    public static final StatsType NUM_NULLS = StatsType.NUM_NULLS;
    public static final StatsType MIN_VALUE = StatsType.MIN_VALUE;
    public static final StatsType MAX_VALUE = StatsType.MAX_VALUE;

    private static final Logger LOG = LogManager.getLogger(ColumnStatistic.class);

    public static ColumnStatistic DEFAULT = new ColumnStatisticBuilder().setAvgSizeByte(1).setNdv(1)
            .setNumNulls(1).setCount(1).setMaxValue(Double.MAX_VALUE).setMinValue(Double.MIN_VALUE)
            .setSelectivity(1.0).setIsUnknown(true)
            .build();

    public static final Set<Type> MAX_MIN_UNSUPPORTED_TYPE = new HashSet<>();

    static {
        MAX_MIN_UNSUPPORTED_TYPE.add(Type.HLL);
        MAX_MIN_UNSUPPORTED_TYPE.add(Type.BITMAP);
        MAX_MIN_UNSUPPORTED_TYPE.add(Type.ARRAY);
        MAX_MIN_UNSUPPORTED_TYPE.add(Type.STRUCT);
        MAX_MIN_UNSUPPORTED_TYPE.add(Type.MAP);
    }

    public final double count;
    public final double ndv;
    public final double numNulls;
    public final double dataSize;
    public final double avgSizeByte;
    public final double minValue;
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

    // For display only.
    public final LiteralExpr minExpr;
    public final LiteralExpr maxExpr;

    public ColumnStatistic(double count, double ndv, double avgSizeByte,
            double numNulls, double dataSize, double minValue, double maxValue,
            double selectivity, LiteralExpr minExpr, LiteralExpr maxExpr, boolean isUnKnown) {
        this.count = count;
        this.ndv = ndv;
        this.avgSizeByte = avgSizeByte;
        this.numNulls = numNulls;
        this.dataSize = dataSize;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.selectivity = selectivity;
        this.minExpr = minExpr;
        this.maxExpr = maxExpr;
        this.isUnKnown = isUnKnown;
    }

    // TODO: use thrift
    public static ColumnStatistic fromResultRow(ResultRow resultRow) {
        try {
            ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder();
            double count = Double.parseDouble(resultRow.getColumnValue("count"));
            columnStatisticBuilder.setCount(count);
            double ndv = Double.parseDouble(resultRow.getColumnValue("ndv"));
            if (0.99 * count < ndv && ndv < 1.01 * count) {
                ndv = count;
            }
            columnStatisticBuilder.setNdv(ndv);
            columnStatisticBuilder.setNumNulls(Double.parseDouble(resultRow.getColumnValue("null_count")));
            columnStatisticBuilder.setDataSize(Double
                    .parseDouble(resultRow.getColumnValue("data_size_in_bytes")));
            columnStatisticBuilder.setAvgSizeByte(columnStatisticBuilder.getDataSize()
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
                return ColumnStatistic.DEFAULT;
            }
            String min = resultRow.getColumnValue("min");
            String max = resultRow.getColumnValue("max");
            columnStatisticBuilder.setMinValue(StatisticsUtil.convertToDouble(col.getType(), min));
            columnStatisticBuilder.setMaxValue(StatisticsUtil.convertToDouble(col.getType(), max));
            columnStatisticBuilder.setMaxExpr(StatisticsUtil.readableValue(col.getType(), max));
            columnStatisticBuilder.setMinExpr(StatisticsUtil.readableValue(col.getType(), min));
            columnStatisticBuilder.setSelectivity(1.0);
            return columnStatisticBuilder.build();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("Failed to deserialize column statistics, column not exists", e);
            return ColumnStatistic.DEFAULT;
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
            return DEFAULT;
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

    /**
     * the percentage of intersection range to this range
     * @param other
     * @return
     */
    public double coverage(ColumnStatistic other) {
        if (isUnKnown) {
            return 1.0;
        }
        if (minValue == maxValue) {
            if (other.minValue <= minValue && minValue <= other.maxValue) {
                return 1.0;
            } else {
                return 0.0;
            }
        } else {
            double myRange = maxValue - minValue;
            double interSection = Math.min(maxValue, other.maxValue) - Math.max(minValue, other.minValue);
            return interSection / myRange;
        }
    }

    @Override
    public String toString() {
        return isUnKnown ? "unKnown" : String.format("ndv=%.4f, min=%f, max=%f, sel=%f, count=%.4f",
                ndv, minValue, maxValue, selectivity, count);
    }
}

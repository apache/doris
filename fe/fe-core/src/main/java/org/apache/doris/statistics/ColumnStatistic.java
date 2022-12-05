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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class ColumnStatistic {

    private static final Logger LOG = LogManager.getLogger(StmtExecutor.class);

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
            double selectivity, LiteralExpr minExpr,
            LiteralExpr maxExpr, boolean isNaN) {
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
        this.isUnKnown = isNaN;
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
            long dbID = Long.parseLong(resultRow.getColumnValue("db_id"));
            long tblId = Long.parseLong(resultRow.getColumnValue("tbl_id"));
            String colName = resultRow.getColumnValue("col_id");
            Column col = findColumn(catalogId, dbID, tblId, colName);
            if (col == null) {
                LOG.warn("Failed to deserialize column statistics, column:{}.{}.{}.{} not exists",
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

    public ColumnStatistic copy() {
        return new ColumnStatisticBuilder().setCount(count).setNdv(ndv).setAvgSizeByte(avgSizeByte)
                .setNumNulls(numNulls).setDataSize(dataSize).setMinValue(minValue)
                .setMaxValue(maxValue).setMinExpr(minExpr).setMaxExpr(maxExpr)
                .setSelectivity(selectivity).setIsUnknown(isUnKnown).build();
    }

    public ColumnStatistic multiply(double d) {
        return new ColumnStatisticBuilder()
                .setCount(Math.ceil(count * d))
                .setNdv(Math.ceil(ndv * d))
                .setAvgSizeByte(Math.ceil(avgSizeByte * d))
                .setNumNulls(Math.ceil(numNulls * d))
                .setDataSize(Math.ceil(dataSize * d))
                .setMinValue(minValue)
                .setMaxValue(maxValue)
                .setMinExpr(minExpr)
                .setMaxExpr(maxExpr)
                .setSelectivity(selectivity)
                .setIsUnknown(isUnKnown)
                .build();
    }

    public boolean hasIntersect(ColumnStatistic other) {
        return Math.max(this.minValue, other.minValue) <= Math.min(this.maxValue, other.maxValue);
    }

    public static Column findColumn(long catalogId, long dbId, long tblId, String columnName) {
        CatalogIf<DatabaseIf<TableIf>> catalogIf = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        if (catalogIf == null) {
            return null;
        }
        DatabaseIf<TableIf> db = catalogIf.getDb(dbId).orElse(null);
        if (db == null) {
            return null;
        }
        TableIf tblIf = db.getTable(tblId).orElse(null);
        if (tblIf == null) {
            return null;
        }
        return tblIf.getColumn(columnName);
    }

    public ColumnStatistic updateBySelectivity(double selectivity, double rowCount) {
        if (isUnKnown) {
            return DEFAULT;
        }
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder(this);
        Double rowsAfterFilter = rowCount * selectivity;
        if (ColumnStat.isAlmostUnique(ndv, rowCount)) {
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

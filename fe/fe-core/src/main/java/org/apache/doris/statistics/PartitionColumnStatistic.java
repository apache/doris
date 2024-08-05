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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Hll;
import org.apache.doris.statistics.util.Hll128;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Base64;
import java.util.List;
import java.util.StringJoiner;

public class PartitionColumnStatistic {

    private static final Logger LOG = LogManager.getLogger(PartitionColumnStatistic.class);

    public static PartitionColumnStatistic UNKNOWN = new PartitionColumnStatisticBuilder().setAvgSizeByte(1)
            .setNdv(new Hll128()).setNumNulls(1).setCount(1).setMaxValue(Double.POSITIVE_INFINITY)
            .setMinValue(Double.NEGATIVE_INFINITY)
            .setIsUnknown(true).setUpdatedTime("")
            .build();

    public static PartitionColumnStatistic ZERO = new PartitionColumnStatisticBuilder().setAvgSizeByte(0)
            .setNdv(new Hll128()).setNumNulls(0).setCount(0).setMaxValue(Double.NaN).setMinValue(Double.NaN)
            .build();

    public final double count;
    public final Hll128 ndv;
    public final double numNulls;
    public final double dataSize;
    public final double avgSizeByte;
    public final double minValue;
    public final double maxValue;
    public final boolean isUnKnown;
    public final LiteralExpr minExpr;
    public final LiteralExpr maxExpr;
    public final String updatedTime;

    public PartitionColumnStatistic(double count, Hll128 ndv, double avgSizeByte,
                           double numNulls, double dataSize, double minValue, double maxValue,
                           LiteralExpr minExpr, LiteralExpr maxExpr, boolean isUnKnown,
                           String updatedTime) {
        this.count = count;
        this.ndv = ndv;
        this.avgSizeByte = avgSizeByte;
        this.numNulls = numNulls;
        this.dataSize = dataSize;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.minExpr = minExpr;
        this.maxExpr = maxExpr;
        this.isUnKnown = isUnKnown;
        this.updatedTime = updatedTime;
    }

    public static PartitionColumnStatistic fromResultRow(List<ResultRow> resultRows) {
        if (resultRows == null || resultRows.isEmpty()) {
            return PartitionColumnStatistic.UNKNOWN;
        }
        // This should never happen. resultRows should be empty or contain only 1 result row.
        if (resultRows.size() > 1) {
            StringJoiner stringJoiner = new StringJoiner("][", "[", "]");
            for (ResultRow row : resultRows) {
                stringJoiner.add(row.toString());
            }
            LOG.warn("Partition stats has more than one row, please drop stats and analyze again. {}",
                    stringJoiner.toString());
            return PartitionColumnStatistic.UNKNOWN;
        }
        try {
            return fromResultRow(resultRows.get(0));
        } catch (Throwable t) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to deserialize column stats", t);
            }
            return PartitionColumnStatistic.UNKNOWN;
        }
    }

    public static PartitionColumnStatistic fromResultRow(ResultRow row) {
        // row : [catalog_id, db_id, tbl_id, idx_id, part_name, col_id,
        //        count, ndv, null_count, min, max, data_size, update_time]
        try {
            long catalogId = Long.parseLong(row.get(0));
            long dbID = Long.parseLong(row.get(1));
            long tblId = Long.parseLong(row.get(2));
            long idxId = Long.parseLong(row.get(3));
            String colName = row.get(5);
            Column col = StatisticsUtil.findColumn(catalogId, dbID, tblId, idxId, colName);
            if (col == null) {
                LOG.info("Failed to deserialize column statistics, ctlId: {} dbId: {}, "
                        + "tblId: {} column: {} not exists", catalogId, dbID, tblId, colName);
                return PartitionColumnStatistic.UNKNOWN;
            }

            PartitionColumnStatisticBuilder partitionStatisticBuilder = new PartitionColumnStatisticBuilder();
            double count = Double.parseDouble(row.get(6));
            partitionStatisticBuilder.setCount(count);
            String ndv = row.get(7);
            Base64.Decoder decoder = Base64.getDecoder();
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(decoder.decode(ndv)));
            Hll hll = new Hll();
            if (!hll.deserialize(dis)) {
                LOG.warn("Failed to deserialize ndv. [{}]", row);
                return PartitionColumnStatistic.UNKNOWN;
            }
            partitionStatisticBuilder.setNdv(Hll128.fromHll(hll));
            String nullCount = row.getWithDefault(8, "0");
            partitionStatisticBuilder.setNumNulls(Double.parseDouble(nullCount));
            partitionStatisticBuilder.setDataSize(Double
                    .parseDouble(row.getWithDefault(11, "0")));
            partitionStatisticBuilder.setAvgSizeByte(partitionStatisticBuilder.getCount() == 0
                    ? 0 : partitionStatisticBuilder.getDataSize()
                    / partitionStatisticBuilder.getCount());
            String min = row.get(9);
            String max = row.get(10);
            if (min != null && !"NULL".equalsIgnoreCase(min)) {
                try {
                    partitionStatisticBuilder.setMinValue(StatisticsUtil.convertToDouble(col.getType(), min));
                    partitionStatisticBuilder.setMinExpr(StatisticsUtil.readableValue(col.getType(), min));
                } catch (AnalysisException e) {
                    LOG.warn("Failed to deserialize column {} min value {}.", col, min, e);
                    partitionStatisticBuilder.setMinValue(Double.NEGATIVE_INFINITY);
                }
            } else {
                partitionStatisticBuilder.setMinValue(Double.NEGATIVE_INFINITY);
            }
            if (max != null && !"NULL".equalsIgnoreCase(max)) {
                try {
                    partitionStatisticBuilder.setMaxValue(StatisticsUtil.convertToDouble(col.getType(), max));
                    partitionStatisticBuilder.setMaxExpr(StatisticsUtil.readableValue(col.getType(), max));
                } catch (AnalysisException e) {
                    LOG.warn("Failed to deserialize column {} max value {}.", col, max, e);
                    partitionStatisticBuilder.setMaxValue(Double.POSITIVE_INFINITY);
                }
            } else {
                partitionStatisticBuilder.setMaxValue(Double.POSITIVE_INFINITY);
            }
            partitionStatisticBuilder.setUpdatedTime(row.get(12));
            return partitionStatisticBuilder.build();
        } catch (Exception e) {
            LOG.warn("Failed to deserialize column statistics. Row [{}]", row, e);
            return PartitionColumnStatistic.UNKNOWN;
        }
    }
}

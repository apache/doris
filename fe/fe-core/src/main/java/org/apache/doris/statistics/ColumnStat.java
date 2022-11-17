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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.Util;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * There are the statistics of column.
 * The column stats are mainly used to provide input for the Optimizer's cost model.
 * <p>
 * The description of column stats are following:
 * 1. @ndv: The number distinct values of column.
 * 2. @avgSize: The average size of column. The unit is bytes.
 * 3. @maxSize: The max size of column. The unit is bytes.
 * 4. @numNulls: The number of nulls.
 * 5. @minValue: The min value of column.
 * 6. @maxValue: The max value of column.
 * <p>
 * The granularity of the statistics is whole table.
 * For example:
 * "@ndv = 10" means that the number distinct values is 10 in the whole table.
 */
public class ColumnStat {

    public static final StatsType NDV = StatsType.NDV;
    public static final StatsType AVG_SIZE = StatsType.AVG_SIZE;
    public static final StatsType MAX_SIZE = StatsType.MAX_SIZE;
    public static final StatsType NUM_NULLS = StatsType.NUM_NULLS;
    public static final StatsType MIN_VALUE = StatsType.MIN_VALUE;
    public static final StatsType MAX_VALUE = StatsType.MAX_VALUE;

    public static final ColumnStat UNKNOWN = new ColumnStat();

    private static final Predicate<Double> DESIRED_NDV_PRED = (v) -> v >= -1L;
    private static final Predicate<Double> DESIRED_AVG_SIZE_PRED = (v) -> (v == -1) || (v >= 0);
    private static final Predicate<Double> DESIRED_MAX_SIZE_PRED = (v) -> v >= -1L;
    private static final Predicate<Double> DESIRED_NUM_NULLS_PRED = (v) -> v >= -1L;

    public static final Set<Type> MAX_MIN_UNSUPPORTED_TYPE = new HashSet<>();

    static {
        MAX_MIN_UNSUPPORTED_TYPE.add(Type.HLL);
        MAX_MIN_UNSUPPORTED_TYPE.add(Type.BITMAP);
        MAX_MIN_UNSUPPORTED_TYPE.add(Type.ARRAY);
        MAX_MIN_UNSUPPORTED_TYPE.add(Type.STRUCT);
        MAX_MIN_UNSUPPORTED_TYPE.add(Type.MAP);
    }

    private double ndv = -1;
    private double avgSizeByte = -1;
    private double maxSizeByte = -1;
    private double numNulls = -1;
    private double minValue = Double.NaN;
    private double maxValue = Double.NaN;
    // For display only.
    private LiteralExpr minExpr;
    private LiteralExpr maxExpr;

    private double selectivity = 1.0;

    public static ColumnStat createDefaultColumnStats() {
        ColumnStat columnStat = new ColumnStat();
        columnStat.setAvgSizeByte(1);
        columnStat.setMaxSizeByte(1);
        columnStat.setNdv(1);
        columnStat.setNumNulls(0);
        return columnStat;
    }

    public static boolean isUnKnown(ColumnStat stats) {
        return stats == UNKNOWN;
    }

    public ColumnStat() {
    }

    public ColumnStat(ColumnStat other) {
        this.ndv = other.ndv;
        this.avgSizeByte = other.avgSizeByte;
        this.maxSizeByte = other.maxSizeByte;
        this.numNulls = other.numNulls;
        this.minValue = other.minValue;
        this.maxValue = other.maxValue;
        this.selectivity = other.selectivity;
    }

    public ColumnStat(double ndv, double avgSizeByte,
            double maxSizeByte, double numNulls, double minValue, double maxValue) {
        this.ndv = ndv;
        this.avgSizeByte = avgSizeByte;
        this.maxSizeByte = maxSizeByte;
        this.numNulls = numNulls;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    public double getNdv() {
        return ndv;
    }

    public double getAvgSizeByte() {
        return avgSizeByte;
    }

    public double getMaxSizeByte() {
        return maxSizeByte;
    }

    public double getNumNulls() {
        return numNulls;
    }

    public double getMinValue() {
        return minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public void setNdv(double ndv) {
        this.ndv = ndv;
    }

    public void setAvgSizeByte(double avgSizeByte) {
        this.avgSizeByte = avgSizeByte;
    }

    public void setMaxSizeByte(double maxSizeByte) {
        this.maxSizeByte = maxSizeByte;
    }

    public void setNumNulls(double numNulls) {
        this.numNulls = numNulls;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }

    public void updateStats(Type columnType, Map<StatsType, String> statsTypeToValue) throws AnalysisException {
        for (Map.Entry<StatsType, String> entry : statsTypeToValue.entrySet()) {
            StatsType statsType = entry.getKey();
            switch (statsType) {
                case NDV:
                    ndv = Util.getDoublePropertyOrDefault(entry.getValue(), ndv,
                        DESIRED_NDV_PRED, NDV + " should >= -1");
                    break;
                case AVG_SIZE:
                    avgSizeByte = Util.getDoublePropertyOrDefault(entry.getValue(), avgSizeByte,
                        DESIRED_AVG_SIZE_PRED, AVG_SIZE + " should (>=0) or (=-1)");
                    break;
                case MAX_SIZE:
                    maxSizeByte = Util.getDoublePropertyOrDefault(entry.getValue(), maxSizeByte,
                        DESIRED_MAX_SIZE_PRED, MAX_SIZE + " should >=-1");
                    break;
                case NUM_NULLS:
                    numNulls = Util.getDoublePropertyOrDefault(entry.getValue(), numNulls,
                        DESIRED_NUM_NULLS_PRED, NUM_NULLS + " should >=-1");
                    break;
                case MIN_VALUE:
                    if (MAX_MIN_UNSUPPORTED_TYPE.contains(statsType)) {
                        minValue = Double.NEGATIVE_INFINITY;
                    } else {
                        minExpr = StatisticsUtil.readableValue(columnType, entry.getValue());
                        minValue = StatisticsUtil.convertToDouble(columnType, entry.getValue());
                    }
                    break;
                case MAX_VALUE:
                    if (MAX_MIN_UNSUPPORTED_TYPE.contains(statsType)) {
                        maxValue = Double.NEGATIVE_INFINITY;
                    } else {
                        maxExpr = StatisticsUtil.readableValue(columnType, entry.getValue());
                        maxValue = StatisticsUtil.convertToDouble(columnType, entry.getValue());
                    }
                    break;
                default:
                    throw new AnalysisException("Unknown stats type: " + statsType);
            }
        }
    }

    public List<String> getShowInfo() {
        List<String> result = Lists.newArrayList();
        result.add(Double.toString(ndv));
        result.add(Double.toString(avgSizeByte));
        result.add(Double.toString(maxSizeByte));
        result.add(Double.toString(numNulls));
        result.add(Double.toString(minValue));
        result.add(Double.toString(maxValue));
        return result;
    }

    public ColumnStat copy() {
        return new ColumnStat(this);
    }


    public boolean hasIntersect(ColumnStat another) {
        double leftMin = this.getMinValue();
        double rightMin = another.getMinValue();
        double leftMax = this.getMaxValue();
        double rightMax = another.getMaxValue();
        return Math.max(leftMin, rightMin) <= Math.min(leftMax, rightMax);
    }

    /**
     * Return default column statistic.
     */
    public static ColumnStat getDefaultColumnStats() {
        return new ColumnStat();
    }

    /**
     * Merge column statistics(the original statistics should not be modified)
     *
     * @param left statistics to be merged
     * @param right statistics to be merged
     */
    public static ColumnStat mergeColumnStats(ColumnStat left, ColumnStat right) {
        // merge ndv
        double leftNdv = left.getNdv();
        double rightNdv = right.getNdv();

        if (leftNdv == -1) {
            leftNdv = rightNdv;
        } else {
            leftNdv = rightNdv != -1 ? (leftNdv + rightNdv) : leftNdv;
        }

        double leftAvgSize = left.getAvgSizeByte();
        double rightAvgSize = right.getAvgSizeByte();
        if (leftAvgSize == -1) {
            leftAvgSize = rightAvgSize;
        } else {
            leftAvgSize = rightAvgSize != -1 ? ((leftAvgSize + rightAvgSize) / 2) : leftAvgSize;
        }

        // merge max_size
        double leftMaxSize = left.getMaxSizeByte();
        double rightMaxSize = right.getMaxSizeByte();
        if (leftMaxSize == -1) {
            leftMaxSize = rightMaxSize;
        } else {
            leftMaxSize = Math.max(leftMaxSize, rightMaxSize);
        }

        // merge num_nulls
        double leftNumNulls = left.getNumNulls();
        double rightNumNulls = right.getNumNulls();
        if (leftNumNulls == -1) {
            leftNumNulls = rightNumNulls;
        } else {
            leftNumNulls = rightNumNulls != -1 ? (leftNumNulls + rightNumNulls) : leftNumNulls;
        }

        // merge min_value
        double leftMinValue = left.getMinValue();
        double rightMinValue = right.getMinValue();
        leftMinValue = Math.min(leftMinValue, rightMinValue);

        // merge max_value
        double leftMaxValue = left.getMaxValue();
        double rightMaxValue = right.getMaxValue();
        leftMaxValue = Math.max(rightMaxValue, leftMaxValue);

        // generate the new merged-statistics
        return new ColumnStat(leftNdv, leftAvgSize, leftMaxSize, leftNumNulls, leftMinValue, leftMaxValue);
    }

    public static boolean isAlmostUnique(double ndv, double rowCount) {
        return rowCount * 0.9 < ndv && ndv < rowCount * 1.1;
    }

    public double getSelectivity() {
        return selectivity;
    }

    public void setSelectivity(double selectivity) {
        this.selectivity = selectivity;
    }

    public double ndvIntersection(ColumnStat other) {
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
}

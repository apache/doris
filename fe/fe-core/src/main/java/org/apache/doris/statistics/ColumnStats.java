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

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * There are the statistics of column.
 * The column stats are mainly used to provide input for the Optimizer's cost model.
 * <p>
 * The description of column stats are following:
 *   1. @ndv: The number distinct values of column.
 *   2. @avgSize: The average size of column. The unit is bytes.
 *   3. @maxSize: The max size of column. The unit is bytes.
 *   4. @numNulls: The number of nulls.
 *   5. @minValue: The min value of column.
 *   6. @maxValue: The max value of column.
 * <p>
 * The granularity of the statistics is whole table.
 * For example:
 * "@ndv = 10" means that the number distinct values is 10 in the whole table or partition.
 * <p>
 * After the statistics task is successfully completed, update the ColumnStats,
 * ColumnStats should not be updated in any other way.
 */
public class ColumnStats {
    public static final StatsType NDV = StatsType.NDV;
    public static final StatsType AVG_SIZE = StatsType.AVG_SIZE;
    public static final StatsType MAX_SIZE = StatsType.MAX_SIZE;
    public static final StatsType NUM_NULLS = StatsType.NUM_NULLS;
    public static final StatsType MIN_VALUE = StatsType.MIN_VALUE;
    public static final StatsType MAX_VALUE = StatsType.MAX_VALUE;

    private static final Predicate<Long> DESIRED_NDV_PRED = (v) -> v >= -1L;
    private static final Predicate<Float> DESIRED_AVG_SIZE_PRED = (v) -> (v == -1) || (v >= 0);
    private static final Predicate<Long> DESIRED_MAX_SIZE_PRED = (v) -> v >= -1L;
    private static final Predicate<Long> DESIRED_NUM_NULLS_PRED = (v) -> v >= -1L;

    private long ndv = -1;
    private float avgSize = -1;  // in bytes
    private long maxSize = -1;  // in bytes
    private long numNulls = -1;

    private LiteralExpr minValue = new NullLiteral();
    private LiteralExpr maxValue = new NullLiteral();

    /**
     * Return default column statistic.
     */
    public static ColumnStats getDefaultColumnStats() {
        return new ColumnStats();
    }

    /**
     * Merge column statistics(the original statistics should not be modified)
     *
     * @param left statistics to be merged
     * @param right statistics to be merged
     */
    public static ColumnStats aggColumnStats(ColumnStats left, ColumnStats right) {
        // merge ndv
        long leftNdv = left.getNdv();
        long rightNdv = right.getNdv();

        if (leftNdv == -1) {
            leftNdv = rightNdv;
        } else {
            leftNdv = rightNdv != -1 ? (leftNdv + rightNdv) : leftNdv;
        }

        // merge avg_size
        float leftAvgSize = left.getAvgSize();
        float rightAvgSize = right.getAvgSize();
        if (leftAvgSize == -1) {
            leftAvgSize = rightAvgSize;
        } else {
            leftAvgSize = rightAvgSize != -1 ? ((leftAvgSize + rightAvgSize) / 2) : leftAvgSize;
        }

        // merge max_size
        long leftMaxSize = left.getMaxSize();
        long rightMaxSize = right.getMaxSize();
        if (leftMaxSize == -1) {
            leftMaxSize = rightMaxSize;
        } else {
            leftMaxSize = rightMaxSize != -1 ? (Math.max(leftMaxSize, rightMaxSize)) : leftMaxSize;
        }

        // merge num_nulls
        long leftNumNulls = left.getNumNulls();
        long rightNumNulls = right.getNumNulls();
        if (leftNumNulls == -1) {
            leftNumNulls = rightNumNulls;
        } else {
            leftNumNulls = rightNumNulls != -1 ? (leftNumNulls + rightNumNulls) : leftNumNulls;
        }

        // merge min_value
        LiteralExpr leftMinValue = left.getMinValue();
        LiteralExpr rightMinValue = right.getMinValue();
        if (leftMinValue == null) {
            leftMinValue = rightMinValue;
        } else {
            leftMinValue = leftMinValue.compareTo(rightMinValue) > 0 ? rightMinValue : leftMinValue;
        }

        // merge max_value
        LiteralExpr leftMaxValue = left.getMaxValue();
        LiteralExpr rightMaxValue = right.getMaxValue();
        if (leftMaxValue == null) {
            leftMaxValue = rightMaxValue;
        } else {
            leftMaxValue = leftMaxValue.compareTo(rightMaxValue) < 0 ? rightMaxValue : leftMaxValue;
        }

        // generate the new merged-statistics
        return new ColumnStats(leftNdv, leftAvgSize, leftMaxSize, leftNumNulls, leftMinValue, leftMaxValue);
    }

    public ColumnStats() {
    }

    public ColumnStats(long ndv, float avgSize, long maxSize,
            long numNulls, LiteralExpr minValue, LiteralExpr maxValue) {
        this.ndv = ndv;
        this.avgSize = avgSize;
        this.maxSize = maxSize;
        this.numNulls = numNulls;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    public ColumnStats(ColumnStats other) {
        this.ndv = other.ndv;
        this.avgSize = other.avgSize;
        this.maxSize = other.maxSize;
        this.numNulls = other.numNulls;
        if (other.minValue != null) {
            this.minValue = (LiteralExpr) other.minValue.clone();
        }
        if (other.maxValue != null) {
            this.maxValue = (LiteralExpr) other.maxValue.clone();
        }
    }

    public long getNdv() {
        return ndv;
    }

    public float getAvgSize() {
        return avgSize;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public long getNumNulls() {
        return numNulls;
    }

    public LiteralExpr getMinValue() {
        return minValue;
    }

    public LiteralExpr getMaxValue() {
        return maxValue;
    }

    public List<String> getShowInfo() {
        List<String> result = Lists.newArrayList();
        result.add(Long.toString(ndv));
        result.add(Float.toString(avgSize));
        result.add(Long.toString(maxSize));
        result.add(Long.toString(numNulls));
        if (minValue != null) {
            result.add(minValue.getStringValue());
        } else {
            result.add("N/A");
        }
        if (maxValue != null) {
            result.add(maxValue.getStringValue());
        } else {
            result.add("N/A");
        }
        return result;
    }

    public ColumnStats copy() {
        return new ColumnStats(this);
    }

    /**
     * After the statistics task is successfully completed, update the statistics of the column,
     * statistics should not be updated in any other way.
     */
    public void updateStats(Type columnType, Map<StatsType, String> statsTypeToValue)
            throws AnalysisException {
        for (Map.Entry<StatsType, String> entry : statsTypeToValue.entrySet()) {
            StatsType statsType = entry.getKey();
            switch (statsType) {
                case NDV:
                    ndv = Util.getLongPropertyOrDefault(entry.getValue(), ndv,
                        DESIRED_NDV_PRED, NDV + " should >= -1");
                    break;
                case AVG_SIZE:
                    avgSize = Util.getFloatPropertyOrDefault(entry.getValue(), avgSize,
                        DESIRED_AVG_SIZE_PRED, AVG_SIZE + " should (>=0) or (=-1)");
                    break;
                case MAX_SIZE:
                    maxSize = Util.getLongPropertyOrDefault(entry.getValue(), maxSize,
                        DESIRED_MAX_SIZE_PRED, MAX_SIZE + " should >=-1");
                    break;
                case NUM_NULLS:
                    numNulls = Util.getLongPropertyOrDefault(entry.getValue(), numNulls,
                        DESIRED_NUM_NULLS_PRED, NUM_NULLS + " should >=-1");
                    break;
                case MIN_VALUE:
                    minValue = validateColumnValue(columnType, entry.getValue());
                    break;
                case MAX_VALUE:
                    maxValue = validateColumnValue(columnType, entry.getValue());
                    break;
                default:
                    throw new AnalysisException("Unknown stats type: " + statsType);
            }
        }
    }

    // TODO: The generated statistics should not be modified
    public void updateBySelectivity(double selectivity) {
        ndv = (long) Math.ceil(ndv * selectivity);
        numNulls = (long) Math.ceil(numNulls * selectivity);
    }

    private LiteralExpr validateColumnValue(Type type, String columnValue) throws AnalysisException {
        Preconditions.checkArgument(type.isScalarType());
        ScalarType scalarType = (ScalarType) type;

        // check if default value is valid.
        // if not, some literal constructor will throw AnalysisException
        PrimitiveType primitiveType = scalarType.getPrimitiveType();
        switch (primitiveType) {
            case BOOLEAN:
                return new BoolLiteral(columnValue);
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return new IntLiteral(columnValue, type);
            case LARGEINT:
                return new LargeIntLiteral(columnValue);
            case FLOAT:
                // the min max value will loose precision when value type is double.
            case DOUBLE:
                return new FloatLiteral(columnValue);
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                DecimalLiteral decimalLiteral = new DecimalLiteral(columnValue);
                decimalLiteral.checkPrecisionAndScale(scalarType.getScalarPrecision(), scalarType.getScalarScale());
                return decimalLiteral;
            case DATE:
            case DATETIME:
            case DATEV2:
            case DATETIMEV2:
                return new DateLiteral(columnValue, type);
            case CHAR:
            case VARCHAR:
                if (columnValue.length() > scalarType.getLength()) {
                    throw new AnalysisException("Min/Max value is longer than length of column type: "
                            + columnValue);
                }
                return new StringLiteral(columnValue);
            case HLL:
            case BITMAP:
            case ARRAY:
            case MAP:
            case STRUCT:
            default:
                throw new AnalysisException("Unsupported setting this type: " + type + " of min max value");
        }
    }
}

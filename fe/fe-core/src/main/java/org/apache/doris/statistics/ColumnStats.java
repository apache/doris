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
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.Util;

import com.google.common.base.Preconditions;

import java.util.Map;
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
public class ColumnStats {

    public static final String NDV = "ndv";
    public static final String AVG_SIZE = "avg_size";
    public static final String MAX_SIZE = "max_size";
    public static final String NUM_NULLS = "num_nulls";
    public static final String MIN_VALUE = "min_value";
    public static final String MAX_VALUE = "max_value";

    private static final Predicate<Long> DESIRED_NDV_PRED = (v) -> v >= -1L;
    private static final Predicate<Float> DESIRED_AVG_SIZE_PRED = (v) -> (v == -1) || (v >= 0);
    private static final Predicate<Long> DESIRED_MAX_SIZE_PRED = (v) -> v >= -1L;
    private static final Predicate<Long> DESIRED_NUM_NULLS_PRED = (v) -> v >= -1L;

    private long ndv = -1;
    private float avgSize = -1;  // in bytes
    private long maxSize = -1;  // in bytes
    private long numNulls = -1;
    private LiteralExpr minValue;
    private LiteralExpr maxValue;

    public void updateStats(Type columnType, Map<String, String> statsNameToValue) throws AnalysisException {
        for (Map.Entry<String, String> entry : statsNameToValue.entrySet()) {
            String statsName = entry.getKey();
            if (statsName.equalsIgnoreCase(NDV)) {
                ndv = Util.getLongPropertyOrDefault(entry.getValue(), ndv,
                        DESIRED_NDV_PRED, NDV + " should >= -1");
            } else if (statsName.equalsIgnoreCase(AVG_SIZE)) {
                avgSize = Util.getFloatPropertyOrDefault(entry.getValue(), avgSize,
                        DESIRED_AVG_SIZE_PRED, AVG_SIZE + " should (>=0) or (=-1)");
            } else if (statsName.equalsIgnoreCase(MAX_SIZE)) {
                maxSize = Util.getLongPropertyOrDefault(entry.getValue(), maxSize,
                        DESIRED_MAX_SIZE_PRED, MAX_SIZE + " should >=-1");
            } else if (statsName.equalsIgnoreCase(NUM_NULLS)) {
                numNulls = Util.getLongPropertyOrDefault(entry.getValue(), numNulls,
                        DESIRED_NUM_NULLS_PRED, NUM_NULLS + " should >=-1");
            } else if (statsName.equalsIgnoreCase(MIN_VALUE)) {
                minValue = validateColumnValue(columnType, entry.getValue());
            } else if (statsName.equalsIgnoreCase(MAX_VALUE)) {
                maxValue = validateColumnValue(columnType, entry.getValue());
            }
        }
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
                DecimalLiteral decimalLiteral = new DecimalLiteral(columnValue);
                decimalLiteral.checkPrecisionAndScale(scalarType.getScalarPrecision(), scalarType.getScalarScale());
                return decimalLiteral;
            case DATE:
            case DATETIME:
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

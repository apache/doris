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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ColumnDef.java
// and modified by Doris

package org.apache.doris.nereids.analyzer;

import org.apache.doris.analysis.DefaultValueExprDef;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.types.DataType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class ColumnDefinition {
    private static final Logger LOG = LogManager.getLogger(ColumnDefinition.class);

    public static class DefaultValue {
        public boolean isSet;
        public String value;
        // used for column which defaultValue is an expression.
        public DefaultValueExprDef defaultValueExprDef;

        public DefaultValue(boolean isSet, String value) {
            this.isSet = isSet;
            this.value = value;
            this.defaultValueExprDef = null;
        }

        /**
         * used for column which defaultValue is an expression.
         *
         * @param isSet is Set DefaultValue
         * @param value default value
         * @param exprName default value expression
         */
        public DefaultValue(boolean isSet, String value, String exprName) {
            this.isSet = isSet;
            this.value = value;
            this.defaultValueExprDef = new DefaultValueExprDef(exprName);
        }

        public DefaultValue(boolean isSet, String value, String exprName, Long precision) {
            this.isSet = isSet;
            this.value = value;
            this.defaultValueExprDef = new DefaultValueExprDef(exprName, precision);
        }

        // default "CURRENT_TIMESTAMP", only for DATETIME type
        public static String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
        public static String NOW = "now";
        public static DefaultValue CURRENT_TIMESTAMP_DEFAULT_VALUE = new DefaultValue(true, CURRENT_TIMESTAMP, NOW);
        // no default value
        public static DefaultValue NOT_SET = new DefaultValue(false, null);
        // default null
        public static DefaultValue NULL_DEFAULT_VALUE = new DefaultValue(true, null);
        public static String ZERO = new String(new byte[] {0});
        // default "value", "0" means empty hll
        public static DefaultValue HLL_EMPTY_DEFAULT_VALUE = new DefaultValue(true, ZERO);
        // default "value", "0" means empty bitmap
        public static DefaultValue BITMAP_EMPTY_DEFAULT_VALUE = new DefaultValue(true, ZERO);
        // default "value", "[]" means empty array
        public static DefaultValue ARRAY_EMPTY_DEFAULT_VALUE = new DefaultValue(true, "[]");

        public static DefaultValue currentTimeStampDefaultValueWithPrecision(Long precision) {
            if (precision > ScalarType.MAX_DATETIMEV2_SCALE || precision < 0) {
                throw new IllegalArgumentException("column's default value current_timestamp"
                        + " precision must be between 0 and 6");
            }
            if (precision == 0) {
                return new DefaultValue(true, CURRENT_TIMESTAMP, NOW);
            }
            String value = CURRENT_TIMESTAMP + "(" + precision + ")";
            String exprName = NOW;
            return new DefaultValue(true, value, exprName, precision);
        }

        public boolean isCurrentTimeStamp() {
            return "CURRENT_TIMESTAMP".equals(value) && NOW.equals(defaultValueExprDef.getExprName());
        }

        public boolean isCurrentTimeStampWithPrecision() {
            return defaultValueExprDef != null && value.startsWith(CURRENT_TIMESTAMP + "(")
                    && NOW.equals(defaultValueExprDef.getExprName());
        }

        public long getCurrentTimeStampPrecision() {
            if (isCurrentTimeStampWithPrecision()) {
                return Long.parseLong(value.substring(CURRENT_TIMESTAMP.length() + 1, value.length() - 1));
            }
            return 0;
        }

        public String getValue() {
            if (isCurrentTimeStamp()) {
                return LocalDateTime.now(TimeUtils.getTimeZone().toZoneId()).toString().replace('T', ' ');
            } else if (isCurrentTimeStampWithPrecision()) {
                long precision = getCurrentTimeStampPrecision();
                String format = "yyyy-MM-dd HH:mm:ss";
                if (precision == 0) {
                    return LocalDateTime.now(TimeUtils.getTimeZone().toZoneId()).toString().replace('T', ' ');
                } else if (precision == 1) {
                    format = "yyyy-MM-dd HH:mm:ss.S";
                } else if (precision == 2) {
                    format = "yyyy-MM-dd HH:mm:ss.SS";
                } else if (precision == 3) {
                    format = "yyyy-MM-dd HH:mm:ss.SSS";
                } else if (precision == 4) {
                    format = "yyyy-MM-dd HH:mm:ss.SSSS";
                } else if (precision == 5) {
                    format = "yyyy-MM-dd HH:mm:ss.SSSSS";
                } else if (precision == 6) {
                    format = "yyyy-MM-dd HH:mm:ss.SSSSSS";
                }
                return LocalDateTime.now(TimeUtils.getTimeZone().toZoneId())
                        .format(DateTimeFormatter.ofPattern(format));
            }
            return value;
        }
    }

    // parameter initialized in constructor
    private String name;
    private DataType dataType;
    private AggregateType aggregateType;
    private String genericAggregationName;
    private List<TypeDef> genericAggregationArguments;

    private boolean isKey;
    private boolean isAllowNull;
    private boolean isAutoInc;
    private DefaultValue defaultValue;
    private String comment;
    private boolean visible;

    public ColumnDefinition(String name, DataType dataType) {
        this(name, dataType, false, null, false, false, DefaultValue.NOT_SET, "");
    }

    public ColumnDefinition(String name, DataType dataType, boolean isKey, AggregateType aggregateType,
            boolean isAllowNull, boolean isAutoInc, DefaultValue defaultValue, String comment) {
        this(name, dataType, isKey, aggregateType, isAllowNull, isAutoInc, defaultValue, comment, true);
    }

    public ColumnDefinition(String name, DataType dataType, boolean isAllowNull) {
        this(name, dataType, false, null, isAllowNull, DefaultValue.NOT_SET, "");
    }

    public ColumnDefinition(String name, DataType dataType, boolean isKey, AggregateType aggregateType,
            boolean isAllowNull, DefaultValue defaultValue, String comment) {
        this(name, dataType, isKey, aggregateType, isAllowNull, false, defaultValue, comment, true);
    }

    public ColumnDefinition(String name, DataType dataType, boolean isKey, AggregateType aggregateType,
            boolean isAllowNull, boolean isAutoInc, DefaultValue defaultValue, String comment, boolean visible) {
        this.name = name;
        this.dataType = dataType;
        this.isKey = isKey;
        this.aggregateType = aggregateType;
        this.isAllowNull = isAllowNull;
        this.isAutoInc = isAutoInc;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.visible = visible;
    }



    public boolean isAllowNull() {
        return isAllowNull;
    }

    public String getDefaultValue() {
        return defaultValue.value;
    }

    public String getName() {
        return name;
    }

    public AggregateType getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(AggregateType aggregateType) {
        this.aggregateType = aggregateType;
    }

    public void setGenericAggregationName(String genericAggregationName) {
        this.genericAggregationName = genericAggregationName;
    }

    public void setGenericAggregationName(AggregateType aggregateType) {
        this.genericAggregationName = aggregateType.name().toLowerCase();
    }

    public void setGenericAggregationArguments(List<TypeDef> genericAggregationArguments) {
        this.genericAggregationArguments = genericAggregationArguments;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setIsKey(boolean isKey) {
        this.isKey = isKey;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public String getComment() {
        return comment;
    }

    public boolean isVisible() {
        return visible;
    }

    public void analyze(boolean isOlap) throws AnalysisException {

    }

    @SuppressWarnings("checkstyle:Indentation")
    public static void validateDefaultValue(Type type, String defaultValue, DefaultValueExprDef defaultValueExprDef)
            throws AnalysisException {

    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();

        return sb.toString();
    }

    public Column toColumn() {
        return new Column(name, Type.INT, isKey, aggregateType, isAllowNull, isAutoInc, defaultValue.value, comment,
                visible, defaultValue.defaultValueExprDef, Column.COLUMN_UNIQUE_ID_INIT_VALUE, defaultValue.getValue());
    }

    @Override
    public String toString() {
        return toSql();
    }

    public void setAllowNull(boolean allowNull) {
        isAllowNull = allowNull;
    }
}

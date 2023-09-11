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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.DefaultValueExprDef;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.util.TimeUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * default value of a column.
 */
public class DefaultValue {
    public static String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
    public static String NOW = "now";
    public static DefaultValue CURRENT_TIMESTAMP_DEFAULT_VALUE = new DefaultValue(CURRENT_TIMESTAMP, NOW);
    // default null
    public static DefaultValue NULL_DEFAULT_VALUE = new DefaultValue(null);
    public static String ZERO = new String(new byte[] {0});
    public static String ZERO_NUMBER = "0";
    // default "value", "0" means empty hll
    public static DefaultValue HLL_EMPTY_DEFAULT_VALUE = new DefaultValue(ZERO);
    // default "value", "0" means empty bitmap
    public static DefaultValue BITMAP_EMPTY_DEFAULT_VALUE = new DefaultValue(ZERO);
    // default "value", "[]" means empty array
    public static DefaultValue ARRAY_EMPTY_DEFAULT_VALUE = new DefaultValue("[]");

    private final String value;
    // used for column which defaultValue is an expression.
    private final DefaultValueExprDef defaultValueExprDef;

    public DefaultValue(String value) {
        this.value = value;
        this.defaultValueExprDef = null;
    }

    /**
     * used for column which defaultValue is an expression.
     * @param value default value
     * @param exprName default value expression
     */
    public DefaultValue(String value, String exprName) {
        this.value = value;
        this.defaultValueExprDef = new DefaultValueExprDef(exprName);
    }

    public DefaultValue(String value, String exprName, Long precision) {
        this.value = value;
        this.defaultValueExprDef = new DefaultValueExprDef(exprName, precision);
    }

    /**
     * default value current_timestamp(precision)
     */
    public static DefaultValue currentTimeStampDefaultValueWithPrecision(Long precision) {
        if (precision > ScalarType.MAX_DATETIMEV2_SCALE || precision < 0) {
            throw new IllegalArgumentException("column's default value current_timestamp"
                    + " precision must be between 0 and 6");
        }
        if (precision == 0) {
            return new DefaultValue(CURRENT_TIMESTAMP, NOW);
        }
        String value = CURRENT_TIMESTAMP + "(" + precision + ")";
        String exprName = NOW;
        return new DefaultValue(value, exprName, precision);
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

    public String getRawValue() {
        return value;
    }

    /**
     * get string value of a default value expression.
     */
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

    public DefaultValueExprDef getDefaultValueExprDef() {
        return defaultValueExprDef;
    }
}

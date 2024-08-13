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

/**
 * default value of a column.
 */
public class DefaultValue {
    public static String PI = "PI";
    public static String CURRENT_DATE = "CURRENT_DATE";
    public static String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
    public static String NOW = "now";
    public static String HLL_EMPTY = "HLL_EMPTY";
    public static DefaultValue CURRENT_DATE_DEFAULT_VALUE = new DefaultValue(CURRENT_DATE, CURRENT_DATE.toLowerCase());
    public static DefaultValue CURRENT_TIMESTAMP_DEFAULT_VALUE = new DefaultValue(CURRENT_TIMESTAMP, NOW);
    // default null
    public static DefaultValue NULL_DEFAULT_VALUE = new DefaultValue(null);
    public static String ZERO = new String(new byte[] {0});
    public static String ZERO_NUMBER = "0";
    // default "value", "0" means empty hll
    public static DefaultValue HLL_EMPTY_DEFAULT_VALUE = new DefaultValue(ZERO, HLL_EMPTY);
    // default "value", "0" means empty bitmap
    public static DefaultValue BITMAP_EMPTY_DEFAULT_VALUE = new DefaultValue(ZERO);
    // default "value", "[]" means empty array
    public static DefaultValue ARRAY_EMPTY_DEFAULT_VALUE = new DefaultValue("[]");
    // default "value", "3.14159265358979323846" means pi, refer to M_PI in <math.h>.
    // M_PI is enough for type double because the precision of type double is 15~16 digits.
    // but it is not adequate for computation using long double. in this case, use M_PIl in <math.h>.
    public static DefaultValue PI_DEFAULT_VALUE = new DefaultValue("3.14159265358979323846", PI);

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
        return value;
    }

    public DefaultValueExprDef getDefaultValueExprDef() {
        return defaultValueExprDef;
    }
}

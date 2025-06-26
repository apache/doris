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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeV2Literal;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.PrimitiveType;
import org.apache.doris.nereids.types.coercion.RangeScalable;
import org.apache.doris.nereids.types.coercion.ScaleTimeType;

/**
 * Time v2 type in Nereids.
 */
public class TimeV2Type extends PrimitiveType implements RangeScalable, ScaleTimeType {

    public static final int MAX_SCALE = 6;
    public static final TimeV2Type INSTANCE = new TimeV2Type(0);
    public static final TimeV2Type MAX = new TimeV2Type(MAX_SCALE);

    private static final int WIDTH = 8;
    private final int scale;

    private TimeV2Type(int scale) {
        this.scale = scale;
    }

    private TimeV2Type() {
        scale = 0;
    }

    @Override
    public Type toCatalogDataType() {
        return ScalarType.createTimeV2Type(scale);
    }

    /**
     * create TimeV2Type from scale
     */
    public static TimeV2Type of(int scale) {
        if (scale > MAX_SCALE || scale < 0) {
            throw new AnalysisException("Scale of Datetime/Time must between 0 and 6. Scale was set to: " + scale);
        }
        return new TimeV2Type(scale);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TimeV2Type) {
            return ((TimeV2Type) o).getScale() == getScale();
        }
        return false;
    }

    @Override
    public ScaleTimeType forTypeFromString(StringLikeLiteral str) {
        return forTypeFromString(str.getStringValue());
    }

    /**
     * return proper type of timev2 for string
     * if the string is not a valid timev2, return MAX
     */
    public static TimeV2Type forTypeFromString(String s) {
        try {
            new TimeV2Literal(INSTANCE, s);
        } catch (AnalysisException e) {
            return MAX;
        }
        return TimeV2Type.of(TimeV2Literal.determineScale(s));
    }

    /**
     * return proper type of timev2 for other type
     */
    public static TimeV2Type forType(DataType dataType) {
        if (dataType instanceof TimeV2Type) {
            return (TimeV2Type) dataType;
        }
        if (dataType instanceof IntegralType || dataType instanceof BooleanType || dataType instanceof NullType
                || dataType instanceof DateTimeType) {
            return INSTANCE;
        }
        if (dataType instanceof DecimalV3Type) {
            return TimeV2Type.of(Math.min(((DecimalV3Type) dataType).getScale(), 6));
        }
        if (dataType instanceof DecimalV2Type) {
            return TimeV2Type.of(Math.min(((DecimalV2Type) dataType).getScale(), 6));
        }
        if (dataType instanceof DateTimeV2Type) {
            return TimeV2Type.of(((DateTimeV2Type) dataType).getScale());
        }
        return MAX;
    }

    public ScaleTimeType scaleTypeForType(DataType dataType) {
        return forType(dataType);
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public String toSql() {
        return super.toSql() + "(" + scale + ")";
    }
}

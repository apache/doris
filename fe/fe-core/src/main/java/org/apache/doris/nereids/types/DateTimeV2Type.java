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
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.types.coercion.IntegralType;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Datetime type in Nereids.
 */
public class DateTimeV2Type extends DateLikeType {
    public static final int MAX_SCALE = 6;
    public static final DateTimeV2Type SYSTEM_DEFAULT = new DateTimeV2Type(0);
    public static final DateTimeV2Type MAX = new DateTimeV2Type(MAX_SCALE);

    private static final int WIDTH = 8;

    private final int scale;

    private DateTimeV2Type(int scale) {
        Preconditions.checkArgument(0 <= scale && scale <= MAX_SCALE);
        this.scale = scale;
    }

    /**
     * create DateTimeV2Type from scale
     */
    public static DateTimeV2Type of(int scale) {
        if (scale == SYSTEM_DEFAULT.scale) {
            return SYSTEM_DEFAULT;
        } else if (scale > MAX_SCALE || scale < 0) {
            throw new AnalysisException("Scale of Datetime/Time must between 0 and 6. Scale was set to: " + scale);
        } else {
            return new DateTimeV2Type(scale);
        }
    }

    public static DateTimeV2Type getWiderDatetimeV2Type(DateTimeV2Type t1, DateTimeV2Type t2) {
        if (t1.scale > t2.scale) {
            return t1;
        }
        return t2;
    }

    /**
     * return proper type of datetimev2 for other type
     */
    public static DateTimeV2Type forType(DataType dataType) {
        if (dataType instanceof DateTimeV2Type) {
            return (DateTimeV2Type) dataType;
        }
        if (dataType instanceof IntegralType || dataType instanceof BooleanType || dataType instanceof NullType) {
            return SYSTEM_DEFAULT;
        }
        return MAX;
    }

    /**
     * return proper type of datetimev2 for String
     * may be we need to check for validity?
     */
    public static DateTimeV2Type forTypeFromString(String s) {
        int scale = DateTimeLiteral.determineScale(s);
        return DateTimeV2Type.of(scale);
    }

    @Override
    public String toSql() {
        return super.toSql() + "(" + scale + ")";
    }

    @Override
    public Type toCatalogDataType() {
        return ScalarType.createDatetimeV2Type(scale);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DateTimeV2Type that = (DateTimeV2Type) o;
        return Objects.equals(scale, that.scale);
    }

    @Override
    public boolean acceptsType(AbstractDataType other) {
        return other instanceof DateTimeV2Type;
    }

    @Override
    public int width() {
        return WIDTH;
    }

    public int getScale() {
        return scale;
    }
}

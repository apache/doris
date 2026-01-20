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
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.types.coercion.ScaleTimeType;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * TimeStampTz type in Nereids.
 */
public class TimeStampTzType extends DateLikeType implements ScaleTimeType {
    public static final int MAX_SCALE = 6;
    public static final TimeStampTzType SYSTEM_DEFAULT = new TimeStampTzType(0);
    public static final TimeStampTzType MAX = new TimeStampTzType(MAX_SCALE);
    public static final TimeStampTzType WILDCARD = new TimeStampTzType(-1);

    private static final int WIDTH = 8;

    private final int scale;

    private TimeStampTzType(int scale) {
        Preconditions.checkArgument(scale == -1 || (0 <= scale && scale <= MAX_SCALE));
        this.scale = scale;
    }

    @Override
    public Type toCatalogDataType() {
        return ScalarType.createTimeStampTzType(scale);
    }

    /**
     * create TimeStampTzType
     */
    public static TimeStampTzType of(int scale) {
        if (scale == SYSTEM_DEFAULT.scale) {
            return SYSTEM_DEFAULT;
        } else if (scale > MAX_SCALE || scale < 0) {
            throw new AnalysisException("Scale of TimeStampTz must between 0 and 6. Scale was set to: " + scale);
        } else {
            return new TimeStampTzType(scale);
        }
    }

    public int getScale() {
        return scale;
    }

    @Override
    public ScaleTimeType forTypeFromString(StringLikeLiteral s) {
        return forTypeFromString(s.getValue());
    }

    public static TimeStampTzType forTypeFromString(String s) {
        DateTimeV2Type dateTimeV2Type = DateTimeV2Type.forTypeFromString(s);
        return TimeStampTzType.of(dateTimeV2Type.getScale());
    }

    @Override
    public ScaleTimeType scaleTypeForType(DataType dataType) {
        DateTimeV2Type dateTimeV2Type = DateTimeV2Type.forType(dataType);
        return TimeStampTzType.of(dateTimeV2Type.getScale());
    }

    /**
     * Determine the minimum scale
     */
    public static TimeStampTzType forTypeFromMicroSeconds(long microSecond) {
        if (microSecond == 0) {
            return TimeStampTzType.of(0);
        }
        int scale = TimeStampTzType.MAX_SCALE;
        long value = microSecond;
        while (scale > 0 && value % 10 == 0) {
            value /= 10;
            scale--;
        }
        return TimeStampTzType.of(scale);
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
        TimeStampTzType that = (TimeStampTzType) o;
        return Objects.equals(scale, that.scale);
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public String toSql() {
        return super.toSql() + "(" + scale + ")";
    }
}

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
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.types.coercion.ScaleTimeType;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Timestamp represented as signed nanoseconds since the Unix epoch.
 *
 * <p>TIMESTAMP_NS is date-like and has a fixed scale, but it is not a DATETIMEV2 precision variant:
 * the two types have different physical representations and coercion rules. Keeping it directly under
 * {@link DateLikeType} prevents generic DATETIMEV2 code from treating its fixed nanosecond scale as a
 * DATETIMEV2 scale.</p>
 */
public final class TimeStampNsType extends DateLikeType implements ScaleTimeType {
    public static final int SCALE = ScalarType.TIMESTAMP_NS_SCALE;
    public static final TimeStampNsType INSTANCE = new TimeStampNsType();

    private static final int WIDTH = 8;

    private TimeStampNsType() {
    }

    @Override
    public boolean isInjectiveCastTo(DataType target) {
        return target instanceof TimeStampNsType || target instanceof CharacterType;
    }

    @Override
    public Type toCatalogDataType() {
        return ScalarType.createTimeStampNsType();
    }

    @Override
    public ScaleTimeType scaleTypeForType(DataType dataType) {
        return INSTANCE;
    }

    @Override
    public ScaleTimeType forTypeFromString(StringLikeLiteral str) {
        return INSTANCE;
    }

    @Override
    public int getScale() {
        return SCALE;
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public double rangeLength(double high, double low) {
        if (high == low) {
            return 0;
        }
        if (Double.isInfinite(high) || Double.isInfinite(low)) {
            return Double.POSITIVE_INFINITY;
        }
        try {
            LocalDateTime to = toLocalDateTime(high);
            LocalDateTime from = toLocalDateTime(low);
            return ChronoUnit.SECONDS.between(from, to);
        } catch (DateTimeException e) {
            return Double.POSITIVE_INFINITY;
        }
    }

    @Override
    public String toSql() {
        return "timestamp_ns";
    }
}

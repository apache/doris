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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.types.coercion.DateLikeType;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Datetime type in Nereids.
 */
public class DateTimeType extends DateLikeType {

    public static final DateTimeType INSTANCE = new DateTimeType();
    public static final DateTimeType NOT_CONVERSION = new DateTimeType(false);

    private static final int WIDTH = 16;

    private final boolean shouldConversion;

    private DateTimeType() {
        this.shouldConversion = true;
    }

    private DateTimeType(boolean shouldConversion) {
        this.shouldConversion = shouldConversion;
    }

    @Override
    public DataType conversion() {
        if (Config.enable_date_conversion && shouldConversion) {
            return DateTimeV2Type.SYSTEM_DEFAULT;
        }
        return this;
    }

    @Override
    public Type toCatalogDataType() {
        return Type.DATETIME;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof DateTimeType;
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
}

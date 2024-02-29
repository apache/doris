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

package org.apache.doris.job.common;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public enum IntervalUnit {
    SECOND("second", 0L, TimeUnit.SECONDS::toMillis),
    MINUTE("minute", 0L, TimeUnit.MINUTES::toMillis),
    HOUR("hour", 0L, TimeUnit.HOURS::toMillis),
    DAY("day", 0L, TimeUnit.DAYS::toMillis),
    WEEK("week", 0L, v -> TimeUnit.DAYS.toMillis(v * 7));
    private final String unit;

    public String getUnit() {
        return unit;
    }

    public static IntervalUnit fromString(String unit) {
        for (IntervalUnit u : IntervalUnit.values()) {
            if (u.unit.equalsIgnoreCase(unit)) {
                return u;
            }
        }
        return null;
    }

    private final Object defaultValue;

    private final Function<Long, Long> converter;

    IntervalUnit(String unit, Long defaultValue, Function<Long, Long> converter) {
        this.unit = unit;
        this.defaultValue = defaultValue;
        this.converter = converter;
    }

    IntervalUnit getByName(String name) {
        return Arrays.stream(IntervalUnit.values())
                .filter(config -> config.getUnit().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown configuration interval " + name));
    }

    public Long getIntervalMs(Long interval) {
        return (Long) (interval != null ? converter.apply(interval) : defaultValue);
    }
}

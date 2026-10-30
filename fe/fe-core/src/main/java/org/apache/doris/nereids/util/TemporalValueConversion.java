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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;

/** Timezone value conversion shared by literal CAST and executable SQL functions. */
public final class TemporalValueConversion {
    private TemporalValueConversion() {
    }

    /** Resolve string zones with the existing SQL offset validation and DST policy. */
    public static LocalDateTime convertZone(LocalDateTime datetime, String fromTz, String toTz) {
        // Validate timezone offset ranges before parsing
        validateTimezoneOffset(fromTz);
        validateTimezoneOffset(toTz);

        DateTimeFormatter zoneFormatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendZoneOrOffsetId()
                .toFormatter()
                .withResolverStyle(ResolverStyle.STRICT);
        ZoneId fromZone = ZoneId.from(zoneFormatter.parse(fromTz));
        ZoneId toZone = ZoneId.from(zoneFormatter.parse(toTz));

        Instant instant = DateTimeLiteral.convertLocalToInstant(datetime, fromZone);
        return LocalDateTime.ofInstant(instant, toZone);
    }

    private static void validateTimezoneOffset(String timezone) {
        // Pattern to match offset format like +HH:MM or -HH:MM
        if (timezone.matches("^[+-]\\d{2}:\\d{2}$")) {
            boolean positive = timezone.charAt(0) == '+';
            int hour = Integer.parseInt(timezone.substring(1, 3));
            int minute = Integer.parseInt(timezone.substring(4, 6));

            if (!positive && hour > 12) {
                throw new AnalysisException("Invalid timezone offset: " + timezone
                        + ". Timezone offsets must be between -12:00 and +14:00");
            } else if (positive && hour > 14) {
                throw new AnalysisException("Invalid timezone offset: " + timezone
                        + ". Timezone offsets must be between -12:00 and +14:00");
            }

            if (minute != 0 && minute != 15 && minute != 30 && minute != 45) {
                throw new AnalysisException("Invalid timezone offset: " + timezone
                        + ". Minute part should be 00, 15, 30, or 45");
            }
        }
    }

    /**
     * Convert an already resolved zone while retaining the fraction. NS casts round to the target
     * scale before this step; the legacy string-zone path above keeps its existing DST behavior.
     */
    public static LocalDateTime convertZonePreservingFraction(LocalDateTime value, ZoneId from, ZoneId to) {
        return LocalDateTime.ofInstant(DateTimeLiteral.convertLocalToInstantPreservingFraction(value, from), to);
    }
}

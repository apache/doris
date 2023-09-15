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

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;

/**
 * Composition of MySQL Date:
 * 1. Without any delimiter, e.g.: '20220801', '20220801010101'
 *    MySQL supports '20220801T010101' but doesn't support '20220801 010101'
 *    In this scenario, MySQL does not support zone/offset
 * 2. With delimiters (':'/'-'):
 *    The composition is Date + ' '/'T' + Time + Zone + Offset
 *    Both Zone and Offset are Optional
 *    Date needs to be cautious about the two-digit year https://dev.mysql.com/doc/refman/8.0/en/datetime.html
 *      Dates containing 2-digit year values are ambiguous as the century is unknown.
 *      MySQL interprets 2-digit year values using these rules:
 *          Year values in the range 00-69 become 2000-2069.
 *          Year values in the range 70-99 become 1970-1999.
 *    Time needs to be cautious about microseconds:
 *      Note incomplete times 'hh:mm:ss', 'hh:mm', 'D hh:mm', 'D hh', or 'ss'
 */
public class DateTimeFormatterUtils {
    public static final DateTimeFormatter ZONE_FORMATTER = new DateTimeFormatterBuilder()
            .optionalStart()
            // .appendZoneText(TextStyle.FULL)
            .appendZoneOrOffsetId()
            .optionalEnd()
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);
    public static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendLiteral('-').appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral('-').appendValue(ChronoField.DAY_OF_MONTH, 2)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    // HH[:mm][:ss][.microsecond]
    public static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendOptional(new DateTimeFormatterBuilder()
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 1, 6, true).toFormatter())
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    // Time without delimiter: HHmmss[.microsecond]
    private static final DateTimeFormatter BASIC_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendOptional(new DateTimeFormatterBuilder()
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 1, 6, true).toFormatter())
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    // yyyymmdd
    private static final DateTimeFormatter BASIC_DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    // Date without delimiter
    public static final DateTimeFormatter BASIC_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(BASIC_DATE_FORMATTER)
            .appendLiteral('T')
            .append(BASIC_TIME_FORMATTER)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    // Date without delimiter
    public static final DateTimeFormatter BASIC_FORMATTER_WITHOUT_T = new DateTimeFormatterBuilder()
            .append(BASIC_DATE_FORMATTER)
            .appendOptional(BASIC_TIME_FORMATTER)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);

    // Datetime
    public static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
            .appendLiteral(' ')
            .append(TIME_FORMATTER)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    public static final DateTimeFormatter ZONE_DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendLiteral('-').appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral('-').appendValue(ChronoField.DAY_OF_MONTH, 2)
            .append(ZONE_FORMATTER)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    public static final DateTimeFormatter ZONE_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
            .appendLiteral(' ')
            .append(TIME_FORMATTER)
            .append(ZONE_FORMATTER)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
}

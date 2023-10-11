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
 * Following format is *standard* format of date/datetime. It will keep format of memory/output/printf is standard.
 */
public class StandardDateFormat {
    // Date: %Y-%m-%d
    public static DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendLiteral('-').appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral('-').appendValue(ChronoField.DAY_OF_MONTH, 2)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    // %H:%i:%s
    public static DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    public static DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
            .appendLiteral(' ')
            .append(TIME_FORMATTER)
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    // "%Y-%m-%d %H:%i:%s.%f"
    public static DateTimeFormatter DATE_TIME_FORMATTER_TO_MICRO_SECOND = new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
            .appendLiteral(' ')
            .append(TIME_FORMATTER)
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true) // Notice: min size is 0
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
}

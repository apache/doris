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
import org.apache.doris.qe.ConnectContext;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.WeekFields;

/**
 * date util tools.
 */
public class DateUtils {
    private static final WeekFields weekFields = WeekFields.of(DayOfWeek.SUNDAY, 7);

    /**
     * format builder.
     */
    public static DateTimeFormatterBuilder formatBuilder(String pattern) throws AnalysisException {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char character = pattern.charAt(i);
            if (escaped) {
                switch (character) {
                    case 'a': // %a Abbreviated weekday name (Sun..Sat)
                        builder.appendText(ChronoField.DAY_OF_WEEK, TextStyle.SHORT);
                        break;
                    case 'b': // %b Abbreviated month name (Jan..Dec)
                        builder.appendText(ChronoField.MONTH_OF_YEAR, TextStyle.SHORT);
                        break;
                    case 'c': // %c Month, numeric (0..12)
                        builder.appendValue(ChronoField.MONTH_OF_YEAR);
                        break;
                    case 'd': // %d Day of the month, numeric (00..31)
                        builder.appendValue(ChronoField.DAY_OF_MONTH, 2);
                        break;
                    case 'e': // %e Day of the month, numeric (0..31)
                        builder.appendValue(ChronoField.DAY_OF_MONTH);
                        break;
                    case 'H': // %H Hour (00..23)
                        builder.appendValue(ChronoField.HOUR_OF_DAY, 2);
                        break;
                    case 'h': // %h Hour (01..12)
                    case 'I': // %I Hour (01..12)
                        builder.appendValue(ChronoField.HOUR_OF_AMPM, 2);
                        break;
                    case 'i': // %i Minutes, numeric (00..59)
                        builder.appendValue(ChronoField.MINUTE_OF_HOUR, 2);
                        break;
                    case 'j': // %j Day of year (001..366)
                        builder.appendValue(ChronoField.DAY_OF_YEAR, 3);
                        break;
                    case 'k': // %k Hour (0..23)
                        builder.appendValue(ChronoField.HOUR_OF_DAY);
                        break;
                    case 'l': // %l Hour (1..12)
                        builder.appendValue(ChronoField.HOUR_OF_AMPM);
                        break;
                    case 'M': // %M Month name (January..December)
                        builder.appendText(ChronoField.MONTH_OF_YEAR, TextStyle.FULL);
                        break;
                    case 'm': // %m Month, numeric (00..12)
                        builder.appendValue(ChronoField.MONTH_OF_YEAR, 2);
                        break;
                    case 'p': // %p AM or PM
                        builder.appendText(ChronoField.AMPM_OF_DAY);
                        break;
                    case 'r': // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
                        builder.appendValue(ChronoField.HOUR_OF_AMPM, 2)
                                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                                .appendLiteral(' ')
                                .appendText(ChronoField.AMPM_OF_DAY, TextStyle.FULL)
                                .toFormatter();
                        break;
                    case 'S': // %S Seconds (00..59)
                    case 's': // %s Seconds (00..59)
                        builder.appendValue(ChronoField.SECOND_OF_MINUTE, 2);
                        break;
                    case 'T': // %T Time, 24-hour (HH:mm:ss)
                        builder.appendPattern("HH:mm:ss");
                        break;
                    case 'V': // %V Week (01..53), where Sunday is the first day of the week; used with %X
                        builder.appendValue(weekFields.weekOfWeekBasedYear(), 2);
                        break;
                    case 'v': // %v Week (01..53), where Monday is the first day of the week; used with %x
                        builder.appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2);
                        break;
                    case 'W': // %W Weekday name (Sunday..Saturday)
                        builder.appendText(ChronoField.DAY_OF_WEEK, TextStyle.FULL);
                        break;
                    case 'x': // %x Year for the week where Monday is the first day of the week,
                        builder.appendValue(IsoFields.WEEK_BASED_YEAR, 4);
                        break;
                    case 'X':
                        builder.appendValue(weekFields.weekBasedYear(), 4, 10, SignStyle.EXCEEDS_PAD);
                        break;
                    case 'Y': // %Y Year, numeric, four digits
                        // %X Year for the week, where Sunday is the first day of the week,
                        // numeric, four digits; used with %v
                        builder.appendValue(ChronoField.YEAR, 4);
                        break;
                    case 'y': // %y Year, numeric (two digits)
                        builder.appendValueReduced(ChronoField.YEAR, 2, 2, 1970);
                        break;
                    // TODO(Gabriel): support microseconds in date literal
                    case 'D': // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
                    case 'f': // %f Microseconds (000000..999999)
                    case 'U': // %U Week (00..53), where Sunday is the first day of the week
                    case 'u': // %u Week (00..53), where Monday is the first day of the week
                    case 'w': // %w Day of the week (0=Sunday..6=Saturday)
                        throw new AnalysisException(String.format("%%%s not supported in date format string",
                                character));
                    case '%': // %% A literal "%" character
                        builder.appendLiteral('%');
                        break;
                    default: // %<x> The literal character represented by <x>
                        builder.appendLiteral(character);
                        break;
                }
                escaped = false;
            } else if (character == '%') {
                escaped = true;
            } else {
                builder.appendLiteral(character);
            }
        }
        return builder;
    }

    /**
     * construct local date time from string
     */
    public static LocalDateTime getTime(DateTimeFormatter formatter, String value) {
        TemporalAccessor accessor = formatter.parse(value);
        return LocalDateTime.of(
                getOrDefault(accessor, ChronoField.YEAR),
                getOrDefault(accessor, ChronoField.MONTH_OF_YEAR),
                getOrDefault(accessor, ChronoField.DAY_OF_MONTH),
                getHourOrDefault(accessor),
                getOrDefault(accessor, ChronoField.MINUTE_OF_HOUR),
                getOrDefault(accessor, ChronoField.SECOND_OF_MINUTE),
                getOrDefault(accessor, ChronoField.NANO_OF_SECOND));
    }

    public static int getOrDefault(final TemporalAccessor accessor, final ChronoField field) {
        return accessor.isSupported(field) ? accessor.get(field) : /* default value */ 0;
    }

    /**
     * get hour from accessor, if not support hour field, return 0
     */
    public static int getHourOrDefault(final TemporalAccessor accessor) {
        if (accessor.isSupported(ChronoField.HOUR_OF_DAY)) {
            return accessor.get(ChronoField.HOUR_OF_DAY);
        } else if (accessor.isSupported(ChronoField.HOUR_OF_AMPM)) {
            return accessor.get(ChronoField.HOUR_OF_AMPM);
        } else {
            return 0;
        }
    }

    public static ZoneId getTimeZone() {
        if (ConnectContext.get() == null || ConnectContext.get().getSessionVariable() == null) {
            return ZoneId.systemDefault();
        }
        return ZoneId.of(ConnectContext.get().getSessionVariable().getTimeZone());
    }
}

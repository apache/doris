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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.util.DateUtils;

import com.google.common.base.Preconditions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Utility class providing a static factory method for creating {@link DateLiteral} instances
 * from string representations. This replicates the parsing logic of the
 * {@code DateLiteral(String, Type)} constructor and its {@code init()} method.
 */
public class DateLiteralUtils {

    private static final Pattern HAS_OFFSET_PART = Pattern.compile("[\\+\\-]\\d{2}:\\d{2}");

    private DateLiteralUtils() {
    }

    /**
     * Creates a {@link DateLiteral} by parsing the given string, replicating the exact behavior
     * of the {@code DateLiteral(String, Type)} constructor and its internal {@code init()} method.
     *
     * @param s    the date/datetime string to parse
     * @param type the target date type, or {@code null} for auto-detection
     * @return a new {@link DateLiteral} instance
     * @throws AnalysisException if the string cannot be parsed or the resulting value is out of range
     */
    public static DateLiteral createDateLiteral(String s, @Nullable Type type) throws AnalysisException {
        try {
            if (type != null) {
                Preconditions.checkArgument(type.isDateType());
            }
            TemporalAccessor dateTime = null;
            boolean parsed = false;
            int offset = 0;

            // parse timezone
            if (haveTimeZoneOffset(s) || haveTimeZoneName(s)) {
                String tzString = new String();
                if (haveTimeZoneName(s)) { // GMT, UTC+8, Z[, CN, Asia/Shanghai]
                    int split = getTimeZoneSplitPos(s);
                    Preconditions.checkArgument(split > 0);
                    tzString = s.substring(split);
                    s = s.substring(0, split);
                } else { // +04:30
                    Preconditions.checkArgument(
                            s.charAt(s.length() - 6) == '-' || s.charAt(s.length() - 6) == '+');
                    tzString = s.substring(s.length() - 6);
                    s = s.substring(0, s.length() - 6);
                }
                ZoneId zone = ZoneId.of(tzString);
                ZoneId dorisZone = DateUtils.getTimeZone();
                if (type != null && type.isTimeStampTz()) {
                    dorisZone = ZoneId.of("UTC");
                }
                offset = dorisZone.getRules().getOffset(Instant.now()).getTotalSeconds()
                        - zone.getRules().getOffset(Instant.now()).getTotalSeconds();
            }

            if (!s.contains("-")) {
                // handle format like 20210106, but should not handle 2021-1-6
                for (DateTimeFormatter formatter : DateLiteral.formatterList) {
                    try {
                        dateTime = formatter.parse(s);
                        parsed = true;
                        break;
                    } catch (DateTimeParseException ex) {
                        // ignore
                    }
                }
                if (!parsed) {
                    throw new AnalysisException("Invalid date value: " + s);
                }
            } else {
                String[] datePart = s.contains(" ") ? s.split(" ")[0].split("-") : s.split("-");
                DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
                if (datePart.length != 3) {
                    throw new AnalysisException("Invalid date value: " + s);
                }
                for (int i = 0; i < datePart.length; i++) {
                    switch (i) {
                        case 0:
                            if (datePart[i].length() == 2) {
                                // If year is represented by two digits, number bigger than 70 will be
                                // prefixed with 19 otherwise 20. e.g. 69 -> 2069, 70 -> 1970.
                                builder.appendValueReduced(ChronoField.YEAR, 2, 2, 1970);
                            } else {
                                builder.appendPattern(
                                        String.join("", Collections.nCopies(datePart[i].length(), "u")));
                            }
                            break;
                        case 1:
                            builder.appendPattern(
                                    String.join("", Collections.nCopies(datePart[i].length(), "M")));
                            break;
                        case 2:
                            builder.appendPattern(
                                    String.join("", Collections.nCopies(datePart[i].length(), "d")));
                            break;
                        default:
                            throw new AnalysisException("Two many parts in date format " + s);
                    }
                    if (i < datePart.length - 1) {
                        builder.appendLiteral("-");
                    }
                }
                if (s.contains(" ")) {
                    builder.appendLiteral(" ");
                }
                String[] timePart = s.contains(" ") ? s.split(" ")[1].split(":") : new String[]{};
                if (timePart.length > 0 && type != null
                        && (type.equals(Type.DATE) || type.equals(Type.DATEV2))) {
                    throw new AnalysisException("Invalid date value: " + s);
                }
                if (timePart.length == 0 && type != null
                        && (type.equals(Type.DATETIME) || type.equals(Type.DATETIMEV2))) {
                    throw new AnalysisException("Invalid datetime value: " + s);
                }
                for (int i = 0; i < timePart.length; i++) {
                    switch (i) {
                        case 0:
                            builder.appendPattern(
                                    String.join("", Collections.nCopies(timePart[i].length(), "H")));
                            break;
                        case 1:
                            builder.appendPattern(
                                    String.join("", Collections.nCopies(timePart[i].length(), "m")));
                            break;
                        case 2:
                            builder.appendPattern(String.join("", Collections.nCopies(
                                    timePart[i].contains(".")
                                            ? timePart[i].split("\\.")[0].length()
                                            : timePart[i].length(), "s")));
                            if (timePart[i].contains(".")) {
                                builder.appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true);
                            }
                            break;
                        default:
                            throw new AnalysisException("Two many parts in time format " + s);
                    }
                    if (i < timePart.length - 1) {
                        builder.appendLiteral(":");
                    }
                }
                // The default resolver style is 'SMART', which parses "2022-06-31" as "2022-06-30"
                // and does not throw an exception. 'STRICT' is used here.
                DateTimeFormatter formatter = builder.toFormatter().withResolverStyle(ResolverStyle.STRICT);
                dateTime = formatter.parse(s);
                parsed = true;
            }

            Preconditions.checkArgument(parsed);
            long year = getOrDefault(dateTime, ChronoField.YEAR, 0);
            long month = getOrDefault(dateTime, ChronoField.MONTH_OF_YEAR, 0);
            long day = getOrDefault(dateTime, ChronoField.DAY_OF_MONTH, 0);
            long hour = getOrDefault(dateTime, ChronoField.HOUR_OF_DAY, 0);
            long minute = getOrDefault(dateTime, ChronoField.MINUTE_OF_HOUR, 0);
            long second = getOrDefault(dateTime, ChronoField.SECOND_OF_MINUTE, 0);
            long microsecond = getOrDefault(dateTime, ChronoField.MICRO_OF_SECOND, 0);

            if (type != null) {
                if (microsecond != 0 && type.isDatetime()) {
                    int dotIndex = s.lastIndexOf(".");
                    int scale = s.length() - dotIndex - 1;
                    type = ScalarType.createDatetimeV2Type(scale);
                }
            } else {
                if (hour == 0 && minute == 0 && second == 0 && microsecond == 0) {
                    type = ScalarType.getDefaultDateType(Type.DATE);
                } else {
                    type = ScalarType.getDefaultDateType(Type.DATETIME);
                    if (type.isDatetimeV2() && microsecond != 0) {
                        int scale = 6;
                        for (int i = 0; i < 6; i++) {
                            if (microsecond % Math.pow(10.0, i + 1) > 0) {
                                break;
                            } else {
                                scale -= 1;
                            }
                        }
                        type = ScalarType.createDatetimeV2Type(scale);
                    }
                }
            }

            // Apply timezone offset before constructing the DateLiteral.
            // The original init() calls this.plusSeconds(offset) which internally uses
            // LocalDateTime arithmetic. We replicate that here directly.
            if (offset != 0) {
                LocalDateTime ldt = LocalDateTime.of(
                        (int) year, (int) month, (int) day,
                        (int) hour, (int) minute, (int) second,
                        (int) (microsecond * 1000));
                ldt = ldt.plusSeconds(offset);
                year = ldt.getYear();
                month = ldt.getMonthValue();
                day = ldt.getDayOfMonth();
                hour = ldt.getHour();
                minute = ldt.getMinute();
                second = ldt.getSecond();
                // microsecond is intentionally NOT updated, matching the original init() behavior
            }

            // Construct DateLiteral using the appropriate constructor based on the determined type
            DateLiteral result;
            if (type.isDate() || type.isDateV2()) {
                result = new DateLiteral(year, month, day, type);
            } else if (microsecond != 0 && (type.isDatetimeV2() || type.isTimeStampTz())) {
                result = new DateLiteral(year, month, day, hour, minute, second, microsecond, type);
            } else {
                result = new DateLiteral(year, month, day, hour, minute, second, type);
            }

            if (result.checkRange() || result.checkDate()) {
                throw new AnalysisException("Datetime value is out of range");
            }
            return result;
        } catch (Exception ex) {
            throw new AnalysisException("date literal [" + s + "] is invalid: " + ex.getMessage());
        }
    }

    private static int getOrDefault(TemporalAccessor accessor, ChronoField field, int defaultValue) {
        return accessor.isSupported(field) ? accessor.get(field) : defaultValue;
    }

    private static boolean haveTimeZoneOffset(String arg) {
        Preconditions.checkArgument(arg.length() > 6);
        return HAS_OFFSET_PART.matcher(arg.substring(arg.length() - 6)).matches();
    }

    private static boolean haveTimeZoneName(String arg) {
        for (char ch : arg.toCharArray()) {
            if (Character.isUpperCase(ch) && ch != 'T') {
                return true;
            }
        }
        return false;
    }

    private static int getTimeZoneSplitPos(String arg) {
        int split = arg.length() - 1;
        for (; !Character.isAlphabetic(arg.charAt(split)); split--) {
        } // skip +8 of UTC+8
        for (; split >= 0 && (Character.isUpperCase(arg.charAt(split)) || arg.charAt(split) == '/'); split--) {
        }
        return split + 1;
    }
}

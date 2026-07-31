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
import org.apache.doris.common.util.TimeUtils;
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

/** Utility methods for parsing legacy date-like literals from strings. */
public class DateLiteralUtils {

    private static final Pattern HAS_OFFSET_PART = Pattern.compile("[\\+\\-]\\d{2}:\\d{2}");
    private static final long NANOSECONDS_PER_SECOND = 1_000_000_000L;
    private static final long[] NANOSECOND_SCALE_FACTORS = {
            1_000_000_000L, 100_000_000L, 10_000_000L, 1_000_000L, 100_000L,
            10_000L, 1_000L, 100L, 10L, 1L
    };

    private DateLiteralUtils() {
    }

    /**
     * Parse a legacy date-like literal.
     *
     * <p>TIMESTAMP_NS has a dedicated {@link TimeStampNsLiteral} because its signed epoch-nanosecond
     * representation and range differ from {@link DateLiteral}. Callers that can receive any
     * date-like type must therefore use this method and retain the {@link LiteralExpr} result.</p>
     */
    public static LiteralExpr createLiteral(String s, @Nullable Type type) throws AnalysisException {
        return createDateTimeLiteral(s, type);
    }

    /**
     * Parse a literal backed by {@link DateLiteral}.
     *
     * <p>This compatibility entry point deliberately rejects TIMESTAMP_NS instead of hiding it in
     * DateLiteral's microsecond fields. New date-generic callers should use {@link #createLiteral}.</p>
     */
    public static DateLiteral createDateLiteral(String s, @Nullable Type type) throws AnalysisException {
        LiteralExpr literal = createDateTimeLiteral(s, type);
        if (!(literal instanceof DateLiteral)) {
            throw new AnalysisException("date literal [" + s
                    + "] resolves to TIMESTAMP_NS; use DateLiteralUtils.createLiteral instead");
        }
        return (DateLiteral) literal;
    }

    private static LiteralExpr createDateTimeLiteral(String s, @Nullable Type type) throws AnalysisException {
        try {
            if (type != null) {
                Preconditions.checkArgument(type.isDateType());
            }
            TemporalAccessor dateTime = null;
            boolean parsed = false;
            ZoneId sourceZone = null;
            // Explicit DATETIME and TIMESTAMPTZ retain their pre-TIMESTAMP_NS limit of six
            // fractional digits. DATETIMEV2 needs the nanosecond parser so inputs wider than its
            // declared scale can be rounded before DateLiteral discards the extra digits. A null
            // type is used when decoding an untyped DATE_LITERAL thrift node, where 7-9 digits are
            // the only information available to infer TIMESTAMP_NS.
            boolean parseNanoseconds = type == null || type.isDatetimeV2() || type.isTimeStampNs();

            // parse timezone
            if (haveTimeZoneOffset(s) || haveTimeZoneName(s)) {
                String tzString;
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
                sourceZone = ZoneId.of(tzString);
            }

            int nanosecondGuardDigit = -1;
            if (parseNanoseconds) {
                // java.time parses at most nine fractional digits. Keep the tenth digit separately
                // so fixed-scale TIMESTAMP_NS can still implement the same half-up rounding as BE.
                nanosecondGuardDigit = DateUtils.getNanosecondGuardDigit(s);
                s = DateUtils.truncateFractionalSecondForJavaParser(s);
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
                        && (type.equals(Type.DATETIME) || type.equals(Type.DATETIMEV2)
                        || type.equals(Type.TIMESTAMP_NS))) {
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
                                builder.appendFraction(parseNanoseconds
                                                ? ChronoField.NANO_OF_SECOND : ChronoField.MICRO_OF_SECOND,
                                        0, parseNanoseconds ? ScalarType.TIMESTAMP_NS_SCALE
                                                : ScalarType.MAX_DATETIMEV2_SCALE, true);
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
            long nanosecond = parseNanoseconds
                    ? getOrDefault(dateTime, ChronoField.NANO_OF_SECOND, 0)
                    : getOrDefault(dateTime, ChronoField.MICRO_OF_SECOND, 0) * 1000L;

            if (type != null) {
                if (nanosecond != 0 && type.isDatetime()) {
                    int dotIndex = s.lastIndexOf(".");
                    int scale = s.length() - dotIndex - 1;
                    type = ScalarType.createDatetimeV2Type(scale);
                }
            } else {
                // Type inference must observe the scale-9 rounded fraction. For example,
                // .1234560005 rounds to .123456001 and therefore requires TIMESTAMP_NS rather
                // than DATETIMEV2(6). A carry is also time-bearing even when its rounded fraction
                // becomes zero, as in 00:00:00.9999999995 -> 00:00:01.000000000.
                long inferredNanosecond = nanosecond + (nanosecondGuardDigit >= 5 ? 1 : 0);
                boolean fractionalCarry = inferredNanosecond == NANOSECONDS_PER_SECOND;
                if (fractionalCarry) {
                    inferredNanosecond = 0;
                }
                if (hour == 0 && minute == 0 && second == 0
                        && inferredNanosecond == 0 && !fractionalCarry) {
                    type = ScalarType.getDefaultDateType(Type.DATE);
                } else {
                    type = ScalarType.getDefaultDateType(Type.DATETIME);
                    if (type.isDatetimeV2() && inferredNanosecond != 0) {
                        int scale = ScalarType.TIMESTAMP_NS_SCALE;
                        long fractionalSecond = inferredNanosecond;
                        while (fractionalSecond % 10 == 0) {
                            fractionalSecond /= 10;
                            scale--;
                        }
                        type = scale > ScalarType.MAX_DATETIMEV2_SCALE
                                ? ScalarType.createTimeStampNsType()
                                : ScalarType.createDatetimeV2Type(scale);
                    }
                }
            }

            LocalDateTime literalDateTime = LocalDateTime.of(
                    (int) year, (int) month, (int) day,
                    (int) hour, (int) minute, (int) second, (int) nanosecond);

            // Recompute the timezone offset using the target date rather than
            // Instant.now(), so DST-sensitive zones (e.g. America/Chicago)
            // produce the correct shift regardless of when the code runs.
            // The original code used Instant.now() which returns the current
            // DST offset; when the target date falls in a different DST period
            // the computed shift is wrong by the DST gap.
            //
            // We derive the destination wall clock directly from the resolved
            // target instant (via LocalDateTime.ofInstant) rather than computing
            // a delta offset. This correctly handles source-zone DST gaps:
            // e.g. CET spring-forward resolves 02:30 CET (nonexistent) to
            // 03:30 CEST = 01:30Z; ofInstant then reconstructs the correct
            // wall clock for the destination zone from the resolved instant.
            if (sourceZone != null) {
                ZoneId dorisZone = TimeUtils.getTimeZone().toZoneId();
                if (type != null && type.isTimeStampTz()) {
                    dorisZone = ZoneId.of("UTC");
                }
                Instant targetInstant = literalDateTime.atZone(sourceZone).toInstant();
                literalDateTime = LocalDateTime.ofInstant(targetInstant, dorisZone);
            }

            if (type.isDatetimeV2() || type.isTimeStampNs()) {
                int scale = type.isTimeStampNs()
                        ? ScalarType.TIMESTAMP_NS_SCALE : ((ScalarType) type).getScalarScale();
                // Round before constructing the legacy literal. DateLiteral stores only
                // microseconds, so constructing it first would silently truncate discarded digits
                // and could make FE partition boundaries disagree with Nereids and BE.
                literalDateTime = roundFractionalSecond(
                        literalDateTime, scale, nanosecondGuardDigit);
            }

            if (type.isTimeStampNs()) {
                TimeStampNsLiteral result = new TimeStampNsLiteral(
                        literalDateTime.getYear(), literalDateTime.getMonthValue(),
                        literalDateTime.getDayOfMonth(), literalDateTime.getHour(),
                        literalDateTime.getMinute(), literalDateTime.getSecond(),
                        literalDateTime.getNano());
                result.checkValueValid();
                return result;
            }

            DateLiteral result;
            if (type.isDate() || type.isDateV2()) {
                result = new DateLiteral(literalDateTime.getYear(), literalDateTime.getMonthValue(),
                        literalDateTime.getDayOfMonth(), type);
            } else if (literalDateTime.getNano() != 0
                    && (type.isDatetimeV2() || type.isTimeStampTz())) {
                result = new DateLiteral(literalDateTime.getYear(), literalDateTime.getMonthValue(),
                        literalDateTime.getDayOfMonth(), literalDateTime.getHour(),
                        literalDateTime.getMinute(), literalDateTime.getSecond(),
                        literalDateTime.getNano() / 1000L, type);
            } else {
                result = new DateLiteral(literalDateTime.getYear(), literalDateTime.getMonthValue(),
                        literalDateTime.getDayOfMonth(), literalDateTime.getHour(),
                        literalDateTime.getMinute(), literalDateTime.getSecond(), type);
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

    /**
     * Round a fractional second half-up to the target scale.
     *
     * <p>For DATETIMEV2 scales 0-6, the first discarded digit is already present in the parsed
     * nanosecond value. At TIMESTAMP_NS scale 9 there is no discarded digit inside that value, so
     * the separately retained tenth digit decides whether to add one nanosecond. A rounded value
     * of one billion nanoseconds is carried into the next civil second, including date rollover.</p>
     */
    private static LocalDateTime roundFractionalSecond(
            LocalDateTime value, int scale, int nanosecondGuardDigit) {
        long factor = NANOSECOND_SCALE_FACTORS[scale];
        long roundingOffset = factor == 1
                ? (nanosecondGuardDigit >= 5 ? 1 : 0) : factor / 2;
        long roundedNanosecond = (value.getNano() + roundingOffset) / factor * factor;
        if (roundedNanosecond == NANOSECONDS_PER_SECOND) {
            return value.withNano(0).plusSeconds(1);
        }
        return value.withNano((int) roundedNanosecond);
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

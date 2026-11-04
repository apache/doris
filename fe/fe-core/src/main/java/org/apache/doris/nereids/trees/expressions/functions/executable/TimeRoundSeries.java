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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.qe.ConnectContext;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;

/**
 * executable functions:
 * year/month/day/hour/minute/second_ceil/floor
 */
@Developing
public class TimeRoundSeries {
    private static final LocalDateTime START_ORIGINAL_DAY = LocalDateTime.of(1, 1, 1, 0, 0, 0);

    enum DATE {
        YEAR,
        QUARTER,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND
    }

    private static LocalDateTime getDateCeilOrFloor(DATE tag, LocalDateTime date, int period,
            LocalDateTime origin, boolean getCeil) {
        return DateTimeV2Literal.fromJavaDateType(
                getCalendarBoundary(tag, date, period, origin, getCeil), 6).toJavaDateType();
    }

    // A candidate can exceed the Doris year range. Check that range after the
    // calendar calculation, so a gap search can still find a valid boundary.
    private static LocalDateTime getOptionalCalendarBoundary(DATE tag, LocalDateTime date, int period,
            LocalDateTime origin, boolean getCeil) {
        try {
            LocalDateTime boundary = getCalendarBoundary(tag, date, period, origin, getCeil);
            return DateTimeV2Literal.isDateOutOfRange(boundary) ? null : boundary;
        } catch (DateTimeException e) {
            // An Integer.MAX_VALUE year period can exceed Java's year range too.
            return null;
        }
    }

    // Use the same local calendar calculation as BE time_round_two_args.
    private static LocalDateTime getCalendarBoundary(DATE tag, LocalDateTime date, int period, LocalDateTime origin,
            boolean getCeil) {
        if (period < 1) {
            throw new AnalysisException("Operation " + tag.name().toLowerCase()
                    + (getCeil ? "_ceil" : "_floor") + " of " + period + " out of range");
        }

        DateTimeV2Literal dt = (DateTimeV2Literal) DateTimeV2Literal.fromJavaDateType(date);
        DateTimeV2Literal start = (DateTimeV2Literal) DateTimeV2Literal.fromJavaDateType(origin);
        long diff = 0;
        long trivialPart = 0;
        switch (tag) {
            case YEAR: {
                diff = dt.getYear() - start.getYear();
                trivialPart = (dt.getValue() % 10000000000L) - (start.getValue() % 10000000000L);
                break;
            }
            case QUARTER: {
                diff = (dt.getYear() - start.getYear()) * 4 + (dt.getMonth() - start.getMonth()) / 3;
                // For QUARTER, use same logic as MONTH to preserve month+day+time info for proper comparison
                trivialPart = (((dt.getMonth() - 1) % 3) * 100000000L + (dt.getValue() % 100000000L))
                        - (((start.getMonth() - 1) % 3) * 100000000L + (start.getValue() % 100000000L));
                break;
            }
            case MONTH: {
                diff = (dt.getYear() - start.getYear()) * 12 + (dt.getMonth() - start.getMonth());
                trivialPart = (dt.getValue() % 100000000L) - (start.getValue() % 100000000L);
                break;
            }
            case DAY: {
                diff = dt.getTotalDays() - start.getTotalDays();
                long part2 = dt.getHour() * 3600 + dt.getMinute() * 60 + dt.getSecond();
                long part1 = start.getHour() * 3600 + start.getMinute() * 60 + start.getSecond();
                trivialPart = part2 - part1;
                break;
            }
            case HOUR: {
                diff = (dt.getTotalDays() - start.getTotalDays()) * 24 + (dt.getHour() - start.getHour());
                trivialPart = (dt.getMinute() * 60 + dt.getSecond())
                        - (start.getMinute() * 60 + start.getSecond());
                break;
            }
            case MINUTE: {
                diff = (dt.getTotalDays() - start.getTotalDays()) * 24 * 60 + (dt.getHour() - start.getHour()) * 60
                        + (dt.getMinute() - start.getMinute());
                trivialPart = dt.getSecond() - start.getSecond();
                break;
            }
            case SECOND: {
                diff = (dt.getTotalDays() - start.getTotalDays()) * 24 * 60 * 60
                        + (dt.getHour() - start.getHour()) * 60 * 60
                        + (dt.getMinute() - start.getMinute()) * 60
                        + (dt.getSecond() - start.getSecond());
                trivialPart = date.getNano() - origin.getNano();
                break;
            }
            default: {
                return null;
            }
        }
        trivialPart = (trivialPart == 0 ? date.getNano() - origin.getNano() : trivialPart);
        if (getCeil) {
            diff = diff + (trivialPart > 0 ? 1 : 0);
        } else {
            diff = diff - (trivialPart < 0 ? 1 : 0);
        }
        long deltaInsidePeriod = (diff % period + period) % period;
        long step = diff - deltaInsidePeriod;
        if (getCeil) {
            step = step + (deltaInsidePeriod == 0 ? 0 : period);
        }
        LocalDateTime startTime = start.toJavaDateType();
        switch (tag) {
            case YEAR:
                return startTime.plusYears(step);
            case QUARTER:
                return startTime.plusMonths(step * 3);
            case MONTH:
                return startTime.plusMonths(step);
            case DAY:
                return startTime.plusDays(step);
            case HOUR:
                return startTime.plusHours(step);
            case MINUTE:
                return startTime.plusMinutes(step);
            case SECOND:
                return startTime.plusSeconds(step);
            default:
                return null;
        }
    }

    /**
     * Helper method for TimestampTz ceil/floor operations with timezone conversion.
     * 1. Convert UTC to local time (using session timezone)
     * 2. Perform ceil/floor operation on local time
     * 3. Convert result back to UTC
     */
    private static LocalDateTime getDateCeilOrFloorForTimestampTz(DATE tag, LocalDateTime utcDate, int period,
            LocalDateTime utcOrigin, boolean getCeil) {
        // Get session timezone, default to UTC if not available
        String timeZone = "UTC";
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable() != null) {
            timeZone = ctx.getSessionVariable().timeZone;
        }
        ZoneId zoneId = ZoneId.of(timeZone);

        // Seconds use the UTC time line in BE. They do not need a local calendar boundary.
        if (tag == DATE.SECOND) {
            return getDateCeilOrFloor(tag, utcDate, period, utcOrigin, getCeil);
        }
        ZonedDateTime zonedDate = utcDate.atZone(ZoneOffset.UTC).withZoneSameInstant(zoneId);
        LocalDateTime localDate = zonedDate.toLocalDateTime();

        // The default origin is a local calendar value. A supplied origin is an instant.
        // On Lord Howe, an Oct 5 origin uses +10:30 even if the Oct 6 input uses +11:00.
        LocalDateTime localOrigin = utcOrigin.equals(START_ORIGINAL_DAY) ? START_ORIGINAL_DAY
                : utcOrigin.atZone(ZoneOffset.UTC).withZoneSameInstant(zoneId).toLocalDateTime();
        // Keep the input's occurrence for hour and minute boundaries in an overlap, as BE does.
        ZoneOffset preferredOffset = tag == DATE.HOUR || tag == DATE.MINUTE ? zonedDate.getOffset() : null;

        Instant input = utcDate.toInstant(ZoneOffset.UTC);
        Instant selected = null;
        LocalDateTime localResult = getOptionalCalendarBoundary(tag, localDate, period, localOrigin, getCeil);
        // On Lord Howe, the 02:15 gap candidate becomes 02:45, after input 02:35.
        // Recompute each candidate from the original calendar origin.
        while (localResult != null) {
            Instant candidate = DateUtils.convertLocalToInstant(localResult, zoneId, preferredOffset);
            if (getCeil || !candidate.isAfter(input)) {
                selected = candidate;
                break;
            }
            localResult = getOptionalCalendarBoundary(tag, localResult.minusNanos(1000), period, localOrigin, false);
        }
        // A local ceil in year 10000 can coexist with a shifted gap boundary in 9999.
        // Search the gap before reporting an out-of-range result.
        Instant lower = selected == null || input.isBefore(selected) ? input : selected;
        Instant cursor = (selected == null || input.isAfter(selected) ? input : selected).plusNanos(1);
        ZoneRules rules = zoneId.getRules();
        ZoneOffsetTransition transition = rules.previousTransition(cursor);
        // Include the preceding transition. Its gap can move a boundary into the interval.
        while (transition != null) {
            // Skip gaps whose shifted times are all before the search interval.
            if (transition.isGap() && transition.getInstant().plus(transition.getDuration()).isAfter(lower)) {
                selected = selectGapBoundary(tag, input, period, localOrigin, getCeil, zoneId,
                        preferredOffset, transition, selected);
            }
            if (!transition.getInstant().isAfter(lower)) {
                break;
            }
            transition = rules.previousTransition(transition.getInstant());
        }
        if (selected == null) {
            throw new AnalysisException("datetime out of range");
        }
        return LocalDateTime.ofInstant(selected, ZoneOffset.UTC);
    }

    private static Instant selectGapBoundary(DATE tag, Instant input, int period, LocalDateTime origin,
            boolean getCeil, ZoneId zone, ZoneOffset preferredOffset, ZoneOffsetTransition transition,
            Instant selected) {
        LocalDateTime start = transition.getDateTimeBefore();
        LocalDateTime end = transition.getDateTimeAfter();
        LocalDateTime reference = LocalDateTime.ofInstant(input, transition.getOffsetBefore());
        if (getCeil) {
            reference = reference.isBefore(start) ? start : reference;
        } else if (!reference.isBefore(end)) {
            reference = end.minusNanos(1000);
        }
        // On Lord Howe, local grid boundaries 02:15 and 02:35 resolve to 02:45 and 02:35.
        // Their instant order differs from their local order. Keep the closest instant.
        LocalDateTime local = getOptionalCalendarBoundary(tag, reference, period, origin, getCeil);
        if (local != null && !local.isBefore(start) && local.isBefore(end)) {
            selected = selectCloserBoundary(input,
                    DateUtils.convertLocalToInstant(local, zone, preferredOffset), selected, getCeil);
        }
        if (getCeil) {
            local = getOptionalCalendarBoundary(tag, end, period, origin, true);
            // A yearly boundary after a gap in year 9999 can exceed the supported range.
            if (local != null) {
                selected = selectCloserBoundary(input,
                        DateUtils.convertLocalToInstant(local, zone, preferredOffset), selected, true);
            }
        }
        return selected;
    }

    private static Instant selectCloserBoundary(Instant input, Instant candidate, Instant selected, boolean getCeil) {
        if (getCeil ? !candidate.isBefore(input) && (selected == null || candidate.isBefore(selected))
                : !candidate.isAfter(input) && (selected == null || candidate.isAfter(selected))) {
            return candidate;
        }
        return selected;
    }

    /**
     * datetime arithmetic function year-ceil
     */
    @ExecFunction(name = "year_ceil")
    public static Expression yearCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeil(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeil(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.YEAR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeil(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeil(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.YEAR, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeil(TimestampTzLiteral date, IntegerLiteral period, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function quarter-ceil
     */
    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeil(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeil(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.QUARTER, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeil(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.QUARTER, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeil(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.QUARTER, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeil(TimestampTzLiteral date, IntegerLiteral period, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.QUARTER, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function month-ceil
     */
    @ExecFunction(name = "month_ceil")
    public static Expression monthCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeil(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeil(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MONTH, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeil(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeil(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MONTH, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeil(TimestampTzLiteral date, IntegerLiteral period, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function day-ceil
     */
    @ExecFunction(name = "day_ceil")
    public static Expression dayCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeil(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeil(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.DAY, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeil(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.DAY, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeil(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.DAY, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeil(TimestampTzLiteral date, IntegerLiteral period, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.DAY, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function hour-ceil
     */
    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeil(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeil(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.HOUR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeil(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeil(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.HOUR, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeil(TimestampTzLiteral date, IntegerLiteral period, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function minute-ceil
     */
    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeil(DateTimeV2Literal date, IntegerLiteral period,
            DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeil(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MINUTE, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeil(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeil(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MINUTE, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeil(TimestampTzLiteral date, IntegerLiteral period,
            TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function SECOND-ceil
     */
    @ExecFunction(name = "second_ceil")
    public static Expression secondCeil(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeil(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true));
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeil(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeil(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true));
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeil(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeil(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeil(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeil(DateTimeV2Literal date, IntegerLiteral period,
            DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeil(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.SECOND, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeil(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, true), date.getScale());
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeil(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.SECOND, date.toJavaDateType(),
                1, origin.toJavaDateType(), true), date.commonScale(origin));
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeil(TimestampTzLiteral date, IntegerLiteral period,
            TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), true), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function year-floor
     */
    @ExecFunction(name = "year_floor")
    public static Expression yearFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloor(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloor(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.YEAR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloor(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloor(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.YEAR, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloor(TimestampTzLiteral date, IntegerLiteral period, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.YEAR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function quarter-floor
     */
    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloor(DateTimeV2Literal date, IntegerLiteral period,
            DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.QUARTER, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloor(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.QUARTER, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloor(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.QUARTER, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloor(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.QUARTER, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloor(TimestampTzLiteral date, IntegerLiteral period,
            TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.QUARTER, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function month-floor
     */
    @ExecFunction(name = "month_floor")
    public static Expression monthFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloor(DateTimeV2Literal date, IntegerLiteral period,
            DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloor(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MONTH, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloor(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloor(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MONTH, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloor(TimestampTzLiteral date, IntegerLiteral period,
            TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MONTH, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function day-floor
     */
    @ExecFunction(name = "day_floor")
    public static Expression dayFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloor(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.DAY, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloor(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.DAY, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloor(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.DAY, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloor(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.DAY, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloor(TimestampTzLiteral date, IntegerLiteral period, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.DAY, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function hour-floor
     */
    @ExecFunction(name = "hour_floor")
    public static Expression hourFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloor(DateTimeV2Literal date, IntegerLiteral period, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloor(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.HOUR, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloor(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloor(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.HOUR, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloor(TimestampTzLiteral date, IntegerLiteral period, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.HOUR, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function minute-floor
     */
    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloor(DateTimeV2Literal date, IntegerLiteral period,
            DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloor(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MINUTE, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloor(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloor(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MINUTE, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloor(TimestampTzLiteral date, IntegerLiteral period,
            TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.MINUTE, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    /**
     * datetime arithmetic function SECOND-floor
     */
    @ExecFunction(name = "second_floor")
    public static Expression secondFloor(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloor(DateV2Literal date, IntegerLiteral period) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false));
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloor(DateV2Literal date, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloor(DateV2Literal date, IntegerLiteral period, DateV2Literal origin) {
        return DateV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false));
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloor(DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloor(DateTimeV2Literal date, IntegerLiteral period) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloor(DateTimeV2Literal date, DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloor(DateTimeV2Literal date, IntegerLiteral period,
            DateTimeV2Literal origin) {
        return DateTimeV2Literal.fromJavaDateType(getDateCeilOrFloor(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloor(TimestampTzLiteral date) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.SECOND, date.toJavaDateType(),
                1, START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloor(TimestampTzLiteral date, IntegerLiteral period) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), START_ORIGINAL_DAY, false), date.getScale());
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloor(TimestampTzLiteral date, TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.SECOND, date.toJavaDateType(),
                1, origin.toJavaDateType(), false), date.commonScale(origin));
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloor(TimestampTzLiteral date, IntegerLiteral period,
            TimestampTzLiteral origin) {
        return TimestampTzLiteral.fromJavaDateType(getDateCeilOrFloorForTimestampTz(DATE.SECOND, date.toJavaDateType(),
                period.getValue(), origin.toJavaDateType(), false), date.commonScale(origin));
    }

    private static Expression roundTimeStampNs(DATE tag, TimeStampNsLiteral date, int period,
            LocalDateTime origin, boolean getCeil) {
        LocalDateTime result = tag == DATE.QUARTER
                ? getTimeStampNsQuarterCeilOrFloor(date.toJavaDateType(), period, origin, getCeil)
                : getDateCeilOrFloor(tag, date.toJavaDateType(), period, origin, getCeil);
        return TimeStampNsLiteral.fromJavaDateType(result.withNano(origin.getNano()));
    }

    private static LocalDateTime getTimeStampNsQuarterCeilOrFloor(LocalDateTime date, int period,
            LocalDateTime origin, boolean getCeil) {
        if (period < 1) {
            throw new AnalysisException("Operation quarter_" + (getCeil ? "ceil" : "floor")
                    + " of " + period + " out of range");
        }

        long totalMonths = (date.getYear() - origin.getYear()) * 12L
                + date.getMonthValue() - origin.getMonthValue();
        long diff = totalMonths / 3;
        long remainingMonths = totalMonths % 3;
        int remainderComparison = Long.compare(remainingMonths, 0);
        if (remainderComparison == 0) {
            remainderComparison = Integer.compare(date.getDayOfMonth(), origin.getDayOfMonth());
            if (remainderComparison == 0) {
                remainderComparison = date.toLocalTime().compareTo(origin.toLocalTime());
            }
        }
        if (getCeil) {
            diff += remainderComparison > 0 ? 1 : 0;
        } else {
            diff -= remainderComparison < 0 ? 1 : 0;
        }

        long deltaInsidePeriod = (diff % period + period) % period;
        long step = diff - deltaInsidePeriod;
        if (getCeil && deltaInsidePeriod != 0) {
            step += period;
        }
        return origin.plusMonths(step * 3);
    }

    /** TIMESTAMP_NS overloads of year_ceil. */
    @ExecFunction(name = "year_ceil")
    public static Expression yearCeilTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.YEAR, date, 1, START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.YEAR, date, period.getValue(), START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeilTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.YEAR, date, 1, origin.toJavaDateType(), true);
    }

    @ExecFunction(name = "year_ceil")
    public static Expression yearCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.YEAR, date, period.getValue(), origin.toJavaDateType(), true);
    }

    /** TIMESTAMP_NS overloads of quarter_ceil. */
    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeilTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.QUARTER, date, 1, START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.QUARTER, date, period.getValue(), START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeilTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.QUARTER, date, 1, origin.toJavaDateType(), true);
    }

    @ExecFunction(name = "quarter_ceil")
    public static Expression quarterCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.QUARTER, date, period.getValue(), origin.toJavaDateType(), true);
    }

    /** TIMESTAMP_NS overloads of month_ceil. */
    @ExecFunction(name = "month_ceil")
    public static Expression monthCeilTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.MONTH, date, 1, START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.MONTH, date, period.getValue(), START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeilTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.MONTH, date, 1, origin.toJavaDateType(), true);
    }

    @ExecFunction(name = "month_ceil")
    public static Expression monthCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.MONTH, date, period.getValue(), origin.toJavaDateType(), true);
    }

    /** TIMESTAMP_NS overloads of day_ceil. */
    @ExecFunction(name = "day_ceil")
    public static Expression dayCeilTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.DAY, date, 1, START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.DAY, date, period.getValue(), START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeilTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.DAY, date, 1, origin.toJavaDateType(), true);
    }

    @ExecFunction(name = "day_ceil")
    public static Expression dayCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.DAY, date, period.getValue(), origin.toJavaDateType(), true);
    }

    /** TIMESTAMP_NS overloads of hour_ceil. */
    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeilTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.HOUR, date, 1, START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.HOUR, date, period.getValue(), START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeilTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.HOUR, date, 1, origin.toJavaDateType(), true);
    }

    @ExecFunction(name = "hour_ceil")
    public static Expression hourCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.HOUR, date, period.getValue(), origin.toJavaDateType(), true);
    }

    /** TIMESTAMP_NS overloads of minute_ceil. */
    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeilTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.MINUTE, date, 1, START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.MINUTE, date, period.getValue(), START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeilTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.MINUTE, date, 1, origin.toJavaDateType(), true);
    }

    @ExecFunction(name = "minute_ceil")
    public static Expression minuteCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.MINUTE, date, period.getValue(), origin.toJavaDateType(), true);
    }

    /** TIMESTAMP_NS overloads of second_ceil. */
    @ExecFunction(name = "second_ceil")
    public static Expression secondCeilTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.SECOND, date, 1, START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.SECOND, date, period.getValue(), START_ORIGINAL_DAY, true);
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeilTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.SECOND, date, 1, origin.toJavaDateType(), true);
    }

    @ExecFunction(name = "second_ceil")
    public static Expression secondCeilTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.SECOND, date, period.getValue(), origin.toJavaDateType(), true);
    }

    /** TIMESTAMP_NS overloads of year_floor. */
    @ExecFunction(name = "year_floor")
    public static Expression yearFloorTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.YEAR, date, 1, START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.YEAR, date, period.getValue(), START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloorTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.YEAR, date, 1, origin.toJavaDateType(), false);
    }

    @ExecFunction(name = "year_floor")
    public static Expression yearFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.YEAR, date, period.getValue(), origin.toJavaDateType(), false);
    }

    /** TIMESTAMP_NS overloads of quarter_floor. */
    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloorTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.QUARTER, date, 1, START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.QUARTER, date, period.getValue(), START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloorTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.QUARTER, date, 1, origin.toJavaDateType(), false);
    }

    @ExecFunction(name = "quarter_floor")
    public static Expression quarterFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.QUARTER, date, period.getValue(), origin.toJavaDateType(), false);
    }

    /** TIMESTAMP_NS overloads of month_floor. */
    @ExecFunction(name = "month_floor")
    public static Expression monthFloorTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.MONTH, date, 1, START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.MONTH, date, period.getValue(), START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloorTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.MONTH, date, 1, origin.toJavaDateType(), false);
    }

    @ExecFunction(name = "month_floor")
    public static Expression monthFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.MONTH, date, period.getValue(), origin.toJavaDateType(), false);
    }

    /** TIMESTAMP_NS overloads of day_floor. */
    @ExecFunction(name = "day_floor")
    public static Expression dayFloorTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.DAY, date, 1, START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.DAY, date, period.getValue(), START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloorTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.DAY, date, 1, origin.toJavaDateType(), false);
    }

    @ExecFunction(name = "day_floor")
    public static Expression dayFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.DAY, date, period.getValue(), origin.toJavaDateType(), false);
    }

    /** TIMESTAMP_NS overloads of hour_floor. */
    @ExecFunction(name = "hour_floor")
    public static Expression hourFloorTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.HOUR, date, 1, START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.HOUR, date, period.getValue(), START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloorTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.HOUR, date, 1, origin.toJavaDateType(), false);
    }

    @ExecFunction(name = "hour_floor")
    public static Expression hourFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.HOUR, date, period.getValue(), origin.toJavaDateType(), false);
    }

    /** TIMESTAMP_NS overloads of minute_floor. */
    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloorTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.MINUTE, date, 1, START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.MINUTE, date, period.getValue(), START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloorTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.MINUTE, date, 1, origin.toJavaDateType(), false);
    }

    @ExecFunction(name = "minute_floor")
    public static Expression minuteFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.MINUTE, date, period.getValue(), origin.toJavaDateType(), false);
    }

    /** TIMESTAMP_NS overloads of second_floor. */
    @ExecFunction(name = "second_floor")
    public static Expression secondFloorTimeStampNs(TimeStampNsLiteral date) {
        return roundTimeStampNs(DATE.SECOND, date, 1, START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period) {
        return roundTimeStampNs(DATE.SECOND, date, period.getValue(), START_ORIGINAL_DAY, false);
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloorTimeStampNs(TimeStampNsLiteral date, TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.SECOND, date, 1, origin.toJavaDateType(), false);
    }

    @ExecFunction(name = "second_floor")
    public static Expression secondFloorTimeStampNs(TimeStampNsLiteral date, IntegerLiteral period,
            TimeStampNsLiteral origin) {
        return roundTimeStampNs(DATE.SECOND, date, period.getValue(), origin.toJavaDateType(), false);
    }
}

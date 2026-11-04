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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.DateUtils;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;

/** Shared range predicates for monotonic date and time expressions. */
public final class MonotonicityUtils {
    // The last second accepted by FE constant folding for from_{second,millisecond,microsecond}.
    private static final long MAX_EPOCH_SECONDS = 253402271999L;

    private MonotonicityUtils() {
    }

    /** All arguments other than the range argument must be literals. */
    public static boolean hasConstantOtherArguments(ExpressionTrait expression, int rangeArgument) {
        for (int i = 0; i < expression.arity(); i++) {
            if (i != rangeArgument && !(expression.child(i) instanceof Literal)) {
                return false;
            }
        }
        return true;
    }

    public static boolean hasOneVariableArgument(ExpressionTrait expression) {
        return expression.child(0) instanceof Literal ^ expression.child(1) instanceof Literal;
    }

    public static boolean hasConstantRoundingArguments(ExpressionTrait expression) {
        return expression.arity() >= 1 && expression.arity() <= 3
                && (expression.arity() == 1 || !(expression.child(0) instanceof Literal))
                && hasConstantOtherArguments(expression, 0);
    }

    public static boolean hasMonotonicFormat(Expression format) {
        return format instanceof StringLikeLiteral
                && DateUtils.monoFormat.contains(((StringLikeLiteral) format).getValue());
    }

    /** A calendar field cannot wrap while its enclosing date fields are unchanged. */
    public static boolean isWithinSameYear(Literal lower, Literal upper) {
        return hasSameDatePrefix(lower, upper, 1);
    }

    public static boolean isWithinSameMonth(Literal lower, Literal upper) {
        return hasSameDatePrefix(lower, upper, 2);
    }

    public static boolean isWithinSameDay(Literal lower, Literal upper) {
        return hasSameDatePrefix(lower, upper, 3);
    }

    private static boolean hasSameDatePrefix(Literal lower, Literal upper, int fields) {
        if (!(lower instanceof DateLiteral && upper instanceof DateLiteral)) {
            return false;
        }
        DateLiteral start = (DateLiteral) lower;
        DateLiteral end = (DateLiteral) upper;
        return start.getYear() == end.getYear()
                && (fields < 2 || start.getMonth() == end.getMonth())
                && (fields < 3 || start.getDay() == end.getDay());
    }

    /** Preserve the date-only behavior of sub-day extractors. */
    public static boolean isWithinSameHour(Literal lower, Literal upper) {
        return hasSameTimePrefix(lower, upper, 1);
    }

    public static boolean isWithinSameMinute(Literal lower, Literal upper) {
        return hasSameTimePrefix(lower, upper, 2);
    }

    public static boolean isWithinSameSecond(Literal lower, Literal upper) {
        return hasSameTimePrefix(lower, upper, 3);
    }

    private static boolean hasSameTimePrefix(Literal lower, Literal upper, int fields) {
        if (lower instanceof TimeStampNsLiteral && upper instanceof TimeStampNsLiteral) {
            TimeStampNsLiteral start = (TimeStampNsLiteral) lower;
            TimeStampNsLiteral end = (TimeStampNsLiteral) upper;
            return hasSameDatePrefix(lower, upper, 3)
                    && start.getHour() == end.getHour()
                    && (fields < 2 || start.getMinute() == end.getMinute())
                    && (fields < 3 || start.getSecond() == end.getSecond());
        }
        if (lower instanceof DateTimeLiteral && upper instanceof DateTimeLiteral) {
            DateTimeLiteral start = (DateTimeLiteral) lower;
            DateTimeLiteral end = (DateTimeLiteral) upper;
            return hasSameDatePrefix(lower, upper, 3)
                    && start.getHour() == end.getHour()
                    && (fields < 2 || start.getMinute() == end.getMinute())
                    && (fields < 3 || start.getSecond() == end.getSecond());
        }
        return lower instanceof DateLiteral && upper instanceof DateLiteral;
    }

    /** Check the range in the unit used by from_second, from_millisecond, or from_microsecond. */
    public static boolean isEpochToLocalMonotonic(Literal lower, Literal upper, long unitsPerSecond) {
        if (!(lower instanceof BigIntLiteral) || ((BigIntLiteral) lower).getValue() < 0) {
            return false;
        }
        ZoneId zoneId;
        try {
            zoneId = TimeUtils.getDorisZoneId();
        } catch (DateTimeException e) {
            return false;
        }
        if (zoneId.getRules().isFixedOffset()) {
            return true;
        }
        if (!(upper instanceof BigIntLiteral)) {
            return false;
        }
        long lowerValue = ((BigIntLiteral) lower).getValue();
        long upperValue = ((BigIntLiteral) upper).getValue();
        if (upperValue < lowerValue || upperValue / unitsPerSecond > MAX_EPOCH_SECONDS) {
            return false;
        }
        Instant lowerInstant = epochToInstant(lowerValue, unitsPerSecond);
        Instant upperInstant = epochToInstant(upperValue, unitsPerSecond);
        if (LocalDateTime.ofInstant(upperInstant, zoneId).getYear() > 9999) {
            return false;
        }
        return isInstantToLocalMonotonic(zoneId, lowerInstant, upperInstant);
    }

    private static Instant epochToInstant(long value, long unitsPerSecond) {
        return Instant.ofEpochSecond(value / unitsPerSecond,
                value % unitsPerSecond * (1000000000L / unitsPerSecond));
    }

    /** Instant-to-local conversion can move backward only at a fall-back transition. */
    public static boolean isInstantToLocalMonotonic(ZoneId zoneId, Instant lower, Instant upper) {
        return zoneId.getRules().isFixedOffset()
                || lower != null && upper != null && !upper.isBefore(lower)
                && !hasFallbackTransitionInInstantRange(zoneId, lower, upper);
    }

    /** Local-to-instant conversion may lose its order across a spring-forward gap. */
    public static boolean isLocalToInstantMonotonic(ZoneId zoneId, LocalDateTime lower, LocalDateTime upper) {
        return zoneId.getRules().isFixedOffset()
                || lower != null && upper != null && !upper.isBefore(lower)
                && !hasGapTransitionInLocalDateTimeRange(zoneId, lower, upper);
    }

    /** Whether the instant interval (lower, upper] crosses a fall-back transition. */
    private static boolean hasFallbackTransitionInInstantRange(ZoneId zoneId, Instant lower, Instant upper) {
        ZoneRules rules = zoneId.getRules();
        ZoneOffsetTransition transition = rules.nextTransition(lower);
        while (transition != null && !transition.getInstant().isAfter(upper)) {
            if (transition.isOverlap()) {
                return true;
            }
            transition = rules.nextTransition(transition.getInstant());
        }
        return false;
    }

    /** Whether the local interval intersects a spring-forward gap. */
    private static boolean hasGapTransitionInLocalDateTimeRange(
            ZoneId zoneId, LocalDateTime lower, LocalDateTime upper) {
        ZoneRules rules = zoneId.getRules();
        Instant searchStart = lower.minusDays(2).atZone(zoneId).toInstant();
        ZoneOffsetTransition transition = rules.nextTransition(searchStart);
        while (transition != null && !transition.getDateTimeBefore().isAfter(upper)) {
            if (transition.isGap()
                    && upper.isAfter(transition.getDateTimeBefore())
                    && lower.isBefore(transition.getDateTimeAfter())) {
                return true;
            }
            transition = rules.nextTransition(transition.getInstant());
        }
        return false;
    }

    /** Dispatch date-like casts to their range checks. */
    public static boolean isDateLikeCastMonotonic(
            DataType sourceType, DataType targetType, Literal lower, Literal upper) {
        if (!(sourceType instanceof DateLikeType && targetType instanceof DateLikeType)) {
            return false;
        }
        if (targetType instanceof TimeStampNsType && !isRangeWithinTimeStampNs(targetType, lower, upper)) {
            return false;
        }
        if (sourceType instanceof TimeStampTzType
                && (targetType instanceof DateTimeV2Type || targetType instanceof TimeStampNsType)) {
            int destinationScale = targetType instanceof DateTimeV2Type
                    ? ((DateTimeV2Type) targetType).getScale() : TimeStampNsType.SCALE;
            return isTimeStampTzToLocalMonotonic(
                    (TimeStampTzType) sourceType, destinationScale, lower, upper);
        }
        if (sourceType instanceof TimeStampNsType && targetType instanceof TimeStampTzType) {
            return isTimeStampNsToTimeStampTzMonotonic((TimeStampTzType) targetType, lower, upper);
        }
        return true;
    }

    private static boolean isRangeWithinTimeStampNs(DataType targetType, Literal lower, Literal upper) {
        if (lower == null || upper == null) {
            return false;
        }
        try {
            return !(lower.checkedCastTo(targetType) instanceof NullLiteral)
                    && !(upper.checkedCastTo(targetType) instanceof NullLiteral);
        } catch (AnalysisException e) {
            return false;
        }
    }

    /** Check a TIMESTAMPTZ cast to a local datetime after accounting for its output scale. */
    public static boolean isTimeStampTzToLocalMonotonic(
            TimeStampTzType sourceType, int destinationScale, Literal lower, Literal upper) {
        ZoneId zoneId;
        try {
            zoneId = TimeUtils.getDorisZoneId();
        } catch (DateTimeException e) {
            return false;
        }
        if (zoneId.getRules().isFixedOffset()) {
            return true;
        }
        // Rounding UTC before zone conversion may cross a fall-back just outside the input range.
        if (destinationScale < sourceType.getScale()
                || !(lower instanceof TimestampTzLiteral) || !(upper instanceof TimestampTzLiteral)) {
            return false;
        }
        Instant lowerInstant = ((TimestampTzLiteral) lower).toJavaDateType().toInstant(ZoneOffset.UTC);
        Instant upperInstant = ((TimestampTzLiteral) upper).toJavaDateType().toInstant(ZoneOffset.UTC);
        return isInstantToLocalMonotonic(zoneId, lowerInstant, upperInstant);
    }

    /** Check a local TIMESTAMP_NS cast after rounding to the TIMESTAMPTZ output scale. */
    public static boolean isTimeStampNsToTimeStampTzMonotonic(
            TimeStampTzType destinationType, Literal lower, Literal upper) {
        ZoneId zoneId;
        try {
            zoneId = TimeUtils.getDorisZoneId();
        } catch (DateTimeException e) {
            return false;
        }
        if (zoneId.getRules().isFixedOffset()) {
            return true;
        }
        if (!(lower instanceof TimeStampNsLiteral) || !(upper instanceof TimeStampNsLiteral)) {
            return false;
        }
        LocalDateTime lowerDateTime = roundTimeStampNs((TimeStampNsLiteral) lower, destinationType.getScale());
        LocalDateTime upperDateTime = roundTimeStampNs((TimeStampNsLiteral) upper, destinationType.getScale());
        return isLocalToInstantMonotonic(zoneId, lowerDateTime, upperDateTime);
    }

    private static LocalDateTime roundTimeStampNs(TimeStampNsLiteral literal, int scale) {
        long factor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - scale);
        LocalDateTime dateTime = literal.toJavaDateType().plusNanos(factor / 2);
        return dateTime.withNano((int) (dateTime.getNano() / factor * factor));
    }
}

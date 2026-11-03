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

import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.ColumnRange;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

/** Monotonicity of date arithmetic that can clamp the day to the end of a month. */
public interface CalendarAddSubMonotonic extends DateAddSubMonotonic {
    @Override
    default boolean isMonotonic(Literal lower, Literal upper) {
        // Clamping consecutive dates to the same date resets the time at midnight. DATE has no
        // time part, and within one input day the calendar adjustment preserves the time order.
        return canEvaluateRange()
                && (child(0).getDataType().isDateV2Type() || MonotonicityUtils.isWithinSameDay(lower, upper));
    }

    /** The date part remains non-decreasing even when clamping resets the time part. */
    default boolean canEvaluateRange() {
        return MonotonicityUtils.hasConstantOtherArguments(this, 0);
    }

    @Override
    default Optional<ColumnRange> inferRange(Literal lower, Literal upper, ExpressionRewriteContext context) {
        if (!canEvaluateRange()) {
            return Optional.empty();
        }
        boolean monotonic = isMonotonic(lower, upper);
        return evaluateRangeEndpoints(lower, upper, context).flatMap(bounds -> RangeInferable.createRange(
                monotonic || lower == null ? bounds.first
                        : getRangeLowerBound((DateLiteral) lower, (DateLiteral) bounds.first),
                monotonic || upper == null ? bounds.second
                        : getRangeUpperBound((DateLiteral) upper, (DateLiteral) bounds.second)));
    }

    /** Later input days can map to this output date with an earlier time. */
    default Literal getRangeLowerBound(DateLiteral input, DateLiteral output) {
        if (input.getDay() < input.toJavaDateType().toLocalDate().lengthOfMonth()
                && output.getDay() == output.toJavaDateType().toLocalDate().lengthOfMonth()) {
            return withTime(output, LocalTime.MIN);
        }
        return output;
    }

    /** Earlier input days can map to a clamped output date with a later time. */
    default Literal getRangeUpperBound(DateLiteral input, DateLiteral output) {
        if (output.getDay() < input.getDay()) {
            return withTime(output, LocalTime.MAX);
        }
        return output;
    }

    private static Literal withTime(DateLiteral output, LocalTime time) {
        LocalDateTime dateTime = output.toJavaDateType().with(time);
        if (output instanceof TimeStampNsLiteral) {
            return TimeStampNsLiteral.fromJavaDateType(dateTime);
        }
        if (output instanceof TimestampTzLiteral) {
            return TimestampTzLiteral.fromJavaDateType(dateTime, ((TimestampTzLiteral) output).getScale());
        }
        return DateTimeV2Literal.fromJavaDateType(dateTime, ((DateTimeV2Literal) output).getDataType().getScale());
    }
}

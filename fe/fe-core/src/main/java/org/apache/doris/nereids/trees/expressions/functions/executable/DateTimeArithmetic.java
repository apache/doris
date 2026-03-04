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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Interval.TimeUnit;
import org.apache.doris.nereids.trees.expressions.literal.TimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

/**
 * executable function: date_add/sub, years/quarters/months/week/days/hours/minutes/seconds_add/sub, datediff
 */
public class DateTimeArithmetic {
    private static final long[] POW_10 = { 1, 10, 100, 1000, 10000, 100000, 1000000 };
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long MICROS_PER_MINUTE = Math.multiplyExact(MICROS_PER_SECOND, 60L);
    private static final long MICROS_PER_HOUR = Math.multiplyExact(MICROS_PER_MINUTE, 60L);
    private static final long MICROS_PER_DAY = Math.multiplyExact(MICROS_PER_HOUR, 24L);

    private static long parseYearMonthToMonths(String raw) {
        long[] values = parseIntervalValues(raw, TimeUnit.YEAR_MONTH);
        long months = Math.addExact(Math.multiplyExact(values[0], 12L), values[1]);
        return months;
    }

    private static long parseIntervalToMicros(String raw, TimeUnit unit) {
        if (unit == TimeUnit.YEAR_MONTH) {
            throw new AnalysisException("YEAR_MONTH should be handled via months parsing");
        }
        long[] values = parseIntervalValues(raw, unit);
        try {
            long micros;
            switch (unit) {
                case DAY_HOUR: {
                    long dayMicros = Math.multiplyExact(values[0], MICROS_PER_DAY);
                    long hourMicros = Math.multiplyExact(values[1], MICROS_PER_HOUR);
                    micros = Math.addExact(dayMicros, hourMicros);
                    break;
                }
                case DAY_MINUTE: {
                    long dayMicros = Math.multiplyExact(values[0], MICROS_PER_DAY);
                    long hourMicros = Math.multiplyExact(values[1], MICROS_PER_HOUR);
                    long minuteMicros = Math.multiplyExact(values[2], MICROS_PER_MINUTE);
                    micros = Math.addExact(Math.addExact(dayMicros, hourMicros), minuteMicros);
                    break;
                }
                case DAY_SECOND: {
                    long dayMicros = Math.multiplyExact(values[0], MICROS_PER_DAY);
                    long hourMicros = Math.multiplyExact(values[1], MICROS_PER_HOUR);
                    long minuteMicros = Math.multiplyExact(values[2], MICROS_PER_MINUTE);
                    long secondMicros = Math.multiplyExact(values[3], MICROS_PER_SECOND);
                    micros = Math.addExact(Math.addExact(dayMicros, hourMicros),
                            Math.addExact(minuteMicros, secondMicros));
                    break;
                }
                case DAY_MICROSECOND: {
                    long dayMicros = Math.multiplyExact(values[0], MICROS_PER_DAY);
                    long hourMicros = Math.multiplyExact(values[1], MICROS_PER_HOUR);
                    long minuteMicros = Math.multiplyExact(values[2], MICROS_PER_MINUTE);
                    long secondMicros = Math.multiplyExact(values[3], MICROS_PER_SECOND);
                    micros = Math.addExact(Math.addExact(dayMicros, hourMicros),
                            Math.addExact(Math.addExact(minuteMicros, secondMicros), values[4]));
                    break;
                }
                case HOUR_MINUTE: {
                    long hourMicros = Math.multiplyExact(values[0], MICROS_PER_HOUR);
                    long minuteMicros = Math.multiplyExact(values[1], MICROS_PER_MINUTE);
                    micros = Math.addExact(hourMicros, minuteMicros);
                    break;
                }
                case HOUR_SECOND: {
                    long hourMicros = Math.multiplyExact(values[0], MICROS_PER_HOUR);
                    long minuteMicros = Math.multiplyExact(values[1], MICROS_PER_MINUTE);
                    long secondMicros = Math.multiplyExact(values[2], MICROS_PER_SECOND);
                    micros = Math.addExact(Math.addExact(hourMicros, minuteMicros), secondMicros);
                    break;
                }
                case HOUR_MICROSECOND: {
                    long hourMicros = Math.multiplyExact(values[0], MICROS_PER_HOUR);
                    long minuteMicros = Math.multiplyExact(values[1], MICROS_PER_MINUTE);
                    long secondMicros = Math.multiplyExact(values[2], MICROS_PER_SECOND);
                    micros = Math.addExact(Math.addExact(hourMicros, minuteMicros),
                            Math.addExact(secondMicros, values[3]));
                    break;
                }
                case MINUTE_SECOND: {
                    long minuteMicros = Math.multiplyExact(values[0], MICROS_PER_MINUTE);
                    long secondMicros = Math.multiplyExact(values[1], MICROS_PER_SECOND);
                    micros = Math.addExact(minuteMicros, secondMicros);
                    break;
                }
                case MINUTE_MICROSECOND: {
                    long minuteMicros = Math.multiplyExact(values[0], MICROS_PER_MINUTE);
                    long secondMicros = Math.multiplyExact(values[1], MICROS_PER_SECOND);
                    micros = Math.addExact(Math.addExact(minuteMicros, secondMicros), values[2]);
                    break;
                }
                case SECOND_MICROSECOND: {
                    long secondMicros = Math.multiplyExact(values[0], MICROS_PER_SECOND);
                    micros = Math.addExact(secondMicros, values[1]);
                    break;
                }
                default:
                    throw new AnalysisException("Unsupported time unit: " + unit);
            }
            return micros;
        } catch (ArithmeticException ex) {
            throw new AnalysisException("Interval out of range", ex);
        }
    }

    private static long[] parseIntervalValues(String raw, TimeUnit unit) {
        String str = raw.trim();
        if (str.isEmpty()) {
            throw new AnalysisException("Invalid time format");
        }

        int expectedParts;
        boolean transformMicro = false;
        switch (unit) {
            case YEAR_MONTH:
                expectedParts = 2;
                break;
            case DAY_HOUR:
                expectedParts = 2;
                break;
            case DAY_MINUTE:
                expectedParts = 3;
                break;
            case DAY_SECOND:
                expectedParts = 4;
                break;
            case DAY_MICROSECOND:
                expectedParts = 5;
                transformMicro = true;
                break;
            case HOUR_MINUTE:
                expectedParts = 2;
                break;
            case HOUR_SECOND:
                expectedParts = 3;
                break;
            case HOUR_MICROSECOND:
                expectedParts = 4;
                transformMicro = true;
                break;
            case MINUTE_SECOND:
                expectedParts = 2;
                break;
            case MINUTE_MICROSECOND:
                expectedParts = 3;
                transformMicro = true;
                break;
            case SECOND_MICROSECOND:
                expectedParts = 2;
                transformMicro = true;
                break;
            default:
                throw new AnalysisException("Unsupported time unit: " + unit);
        }

        long[] values = new long[expectedParts];
        boolean negative = false;
        int idx = 0;
        int len = str.length();

        while (idx < len && Character.isWhitespace(str.charAt(idx))) {
            idx++;
        }
        if (idx < len && str.charAt(idx) == '-') {
            negative = true;
            idx++;
        }

        idx = advanceToDigit(str, idx);

        int lastDigits = 0;
        for (int i = 0; i < expectedParts; i++) {
            if (idx >= len || !Character.isDigit(str.charAt(idx))) {
                throw new AnalysisException("Invalid time format");
            }
            long val = 0;
            lastDigits = 0;
            while (idx < len && Character.isDigit(str.charAt(idx))) {
                val = val * 10 + (str.charAt(idx) - '0');
                idx++;
                lastDigits++;
                if (val < 0) {
                    throw new AnalysisException("Invalid time format");
                }
            }
            values[i] = val;

            idx = advanceToDigit(str, idx);
            if (idx == len && i != expectedParts - 1) {
                int filled = i + 1;
                System.arraycopy(values, 0, values, expectedParts - filled, filled);
                Arrays.fill(values, 0, expectedParts - filled, 0L);
                break;
            }
        }

        if (idx != len) {
            throw new AnalysisException("Invalid time format");
        }

        if (transformMicro) {
            int microIndex = expectedParts - 1;
            int digits = lastDigits;
            if (digits > 0 && digits < 6) {
                values[microIndex] = Math.multiplyExact(values[microIndex], POW_10[6 - digits]);
            }
        }

        if (negative) {
            for (int i = 0; i < values.length; i++) {
                values[i] = -values[i];
            }
        }
        return values;
    }

    private static int advanceToDigit(String str, int idx) {
        int len = str.length();
        while (idx < len && !Character.isDigit(str.charAt(idx))) {
            idx++;
        }
        return idx;
    }

    private static Expression applyInterval(DateTimeV2Literal date, VarcharLiteral delta, TimeUnit unit,
            boolean add) {
        if (unit == TimeUnit.YEAR_MONTH) {
            long months = parseYearMonthToMonths(delta.getStringValue());
            months = add ? months : -months;
            return date.plusMonths(months);
        }

        long micros = parseIntervalToMicros(delta.getStringValue(), unit);
        micros = add ? micros : -micros;
        Expression result = date.plusMicroSeconds(micros);
        if (result instanceof DateTimeV2Literal) {
            DateTimeV2Literal dt = (DateTimeV2Literal) result;
            if (dt.getScale() != date.getScale()) {
                // Keep original scale to match existing folding behavior.
                return DateTimeV2Literal.fromJavaDateType(dt.toJavaDateType(), date.getScale());
            }
        }
        return result;
    }

    private static Expression applyInterval(TimestampTzLiteral date, VarcharLiteral delta, TimeUnit unit,
            boolean add) {
        if (unit == TimeUnit.YEAR_MONTH) {
            long months = parseYearMonthToMonths(delta.getStringValue());
            months = add ? months : -months;
            return date.plusMonths(months);
        }

        long micros = parseIntervalToMicros(delta.getStringValue(), unit);
        micros = add ? micros : -micros;
        Expression result = date.plusMicroSeconds(micros);
        if (result instanceof TimestampTzLiteral) {
            TimestampTzLiteral dt = (TimestampTzLiteral) result;
            if (dt.getScale() != date.getScale()) {
                // Keep original scale to match existing folding behavior.
                return TimestampTzLiteral.fromJavaDateType(dt.toJavaDateType(), date.getScale());
            }
        }
        return result;
    }

    /**
     * datetime arithmetic function date-add.
     */
    @ExecFunction(name = "date_add")
    public static Expression dateAdd(DateV2Literal date, IntegerLiteral day) {
        return daysAdd(date, day);
    }

    @ExecFunction(name = "date_add")
    public static Expression dateAdd(DateTimeV2Literal date, IntegerLiteral day) {
        return daysAdd(date, day);
    }

    @ExecFunction(name = "date_add")
    public static Expression dateAdd(TimestampTzLiteral date, IntegerLiteral day) {
        return daysAdd(date, day);
    }

    /**
     * datetime arithmetic function day_hour-add.
     */
    @ExecFunction(name = "day_hour_add")
    public static Expression dayHourAdd(DateTimeV2Literal date, VarcharLiteral dayHour) {
        return applyInterval(date, dayHour, TimeUnit.DAY_HOUR, true);
    }

    @ExecFunction(name = "day_hour_add")
    public static Expression dayHourAdd(TimestampTzLiteral date, VarcharLiteral dayHour) {
        return applyInterval(date, dayHour, TimeUnit.DAY_HOUR, true);
    }

    @ExecFunction(name = "day_hour_sub")
    public static Expression dayHourSub(DateTimeV2Literal date, VarcharLiteral dayHour) {
        return applyInterval(date, dayHour, TimeUnit.DAY_HOUR, false);
    }

    @ExecFunction(name = "day_hour_sub")
    public static Expression dayHourSub(TimestampTzLiteral date, VarcharLiteral dayHour) {
        return applyInterval(date, dayHour, TimeUnit.DAY_HOUR, false);
    }

    /**
     * datetime arithmetic function minute_second-add.
     */
    @ExecFunction(name = "minute_second_add")
    public static Expression minuteSecondAdd(DateTimeV2Literal date, VarcharLiteral minuteSecond) {
        return applyInterval(date, minuteSecond, TimeUnit.MINUTE_SECOND, true);
    }

    @ExecFunction(name = "minute_second_add")
    public static Expression minuteSecondAdd(TimestampTzLiteral date, VarcharLiteral minuteSecond) {
        return applyInterval(date, minuteSecond, TimeUnit.MINUTE_SECOND, true);
    }

    @ExecFunction(name = "minute_second_sub")
    public static Expression minuteSecondSub(DateTimeV2Literal date, VarcharLiteral minuteSecond) {
        return applyInterval(date, minuteSecond, TimeUnit.MINUTE_SECOND, false);
    }

    @ExecFunction(name = "minute_second_sub")
    public static Expression minuteSecondSub(TimestampTzLiteral date, VarcharLiteral minuteSecond) {
        return applyInterval(date, minuteSecond, TimeUnit.MINUTE_SECOND, false);
    }

    /**
     * datetime arithmetic function second_microsecond-add.
     */
    @ExecFunction(name = "second_microsecond_add")
    public static Expression secondMicrosecondAdd(DateTimeV2Literal date, VarcharLiteral secondMicrosecond) {
        return applyInterval(date, secondMicrosecond, TimeUnit.SECOND_MICROSECOND, true);
    }

    @ExecFunction(name = "second_microsecond_add")
    public static Expression secondMicrosecondAdd(TimestampTzLiteral date, VarcharLiteral secondMicrosecond) {
        return applyInterval(date, secondMicrosecond, TimeUnit.SECOND_MICROSECOND, true);
    }

    @ExecFunction(name = "second_microsecond_sub")
    public static Expression secondMicrosecondSub(DateTimeV2Literal date, VarcharLiteral secondMicrosecond) {
        return applyInterval(date, secondMicrosecond, TimeUnit.SECOND_MICROSECOND, false);
    }

    @ExecFunction(name = "second_microsecond_sub")
    public static Expression secondMicrosecondSub(TimestampTzLiteral date, VarcharLiteral secondMicrosecond) {
        return applyInterval(date, secondMicrosecond, TimeUnit.SECOND_MICROSECOND, false);
    }

    /**
     * datetime arithmetic function date-sub.
     */
    @ExecFunction(name = "date_sub")
    public static Expression dateSub(DateV2Literal date, IntegerLiteral day) {
        return dateAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @ExecFunction(name = "date_sub")
    public static Expression dateSub(DateTimeV2Literal date, IntegerLiteral day) {
        return dateAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @ExecFunction(name = "date_sub")
    public static Expression dateSub(TimestampTzLiteral date, IntegerLiteral day) {
        return dateAdd(date, new IntegerLiteral(-day.getValue()));
    }

    /**
     * datetime arithmetic function years-add.
     */
    @ExecFunction(name = "years_add")
    public static Expression yearsAdd(DateV2Literal date, IntegerLiteral year) {
        return date.plusYears(year.getValue());
    }

    @ExecFunction(name = "years_add")
    public static Expression yearsAdd(DateTimeV2Literal date, IntegerLiteral year) {
        return date.plusYears(year.getValue());
    }

    @ExecFunction(name = "years_add")
    public static Expression yearsAdd(TimestampTzLiteral date, IntegerLiteral year) {
        return date.plusYears(year.getValue());
    }

    /**
     * datetime arithmetic function quarters-add.
     */
    @ExecFunction(name = "quarters_add")
    public static Expression quartersAdd(DateV2Literal date, IntegerLiteral quarter) {
        return date.plusMonths(3 * quarter.getValue());
    }

    @ExecFunction(name = "quarters_add")
    public static Expression quartersAdd(DateTimeV2Literal date, IntegerLiteral quarter) {
        return date.plusMonths(3 * quarter.getValue());
    }

    @ExecFunction(name = "quarters_add")
    public static Expression quartersAdd(TimestampTzLiteral date, IntegerLiteral quarter) {
        return date.plusMonths(3 * quarter.getValue());
    }

    /**
     * datetime arithmetic function months-add.
     */
    @ExecFunction(name = "months_add")
    public static Expression monthsAdd(DateV2Literal date, IntegerLiteral month) {
        return date.plusMonths(month.getValue());
    }

    @ExecFunction(name = "months_add")
    public static Expression monthsAdd(DateTimeV2Literal date, IntegerLiteral month) {
        return date.plusMonths(month.getValue());
    }

    @ExecFunction(name = "months_add")
    public static Expression monthsAdd(TimestampTzLiteral date, IntegerLiteral month) {
        return date.plusMonths(month.getValue());
    }

    /**
     * datetime arithmetic function weeks-add.
     */
    @ExecFunction(name = "weeks_add")
    public static Expression weeksAdd(DateV2Literal date, IntegerLiteral weeks) {
        return date.plusWeeks(weeks.getValue());
    }

    @ExecFunction(name = "weeks_add")
    public static Expression weeksAdd(DateTimeV2Literal date, IntegerLiteral weeks) {
        return date.plusWeeks(weeks.getValue());
    }

    @ExecFunction(name = "weeks_add")
    public static Expression weeksAdd(TimestampTzLiteral date, IntegerLiteral weeks) {
        return date.plusWeeks(weeks.getValue());
    }

    /**
     * datetime arithmetic function days-add.
     */
    @ExecFunction(name = "days_add")
    public static Expression daysAdd(DateV2Literal date, IntegerLiteral day) {
        return date.plusDays(day.getValue());
    }

    @ExecFunction(name = "days_add")
    public static Expression daysAdd(DateTimeV2Literal date, IntegerLiteral day) {
        return date.plusDays(day.getValue());
    }

    @ExecFunction(name = "days_add")
    public static Expression daysAdd(TimestampTzLiteral date, IntegerLiteral day) {
        return date.plusDays(day.getValue());
    }

    /**
     * datetime arithmetic function day_second-add.
     */
    @ExecFunction(name = "day_second_add")
    public static Expression daysAdd(DateTimeV2Literal date, VarcharLiteral daySecond) {
        return applyInterval(date, daySecond, TimeUnit.DAY_SECOND, true);
    }

    @ExecFunction(name = "day_second_add")
    public static Expression daysAdd(TimestampTzLiteral date, VarcharLiteral daySecond) {
        return applyInterval(date, daySecond, TimeUnit.DAY_SECOND, true);
    }

    @ExecFunction(name = "day_second_sub")
    public static Expression daysSub(DateTimeV2Literal date, VarcharLiteral daySecond) {
        return applyInterval(date, daySecond, TimeUnit.DAY_SECOND, false);
    }

    @ExecFunction(name = "day_second_sub")
    public static Expression daysSub(TimestampTzLiteral date, VarcharLiteral daySecond) {
        return applyInterval(date, daySecond, TimeUnit.DAY_SECOND, false);
    }

    /**
     * datetime arithmetic function days-sub
     */
    @ExecFunction(name = "days_sub")
    public static Expression daysSub(DateV2Literal date, IntegerLiteral day) {
        return daysAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @ExecFunction(name = "days_sub")
    public static Expression daysSub(DateTimeV2Literal date, IntegerLiteral day) {
        return daysAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @ExecFunction(name = "days_sub")
    public static Expression daysSub(TimestampTzLiteral date, IntegerLiteral day) {
        return daysAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @ExecFunction(name = "day_minute_add")
    public static Expression dayMinuteAdd(DateTimeV2Literal date, VarcharLiteral dayMinute) {
        return applyInterval(date, dayMinute, TimeUnit.DAY_MINUTE, true);
    }

    @ExecFunction(name = "day_minute_add")
    public static Expression dayMinuteAdd(TimestampTzLiteral date, VarcharLiteral dayMinute) {
        return applyInterval(date, dayMinute, TimeUnit.DAY_MINUTE, true);
    }

    @ExecFunction(name = "day_minute_sub")
    public static Expression dayMinuteSub(DateTimeV2Literal date, VarcharLiteral dayMinute) {
        return applyInterval(date, dayMinute, TimeUnit.DAY_MINUTE, false);
    }

    @ExecFunction(name = "day_minute_sub")
    public static Expression dayMinuteSub(TimestampTzLiteral date, VarcharLiteral dayMinute) {
        return applyInterval(date, dayMinute, TimeUnit.DAY_MINUTE, false);
    }

    @ExecFunction(name = "day_microsecond_add")
    public static Expression dayMicrosecondAdd(DateTimeV2Literal date, VarcharLiteral dayMicrosecond) {
        return applyInterval(date, dayMicrosecond, TimeUnit.DAY_MICROSECOND, true);
    }

    @ExecFunction(name = "day_microsecond_add")
    public static Expression dayMicrosecondAdd(TimestampTzLiteral date, VarcharLiteral dayMicrosecond) {
        return applyInterval(date, dayMicrosecond, TimeUnit.DAY_MICROSECOND, true);
    }

    @ExecFunction(name = "day_microsecond_sub")
    public static Expression dayMicrosecondSub(DateTimeV2Literal date, VarcharLiteral dayMicrosecond) {
        return applyInterval(date, dayMicrosecond, TimeUnit.DAY_MICROSECOND, false);
    }

    @ExecFunction(name = "day_microsecond_sub")
    public static Expression dayMicrosecondSub(TimestampTzLiteral date, VarcharLiteral dayMicrosecond) {
        return applyInterval(date, dayMicrosecond, TimeUnit.DAY_MICROSECOND, false);
    }

    @ExecFunction(name = "hour_minute_add")
    public static Expression hourMinuteAdd(DateTimeV2Literal date, VarcharLiteral hourMinute) {
        return applyInterval(date, hourMinute, TimeUnit.HOUR_MINUTE, true);
    }

    @ExecFunction(name = "hour_minute_add")
    public static Expression hourMinuteAdd(TimestampTzLiteral date, VarcharLiteral hourMinute) {
        return applyInterval(date, hourMinute, TimeUnit.HOUR_MINUTE, true);
    }

    @ExecFunction(name = "hour_minute_sub")
    public static Expression hourMinuteSub(DateTimeV2Literal date, VarcharLiteral hourMinute) {
        return applyInterval(date, hourMinute, TimeUnit.HOUR_MINUTE, false);
    }

    @ExecFunction(name = "hour_minute_sub")
    public static Expression hourMinuteSub(TimestampTzLiteral date, VarcharLiteral hourMinute) {
        return applyInterval(date, hourMinute, TimeUnit.HOUR_MINUTE, false);
    }

    @ExecFunction(name = "hour_second_add")
    public static Expression hourSecondAdd(DateTimeV2Literal date, VarcharLiteral hourSecond) {
        return applyInterval(date, hourSecond, TimeUnit.HOUR_SECOND, true);
    }

    @ExecFunction(name = "hour_second_add")
    public static Expression hourSecondAdd(TimestampTzLiteral date, VarcharLiteral hourSecond) {
        return applyInterval(date, hourSecond, TimeUnit.HOUR_SECOND, true);
    }

    @ExecFunction(name = "hour_second_sub")
    public static Expression hourSecondSub(DateTimeV2Literal date, VarcharLiteral hourSecond) {
        return applyInterval(date, hourSecond, TimeUnit.HOUR_SECOND, false);
    }

    @ExecFunction(name = "hour_second_sub")
    public static Expression hourSecondSub(TimestampTzLiteral date, VarcharLiteral hourSecond) {
        return applyInterval(date, hourSecond, TimeUnit.HOUR_SECOND, false);
    }

    @ExecFunction(name = "hour_microsecond_add")
    public static Expression hourMicrosecondAdd(DateTimeV2Literal date, VarcharLiteral hourMicrosecond) {
        return applyInterval(date, hourMicrosecond, TimeUnit.HOUR_MICROSECOND, true);
    }

    @ExecFunction(name = "hour_microsecond_add")
    public static Expression hourMicrosecondAdd(TimestampTzLiteral date, VarcharLiteral hourMicrosecond) {
        return applyInterval(date, hourMicrosecond, TimeUnit.HOUR_MICROSECOND, true);
    }

    @ExecFunction(name = "hour_microsecond_sub")
    public static Expression hourMicrosecondSub(DateTimeV2Literal date, VarcharLiteral hourMicrosecond) {
        return applyInterval(date, hourMicrosecond, TimeUnit.HOUR_MICROSECOND, false);
    }

    @ExecFunction(name = "hour_microsecond_sub")
    public static Expression hourMicrosecondSub(TimestampTzLiteral date, VarcharLiteral hourMicrosecond) {
        return applyInterval(date, hourMicrosecond, TimeUnit.HOUR_MICROSECOND, false);
    }

    @ExecFunction(name = "minute_microsecond_add")
    public static Expression minuteMicrosecondAdd(DateTimeV2Literal date, VarcharLiteral minuteMicrosecond) {
        return applyInterval(date, minuteMicrosecond, TimeUnit.MINUTE_MICROSECOND, true);
    }

    @ExecFunction(name = "minute_microsecond_add")
    public static Expression minuteMicrosecondAdd(TimestampTzLiteral date, VarcharLiteral minuteMicrosecond) {
        return applyInterval(date, minuteMicrosecond, TimeUnit.MINUTE_MICROSECOND, true);
    }

    @ExecFunction(name = "minute_microsecond_sub")
    public static Expression minuteMicrosecondSub(DateTimeV2Literal date, VarcharLiteral minuteMicrosecond) {
        return applyInterval(date, minuteMicrosecond, TimeUnit.MINUTE_MICROSECOND, false);
    }

    @ExecFunction(name = "minute_microsecond_sub")
    public static Expression minuteMicrosecondSub(TimestampTzLiteral date, VarcharLiteral minuteMicrosecond) {
        return applyInterval(date, minuteMicrosecond, TimeUnit.MINUTE_MICROSECOND, false);
    }

    @ExecFunction(name = "year_month_add")
    public static Expression yearMonthAdd(DateTimeV2Literal date, VarcharLiteral yearMonth) {
        return applyInterval(date, yearMonth, TimeUnit.YEAR_MONTH, true);
    }

    @ExecFunction(name = "year_month_add")
    public static Expression yearMonthAdd(TimestampTzLiteral date, VarcharLiteral yearMonth) {
        return applyInterval(date, yearMonth, TimeUnit.YEAR_MONTH, true);
    }

    @ExecFunction(name = "year_month_sub")
    public static Expression yearMonthSub(DateTimeV2Literal date, VarcharLiteral yearMonth) {
        return applyInterval(date, yearMonth, TimeUnit.YEAR_MONTH, false);
    }

    @ExecFunction(name = "year_month_sub")
    public static Expression yearMonthSub(TimestampTzLiteral date, VarcharLiteral yearMonth) {
        return applyInterval(date, yearMonth, TimeUnit.YEAR_MONTH, false);
    }

    /**
     * datetime arithmetic function hours-add.
     */
    @ExecFunction(name = "hours_add")
    public static Expression hoursAdd(DateTimeV2Literal date, IntegerLiteral hour) {
        return date.plusHours(hour.getValue());
    }

    @ExecFunction(name = "hours_add")
    public static Expression hoursAdd(TimestampTzLiteral date, IntegerLiteral hour) {
        return date.plusHours(hour.getValue());
    }

    /**
     * datetime arithmetic function minutes-add.
     */
    @ExecFunction(name = "minutes_add")
    public static Expression minutesAdd(DateTimeV2Literal date, BigIntLiteral minute) {
        return date.plusMinutes(minute.getValue());
    }

    @ExecFunction(name = "minutes_add")
    public static Expression minutesAdd(TimestampTzLiteral date, BigIntLiteral minute) {
        return date.plusMinutes(minute.getValue());
    }

    /**
     * datetime arithmetic function seconds-add.
     */
    @ExecFunction(name = "seconds_add")
    public static Expression secondsAdd(DateTimeV2Literal date, BigIntLiteral second) {
        return date.plusSeconds(second.getValue());
    }

    @ExecFunction(name = "seconds_add")
    public static Expression secondsAdd(TimestampTzLiteral date, BigIntLiteral second) {
        return date.plusSeconds(second.getValue());
    }

    /**
     * datetime arithmetic function microseconds-add.
     */
    @ExecFunction(name = "microseconds_add")
    public static Expression microSecondsAdd(DateTimeV2Literal date, BigIntLiteral microSecond) {
        return date.plusMicroSeconds(microSecond.getValue());
    }

    @ExecFunction(name = "microseconds_add")
    public static Expression microSecondsAdd(TimestampTzLiteral date, BigIntLiteral microSecond) {
        return date.plusMicroSeconds(microSecond.getValue());
    }

    /**
     * datetime arithmetic function years-sub.
     */
    @ExecFunction(name = "years_sub")
    public static Expression yearsSub(DateV2Literal date, IntegerLiteral year) {
        return yearsAdd(date, new IntegerLiteral(-year.getValue()));
    }

    @ExecFunction(name = "years_sub")
    public static Expression yearsSub(DateTimeV2Literal date, IntegerLiteral year) {
        return yearsAdd(date, new IntegerLiteral(-year.getValue()));
    }

    @ExecFunction(name = "years_sub")
    public static Expression yearsSub(TimestampTzLiteral date, IntegerLiteral year) {
        return yearsAdd(date, new IntegerLiteral(-year.getValue()));
    }

    /**
     * datetime arithmetic function quarters-sub.
     */
    @ExecFunction(name = "quarters_sub")
    public static Expression quartersSub(DateV2Literal date, IntegerLiteral quarter) {
        return quartersAdd(date, new IntegerLiteral(-quarter.getValue()));
    }

    @ExecFunction(name = "quarters_sub")
    public static Expression quartersSub(DateTimeV2Literal date, IntegerLiteral quarter) {
        return quartersAdd(date, new IntegerLiteral(-quarter.getValue()));
    }

    @ExecFunction(name = "quarters_sub")
    public static Expression quartersSub(TimestampTzLiteral date, IntegerLiteral quarter) {
        return quartersAdd(date, new IntegerLiteral(-quarter.getValue()));
    }

    /**
     * datetime arithmetic function months-sub
     */
    @ExecFunction(name = "months_sub")
    public static Expression monthsSub(DateV2Literal date, IntegerLiteral month) {
        return monthsAdd(date, new IntegerLiteral(-month.getValue()));
    }

    @ExecFunction(name = "months_sub")
    public static Expression monthsSub(DateTimeV2Literal date, IntegerLiteral month) {
        return monthsAdd(date, new IntegerLiteral(-month.getValue()));
    }

    @ExecFunction(name = "months_sub")
    public static Expression monthsSub(TimestampTzLiteral date, IntegerLiteral month) {
        return monthsAdd(date, new IntegerLiteral(-month.getValue()));
    }

    /**
     * datetime arithmetic function weeks-sub.
     */
    @ExecFunction(name = "weeks_sub")
    public static Expression weeksSub(DateV2Literal date, IntegerLiteral weeks) {
        return date.plusWeeks(-weeks.getValue());
    }

    @ExecFunction(name = "weeks_sub")
    public static Expression weeksSub(DateTimeV2Literal date, IntegerLiteral weeks) {
        return date.plusWeeks(-weeks.getValue());
    }

    @ExecFunction(name = "weeks_sub")
    public static Expression weeksSub(TimestampTzLiteral date, IntegerLiteral weeks) {
        return date.plusWeeks(-weeks.getValue());
    }

    /**
     * datetime arithmetic function hours-sub
     */
    @ExecFunction(name = "hours_sub")
    public static Expression hoursSub(DateTimeV2Literal date, IntegerLiteral hour) {
        return hoursAdd(date, new IntegerLiteral(-hour.getValue()));
    }

    @ExecFunction(name = "hours_sub")
    public static Expression hoursSub(TimestampTzLiteral date, IntegerLiteral hour) {
        return hoursAdd(date, new IntegerLiteral(-hour.getValue()));
    }

    /**
     * datetime arithmetic function minutes-sub
     */
    @ExecFunction(name = "minutes_sub")
    public static Expression minutesSub(DateTimeV2Literal date, BigIntLiteral minute) {
        return minutesAdd(date, new BigIntLiteral(-minute.getValue()));
    }

    @ExecFunction(name = "minutes_sub")
    public static Expression minutesSub(TimestampTzLiteral date, BigIntLiteral minute) {
        return minutesAdd(date, new BigIntLiteral(-minute.getValue()));
    }

    /**
     * datetime arithmetic function seconds-sub
     */
    @ExecFunction(name = "seconds_sub")
    public static Expression secondsSub(DateTimeV2Literal date, BigIntLiteral second) {
        return secondsAdd(date, new BigIntLiteral(-second.getValue()));
    }

    @ExecFunction(name = "seconds_sub")
    public static Expression secondsSub(TimestampTzLiteral date, BigIntLiteral second) {
        return secondsAdd(date, new BigIntLiteral(-second.getValue()));
    }

    /**
     * datetime arithmetic function microseconds_sub.
     */
    @ExecFunction(name = "microseconds_sub")
    public static Expression microSecondsSub(DateTimeV2Literal date, BigIntLiteral microSecond) {
        return date.plusMicroSeconds(-microSecond.getValue());
    }

    @ExecFunction(name = "microseconds_sub")
    public static Expression microSecondsSub(TimestampTzLiteral date, BigIntLiteral microSecond) {
        return date.plusMicroSeconds(-microSecond.getValue());
    }

    /**
     * datetime arithmetic function milliseconds_add.
     */
    @ExecFunction(name = "milliseconds_add")
    public static Expression milliSecondsAdd(DateTimeV2Literal date, BigIntLiteral milliSecond) {
        return date.plusMilliSeconds(milliSecond.getValue());
    }

    @ExecFunction(name = "milliseconds_add")
    public static Expression milliSecondsAdd(TimestampTzLiteral date, BigIntLiteral milliSecond) {
        return date.plusMilliSeconds(milliSecond.getValue());
    }

    /**
     * datetime arithmetic function milliseconds_sub.
     */
    @ExecFunction(name = "milliseconds_sub")
    public static Expression milliSecondsSub(DateTimeV2Literal date, BigIntLiteral milliSecond) {
        return date.plusMilliSeconds(-milliSecond.getValue());
    }

    @ExecFunction(name = "milliseconds_sub")
    public static Expression milliSecondsSub(TimestampTzLiteral date, BigIntLiteral milliSecond) {
        return date.plusMilliSeconds(-milliSecond.getValue());
    }

    /**
     * datetime arithmetic function datediff
     */
    @ExecFunction(name = "datediff")
    public static Expression dateDiff(DateV2Literal date1, DateV2Literal date2) {
        return new IntegerLiteral(dateDiff(date1.toJavaDateType(), date2.toJavaDateType()));
    }

    @ExecFunction(name = "datediff")
    public static Expression dateDiff(DateV2Literal date1, DateTimeV2Literal date2) {
        return new IntegerLiteral(dateDiff(date1.toJavaDateType(), date2.toJavaDateType()));
    }

    @ExecFunction(name = "datediff")
    public static Expression dateDiff(DateTimeV2Literal date1, DateV2Literal date2) {
        return new IntegerLiteral(dateDiff(date1.toJavaDateType(), date2.toJavaDateType()));
    }

    @ExecFunction(name = "datediff")
    public static Expression dateDiff(DateTimeV2Literal date1, DateTimeV2Literal date2) {
        return new IntegerLiteral(dateDiff(date1.toJavaDateType(), date2.toJavaDateType()));
    }

    private static int dateDiff(LocalDateTime date1, LocalDateTime date2) {
        return ((int) ChronoUnit.DAYS.between(date2.toLocalDate(), date1.toLocalDate()));
    }

    @ExecFunction(name = "to_days")
    public static Expression toDays(DateV2Literal date) {
        return new IntegerLiteral((int) date.getDay());
    }

    @ExecFunction(name = "to_days")
    public static Expression toDays(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getDay()));
    }

    /**
     * datetime arithmetic function time.
     */
    @ExecFunction(name = "time")
    public static Expression time(DateTimeV2Literal date) {
        return new TimeV2Literal((int) date.getHour(), (int) date.getMinute(), (int) date.getSecond(),
                (int) date.getMicroSecond(), (int) date.getScale(), false);
    }

    /**
     * datetime arithmetic function add_time.
     */
    @ExecFunction(name = "add_time")
    public static Expression addTime(DateTimeV2Literal date, TimeV2Literal time) {
        return date.plusMicroSeconds((long) time.getValue());
    }

    @ExecFunction(name = "add_time")
    public static Expression addTime(TimeV2Literal t1, TimeV2Literal t2) {
        return t1.plusMicroSeconds((long) t2.getValue());
    }

    @ExecFunction(name = "add_time")
    public static Expression addTime(TimestampTzLiteral date, TimeV2Literal time) {
        return date.plusMicroSeconds((long) time.getValue());
    }

    /**
     * datetime arithmetic function sub_time.
     */
    @ExecFunction(name = "sub_time")
    public static Expression subTime(DateTimeV2Literal date, TimeV2Literal time) {
        return date.plusMicroSeconds(-(long) time.getValue());
    }

    @ExecFunction(name = "sub_time")
    public static Expression subTime(TimeV2Literal t1, TimeV2Literal t2) {
        return t1.plusMicroSeconds(-(long) t2.getValue());
    }

    @ExecFunction(name = "sub_time")
    public static Expression subTime(TimestampTzLiteral date, TimeV2Literal time) {
        return date.plusMicroSeconds(-(long) time.getValue());
    }
}

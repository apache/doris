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

import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.util.DateUtils;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;

/**
 * executable functions:
 * unclassified date function
 */
public class DateTimeAcquire {
    /**
     * date acquire function: now
     */
    @ExecFunction(name = "now")
    public static Expression now() {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), 0);
    }

    @ExecFunction(name = "now")
    public static Expression now(IntegerLiteral precision) {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()),
                precision.getValue());
    }

    /**
     * date acquire function: current_timestamp
     */
    @ExecFunction(name = "current_timestamp")
    public static Expression currentTimestamp() {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), 0);
    }

    @ExecFunction(name = "current_timestamp")
    public static Expression currentTimestamp(IntegerLiteral precision) {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), precision.getValue());
    }

    /**
     * date acquire function: localtime/localtimestamp
     */
    @ExecFunction(name = "localtime")
    public static Expression localTime() {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), 0);
    }

    @ExecFunction(name = "localtimestamp")
    public static Expression localTimestamp() {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), 0);
    }

    /**
     * date acquire function: current_date
     */
    @ExecFunction(name = "curdate")
    public static Expression curDate() {
        return DateV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    @ExecFunction(name = "current_date")
    public static Expression currentDate() {
        return DateV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    /**
     * date acquire function: current_time
     */
    @ExecFunction(name = "curtime")
    public static Expression curTime() {
        return TimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    @ExecFunction(name = "curtime")
    public static Expression curTime(TinyIntLiteral precision) {
        return TimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), precision.getValue());
    }

    @ExecFunction(name = "current_time")
    public static Expression currentTime() {
        return TimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    @ExecFunction(name = "current_time")
    public static Expression currentTime(TinyIntLiteral precision) {
        return TimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), precision.getValue());
    }

    /**
     * date acquire function: sysdate
     */
    @ExecFunction(name = "sysdate")
    public static Expression sysDate() {
        Calendar cl = Calendar.getInstance();
        Long clTemp = Long.valueOf(cl.getTimeInMillis());
        cl.setTimeInMillis(clTemp.longValue());
        Date date = cl.getTime();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String result = format.format(date);
        return new StringLiteral(result);
    }

    /**
     * date acquire function: sysdate
     */
    @ExecFunction(name = "sysdate")
    public static Expression sysDate(IntegerLiteral arg) {
        Calendar cl = Calendar.getInstance();
        Long clTemp = Long.valueOf(cl.getTimeInMillis() + arg.getLongValue() * 24L * 60L * 60L * 1000L);
        if (clTemp < 0L) {
            clTemp = 0L;
        }
        cl.setTimeInMillis(clTemp.longValue());
        Date date = cl.getTime();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String result = format.format(date);
        return new StringLiteral(result);
    }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(name = "unix_timestamp")
    public static Expression unixTimestamp() {
        return new BigIntLiteral((int) (System.currentTimeMillis() / 1000L));
    }

    /**
     * date transformation function: utc_timestamp
     */
    @ExecFunction(name = "utc_timestamp")
    public static Expression utcTimestamp() {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(ZoneId.of("UTC+0")), 0);
    }

    @ExecFunction(name = "week_and_year")
    public static Expression weekAndYear(StringLiteral dateStr) {
        return weekAndYear(dateStr, new StringLiteral("%s年第%s周"), new IntegerLiteral(2), new IntegerLiteral(4));
    }

    @ExecFunction(name = "week_and_year")
    public static Expression weekAndYear(StringLiteral dateStr, StringLiteral formatStr) {
        return weekAndYear(dateStr, formatStr, new IntegerLiteral(2), new IntegerLiteral(4));
    }

    @ExecFunction(name = "week_and_year")
    public static Expression weekAndYear(StringLiteral dateStr, StringLiteral formatStr, IntegerLiteral firstDay) {
        return weekAndYear(dateStr, formatStr, firstDay, new IntegerLiteral(4));
    }

    /** week_and_year function with four arguments. */
    @ExecFunction(name = "week_and_year")
    public static Expression weekAndYear(StringLiteral dateStr, StringLiteral formatStr,
                                         IntegerLiteral firstDay, IntegerLiteral minDays) {
        try {
            String dateString = dateStr.getStringValue();
            String format = formatStr.getStringValue();
            int firstDayOfWeek = firstDay.getValue();
            int minimalDays = minDays.getValue();

            if (firstDayOfWeek < 1 || firstDayOfWeek > 7) {
                firstDayOfWeek = 2; // default to Monday (matches BE)
            }
            if (minimalDays < 1 || minimalDays > 7) {
                minimalDays = 4;
            }

            Calendar calendar = Calendar.getInstance();
            calendar.setFirstDayOfWeek(firstDayOfWeek);
            calendar.setMinimalDaysInFirstWeek(minimalDays);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = sdf.parse(dateString);
            calendar.setTime(date);

            int week = calendar.get(Calendar.WEEK_OF_YEAR);
            int month = calendar.get(Calendar.MONTH);
            int year = calendar.get(Calendar.YEAR);

            // Year adjustment: if January and week >= 52, subtract 1 from year
            // If December and week == 1, add 1 to year
            if (month == Calendar.JANUARY && week >= 52) {
                year--;
            } else if (month == Calendar.DECEMBER && week == 1) {
                year++;
            }

            String weekStr = week < 10 ? "0" + week : String.valueOf(week);
            String result = format.replaceFirst("%s", String.valueOf(year))
                                  .replaceFirst("%s", weekStr);
            return new StringLiteral(result);
        } catch (Exception e) {
            throw new RuntimeException("Error calculating week and year: " + e.getMessage(), e);
        }
    }
}

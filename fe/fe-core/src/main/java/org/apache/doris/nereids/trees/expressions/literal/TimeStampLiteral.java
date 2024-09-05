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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.TimeStampType;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.nereids.util.StandardDateFormat;

import com.google.common.base.Preconditions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * timestamp literal for nereids
 */
public class TimeStampLiteral extends DateTimeLiteral {

    public TimeStampLiteral(String s) {
        this(TimeStampType.forTypeFromString(s), s);
    }

    public TimeStampLiteral(TimeStampType dateType, String s) {
        super(dateType, s);
        roundMicroSecond(dateType.getScale());
    }

    public TimeStampLiteral(long year, long month, long day, long hour, long minute, long second) {
        super(TimeStampType.SYSTEM_DEFAULT, year, month, day, hour, minute, second, 0);
    }

    public TimeStampLiteral(long year, long month, long day, long hour, long minute, long second, long microSecond) {
        super(TimeStampType.SYSTEM_DEFAULT, year, month, day, hour, minute, second, microSecond);
    }

    public TimeStampLiteral(TimeStampType dateType,
                             long year, long month, long day, long hour, long minute, long second, long microSecond) {
        super(dateType, year, month, day, hour, minute, second, microSecond);
        roundMicroSecond(dateType.getScale());
    }

    private void roundMicroSecond(int scale) {
        Preconditions.checkArgument(scale >= 0 && scale <= TimeStampType.MAX_SCALE,
                "invalid timestamp scale: %s", scale);
        double factor = Math.pow(10, 6 - scale);

        this.microSecond = Math.round(this.microSecond / factor) * (int) factor;

        if (this.microSecond >= 1000000) {
            LocalDateTime localDateTime = DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER_TO_MICRO_SECOND,
                    getStringValue()).plusSeconds(1);
            this.year = localDateTime.getYear();
            this.month = localDateTime.getMonthValue();
            this.day = localDateTime.getDayOfMonth();
            this.hour = localDateTime.getHour();
            this.minute = localDateTime.getMinute();
            this.second = localDateTime.getSecond();
            this.microSecond -= 1000000;
        }

        if (!hasZone) {
            Instant thatTime = ZonedDateTime
                    .of((int) year, (int) month, (int) day, (int) hour, (int) minute, (int) second,
                            (int) microSecond, ZoneId.systemDefault())
                    .toInstant();
            int offset = DateUtils.getTimeZone().getRules().getOffset(thatTime).getTotalSeconds()
                    - ZoneId.systemDefault().getRules().getOffset(thatTime).getTotalSeconds();

            if (offset != 0) {
                DateTimeLiteral result = (DateTimeLiteral) this.plusSeconds(offset);
                this.second = result.second;
                this.minute = result.minute;
                this.hour = result.hour;
                this.day = result.day;
                this.month = result.month;
                this.year = result.year;
            }
        }

        if (checkRange() || checkDate()) {
            // may fallback to legacy planner. make sure the behaviour of rounding is same.
            throw new AnalysisException("timestamp literal [" + toString() + "] is out of range");
        }
    }

    public String getFullMicroSecondValue() {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d",
            year, month, day, hour, minute, second, microSecond);
    }

    @Override
    public TimeStampType getDataType() throws UnboundException {
        return (TimeStampType) super.getDataType();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTimeStampLiteral(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.DateLiteral(year, month, day, hour, minute, second, microSecond,
            getDataType().toCatalogDataType());
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    @Override
    public String getStringValue() {
        int scale = getDataType().getScale();
        if (scale <= 0) {
            return super.getStringValue();
        }

        if (0 <= year && year <= 9999 && 0 <= month && month <= 99 && 0 <= day && day <= 99
                && 0 <= hour && hour <= 99 && 0 <= minute && minute <= 99 && 0 <= second && second <= 99
                && 0 <= microSecond && microSecond <= MAX_MICROSECOND) {
            char[] format = new char[] {
                '0', '0', '0', '0', '-', '0', '0', '-', '0', '0', ' ', '0', '0', ':', '0', '0', ':', '0', '0',
                '.', '0', '0', '0', '0', '0', '0'};
            int offset = 3;
            long year = this.year;
            while (year > 0) {
                format[offset--] = (char) ('0' + (year % 10));
                year /= 10;
            }

            offset = 6;
            long month = this.month;
            while (month > 0) {
                format[offset--] = (char) ('0' + (month % 10));
                month /= 10;
            }

            offset = 9;
            long day = this.day;
            while (day > 0) {
                format[offset--] = (char) ('0' + (day % 10));
                day /= 10;
            }

            offset = 12;
            long hour = this.hour;
            while (hour > 0) {
                format[offset--] = (char) ('0' + (hour % 10));
                hour /= 10;
            }

            offset = 15;
            long minute = this.minute;
            while (minute > 0) {
                format[offset--] = (char) ('0' + (minute % 10));
                minute /= 10;
            }

            offset = 18;
            long second = this.second;
            while (second > 0) {
                format[offset--] = (char) ('0' + (second % 10));
                second /= 10;
            }

            offset = 19 + scale;
            long microSecond = (int) (this.microSecond / Math.pow(10, TimeStampType.MAX_SCALE - scale));
            while (microSecond > 0) {
                format[offset--] = (char) ('0' + (microSecond % 10));
                microSecond /= 10;
            }
            return String.valueOf(format, 0, 20 + scale);
        }

        return String.format("%04d-%02d-%02d %02d:%02d:%02d"
                + (scale > 0 ? ".%0" + scale + "d" : ""),
            year, month, day, hour, minute, second,
            (int) (microSecond / Math.pow(10, TimeStampType.MAX_SCALE - scale)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TimeStampLiteral literal = (TimeStampLiteral) o;
        return Objects.equals(dataType, literal.dataType);
    }
}

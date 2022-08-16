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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.util.DateUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;

/**
 * date time literal.
 */
public class DateTimeLiteral extends DateLiteral {
    private static final Logger LOG = LogManager.getLogger(DateTimeLiteral.class);

    private static final int DATETIME_TO_MINUTE_STRING_LENGTH = 16;
    private static final int DATETIME_TO_HOUR_STRING_LENGTH = 13;

    private static DateTimeFormatter DATE_TIME_FORMATTER = null;
    private static DateTimeFormatter DATE_TIME_FORMATTER_TO_HOUR = null;
    private static DateTimeFormatter DATE_TIME_FORMATTER_TO_MINUTE = null;
    private static DateTimeFormatter DATE_TIME_FORMATTER_TWO_DIGIT = null;

    private long hour;
    private long minute;
    private long second;

    static {
        try {
            DATE_TIME_FORMATTER = DateUtils.formatBuilder("%Y-%m-%d %H:%i:%s").toFormatter();
            DATE_TIME_FORMATTER_TO_HOUR = DateUtils.formatBuilder("%Y-%m-%d %H").toFormatter();
            DATE_TIME_FORMATTER_TO_MINUTE = DateUtils.formatBuilder("%Y-%m-%d %H:%i").toFormatter();
            DATE_TIME_FORMATTER_TWO_DIGIT = DateUtils.formatBuilder("%y-%m-%d %H:%i:%s").toFormatter();
        } catch (AnalysisException e) {
            LOG.error("invalid date format", e);
            System.exit(-1);
        }
    }

    public DateTimeLiteral(String s) {
        super(DateTimeType.INSTANCE);
        init(s);
    }

    /**
     * C'tor data time literal.
     */
    public DateTimeLiteral(long year, long month, long day, long hour, long minute, long second) {
        super(DateTimeType.INSTANCE);
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.year = year;
        this.month = month;
        this.day = day;
    }

    private void init(String s) throws AnalysisException {
        try {
            LocalDateTime dateTime;
            if (s.split("-")[0].length() == 2) {
                dateTime = DATE_TIME_FORMATTER_TWO_DIGIT.parseLocalDateTime(s);
            } else {
                if (s.length() == DATETIME_TO_MINUTE_STRING_LENGTH) {
                    dateTime = DATE_TIME_FORMATTER_TO_MINUTE.parseLocalDateTime(s);
                } else if (s.length() == DATETIME_TO_HOUR_STRING_LENGTH) {
                    dateTime = DATE_TIME_FORMATTER_TO_HOUR.parseLocalDateTime(s);
                } else {
                    dateTime = DATE_TIME_FORMATTER.parseLocalDateTime(s);
                }
            }
            year = dateTime.getYear();
            month = dateTime.getMonthOfYear();
            day = dateTime.getDayOfMonth();
            hour = dateTime.getHourOfDay();
            minute = dateTime.getMinuteOfHour();
            second = dateTime.getSecondOfMinute();
        } catch (Exception ex) {
            throw new AnalysisException("date time literal [" + s + "] is invalid");
        }
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (getDataType().equals(targetType)) {
            return this;
        }
        if (targetType.isDate()) {
            if (getDataType().equals(targetType)) {
                return this;
            }
            if (targetType.equals(DateType.INSTANCE)) {
                return new DateLiteral(this.year, this.month, this.day);
            } else if (targetType.equals(DateTimeType.INSTANCE)) {
                return new DateTimeLiteral(this.year, this.month, this.day, this.hour, this.minute, this.second);
            } else {
                throw new AnalysisException("Error date literal type");
            }
        }
        //todo other target type cast
        return this;
    }

    @Override
    public Long getValue() {
        return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
    }

    @Override
    public String toSql() {
        return toString();
    }

    @Override
    public String toString() {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
    }

    public long getHour() {
        return hour;
    }

    public long getMinute() {
        return minute;
    }

    public long getSecond() {
        return second;
    }
}

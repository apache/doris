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
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.DateUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormatter;

import java.util.Objects;

/**
 * time stamp literal
 */
public class TimeStampLiteral extends DateLiteral {
    private static final Logger LOG = LogManager.getLogger(TimeStampLiteral.class);
    private static DateTimeFormatter TIMESTAMP_FORMATTER = null;
    private final Instant timeStamp;

    static {
        try {
            TIMESTAMP_FORMATTER = DateUtils.formatBuilder("%Y-%m-%d %H:%i:%s").toFormatter();
        } catch (AnalysisException e) {
            LOG.error("invalid date format", e);
            System.exit(-1);
        }
    }

    public TimeStampLiteral(String timeString) {
        this(DateTimeType.INSTANCE, timeString);
    }

    public TimeStampLiteral(DateLikeType type, String timeString) {
        super(type);
        init(timeString);
        timeStamp = checkAndMaybeSetValue(timeString);
    }

    public TimeStampLiteral(long year, long month, long day, long hour, long minute, long second) {
        this(DateTimeType.INSTANCE, year, month, day, hour, minute, second);
    }

    public TimeStampLiteral(DateLikeType type, long year, long month, long day, long hour, long minute, long second) {
        super(type);
        String timeString = String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
        init(timeString);
        timeStamp = checkAndMaybeSetValue(timeString);
    }

    private Instant checkAndMaybeSetValue(String timeString) {
        return Objects.requireNonNull(Instant.parse(timeString, TIMESTAMP_FORMATTER), "timestamp format error");
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        DateTime dateTime = timeStamp.toDateTime(DateTimeZone.getDefault());
        return new org.apache.doris.analysis.DateLiteral(dateTime.getYear(), dateTime.getMonthOfYear(),
                dateTime.getDayOfMonth(), dateTime.getHourOfDay(), dateTime.getMinuteOfHour(),
                dateTime.getSecondOfMinute());
    }

    @Override
    public String toString() {
        return timeStamp.toDateTime(DateTimeZone.getDefault()).toString();
    }

    @Override
    public String toSql() {
        return toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TimeStampLiteral that = (TimeStampLiteral) o;
        return Objects.equals(timeStamp, that.timeStamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeStamp);
    }
}

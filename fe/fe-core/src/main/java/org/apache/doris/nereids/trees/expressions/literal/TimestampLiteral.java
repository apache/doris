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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;

import java.util.Objects;

/**
 * time stamp literal
 */
public class TimestampLiteral extends DateTimeLiteral {
    private final Instant timeStamp;

    public TimestampLiteral(String timeString) {
        super(timeString);
        init(timeString);
        timeStamp = checkAndMaybeSetValue(getStringValue());
    }

    public TimestampLiteral(long year, long month, long day, long hour, long minute, long second) {
        super(year, month, day, hour, minute, second);
        timeStamp = checkAndMaybeSetValue(getStringValue());
    }

    private Instant checkAndMaybeSetValue(String timeString) {
        return Objects.requireNonNull(Instant.parse(timeString), "invalid timestamp literal");
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
        TimestampLiteral that = (TimestampLiteral) o;
        return Objects.equals(timeStamp, that.timeStamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeStamp);
    }
}

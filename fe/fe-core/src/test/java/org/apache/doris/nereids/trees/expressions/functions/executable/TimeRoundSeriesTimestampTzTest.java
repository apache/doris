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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TimeRoundSeriesTimestampTzTest {
    @Test
    void testOriginsAcrossDst() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("America/New_York");
        context.setThreadLocalInfo();
        try {
            TimestampTzLiteral date = new TimestampTzLiteral("2024-03-10 04:30:00-04:00");
            TimestampTzLiteral origin = new TimestampTzLiteral("2024-03-09 00:00:00-05:00");
            assertInstant("2024-03-10 04:00:00-04:00",
                    TimeRoundSeries.hourFloor(date, new IntegerLiteral(2), origin));
            assertInstant("2024-03-10 06:00:00-04:00",
                    TimeRoundSeries.hourCeil(date, new IntegerLiteral(2), origin));
            date = new TimestampTzLiteral("2024-07-15 12:00:00-04:00");
            origin = new TimestampTzLiteral("2024-01-01 00:00:00-05:00");
            assertInstant("2024-07-01 00:00:00-04:00",
                    TimeRoundSeries.monthFloor(date, new IntegerLiteral(1), origin));
            assertInstant("2024-08-01 00:00:00-04:00",
                    TimeRoundSeries.monthCeil(date, new IntegerLiteral(1), origin));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void testRepeatedHourKeepsInputOffset() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("America/New_York");
        context.setThreadLocalInfo();
        try {
            for (String offset : new String[] {"-04:00", "-05:00"}) {
                TimestampTzLiteral date = new TimestampTzLiteral("2024-11-03 01:35:00" + offset);
                assertInstant("2024-11-03 01:00:00" + offset, TimeRoundSeries.hourFloor(date));
                assertInstant("2024-11-03 01:30:00" + offset,
                        TimeRoundSeries.minuteFloor(date, new IntegerLiteral(10)));
                assertInstant("2024-11-03 01:40:00" + offset,
                        TimeRoundSeries.minuteCeil(date, new IntegerLiteral(10)));
            }
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void testGapUsesTransitionInstantAndPreservesFraction() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("America/New_York");
        context.setThreadLocalInfo();
        try {
            TimestampTzLiteral date = new TimestampTzLiteral("2024-03-10 01:45:00.123456-05:00");
            TimestampTzLiteral origin = new TimestampTzLiteral("2024-03-09 00:30:00.123456-05:00");
            assertInstant("2024-03-10 03:00:00.123456-04:00",
                    TimeRoundSeries.hourCeil(date, new IntegerLiteral(2), origin));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void testSecondRoundingUsesUtc() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("America/New_York");
        context.setThreadLocalInfo();
        try {
            TimestampTzLiteral date = new TimestampTzLiteral("2024-03-10 04:30:00-04:00");
            TimestampTzLiteral origin = new TimestampTzLiteral("2024-03-09 00:00:00-05:00");
            assertInstant("2024-03-10 04:29:54-04:00",
                    TimeRoundSeries.secondFloor(date, new IntegerLiteral(7), origin));
            assertInstant("2024-03-10 04:30:01-04:00",
                    TimeRoundSeries.secondCeil(date, new IntegerLiteral(7), origin));
            assertInstant("2024-03-10 04:29:56-04:00",
                    TimeRoundSeries.secondFloor(date, new IntegerLiteral(7)));
            assertInstant("2024-03-10 04:30:03-04:00",
                    TimeRoundSeries.secondCeil(date, new IntegerLiteral(7)));
        } finally {
            ConnectContext.remove();
        }
    }

    private void assertInstant(String expected, Expression actual) {
        Assertions.assertEquals(new TimestampTzLiteral(expected).toJavaDateType(),
                ((TimestampTzLiteral) actual).toJavaDateType());
    }
}

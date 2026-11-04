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

import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TimeRoundSeriesTest {
    @Test
    void testTimestampTzFloorKeepsItsInstantBound() {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            context.getSessionVariable().setTimeZone("Australia/Lord_Howe");
            TimestampTzLiteral date = new TimestampTzLiteral("2024-10-06 02:35:00.123456+11:00");
            TimestampTzLiteral origin = new TimestampTzLiteral("2024-10-05 02:15:00.123456+10:30");
            Assertions.assertEquals(new TimestampTzLiteral("2024-10-06 01:15:00.123456+10:30"),
                    TimeRoundSeries.hourFloor(date, new IntegerLiteral(1), origin));
            Assertions.assertEquals(new TimestampTzLiteral("2024-10-06 03:15:00.123456+11:00"),
                    TimeRoundSeries.hourCeil(date, new IntegerLiteral(1), origin));

            // Several minute candidates are in the 44-minute, 30-second gap.
            context.getSessionVariable().setTimeZone("Africa/Monrovia");
            date = new TimestampTzLiteral("1972-01-07 00:44:40.123456Z");
            Assertions.assertEquals(new TimestampTzLiteral(TimeStampTzType.of(6), "1972-01-07 00:44:30Z"),
                    TimeRoundSeries.minuteFloor(date));
            Assertions.assertEquals(new TimestampTzLiteral(TimeStampTzType.of(6), "1972-01-07 00:44:00Z"),
                    TimeRoundSeries.secondFloor(date, new IntegerLiteral(60)));

            // The repeated hour keeps the input's second occurrence.
            context.getSessionVariable().setTimeZone("America/New_York");
            Assertions.assertEquals(new TimestampTzLiteral(TimeStampTzType.of(6), "2024-11-03 01:00:00-05:00"),
                    TimeRoundSeries.hourFloor(new TimestampTzLiteral("2024-11-03 01:35:00.123456-05:00")));
        } finally {
            ConnectContext.remove();
        }
    }
}

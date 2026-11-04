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

package org.apache.doris.nereids.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

class DateUtilsTest {
    @Test
    void testGapShiftPreservesFraction() {
        String[][] cases = {
                {"America/New_York", "2024-03-10T02:00:00", "2024-03-10T07:00:00Z"},
                {"America/New_York", "2024-03-10T02:30:00.123456789",
                        "2024-03-10T07:30:00.123456789Z"},
                {"America/New_York", "2024-03-10T02:59:59.999999999",
                        "2024-03-10T07:59:59.999999999Z"},
                {"Australia/Lord_Howe", "2024-10-06T02:15:00.123456789",
                        "2024-10-05T15:45:00.123456789Z"}
        };
        for (String[] c : cases) {
            ZoneId zone = ZoneId.of(c[0]);
            LocalDateTime local = LocalDateTime.parse(c[1]);
            Assertions.assertEquals(Instant.parse(c[2]), DateUtils.convertLocalToInstant(local, zone));
        }
    }

    @Test
    void testNormalOverlapAndFixedOffset() {
        String[][] cases = {
                {"America/New_York", "2024-03-10T01:59:59.999999999", "2024-03-10T06:59:59.999999999Z"},
                {"America/New_York", "2024-03-10T03:00:00", "2024-03-10T07:00:00Z"},
                {"America/New_York", "2024-11-03T01:30:00.123456789", "2024-11-03T05:30:00.123456789Z"},
                {"-05:00", "2024-03-10T02:30:00.123456789", "2024-03-10T07:30:00.123456789Z"}
        };
        for (String[] c : cases) {
            ZoneId zone = ZoneId.of(c[0]);
            LocalDateTime local = LocalDateTime.parse(c[1]);
            Instant expected = Instant.parse(c[2]);
            Assertions.assertEquals(expected, DateUtils.convertLocalToInstant(local, zone));
        }
    }

    @Test
    void testPreferredOffsetOnlySelectsAnOverlap() {
        ZoneId zone = ZoneId.of("America/New_York");
        Assertions.assertEquals(Instant.parse("2024-03-10T07:30:00.123456789Z"),
                DateUtils.convertLocalToInstant(LocalDateTime.parse("2024-03-10T02:30:00.123456789"),
                        zone, ZoneOffset.ofHours(-4)));
        Assertions.assertEquals(Instant.parse("2024-11-03T06:30:00.123456789Z"),
                DateUtils.convertLocalToInstant(LocalDateTime.parse("2024-11-03T01:30:00.123456789"),
                        zone, ZoneOffset.ofHours(-5)));
    }

}

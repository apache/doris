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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

class DeltaLakeTimeTravelTest {

    @Test
    void parsesEpochMillisAndDateTimeLiteralsInSessionZone() {
        TestingConnectorSession utc = new TestingConnectorSession("utc", "UTC");
        TestingConnectorSession shanghai = new TestingConnectorSession("shanghai", "Asia/Shanghai");

        Assertions.assertEquals(0L, DeltaLakeTimeTravel.parseTimestampMillis(
                utc, ConnectorTimeTravelSpec.timestamp("0", true)));
        Assertions.assertEquals(1_700_000_000_123L, DeltaLakeTimeTravel.parseTimestampMillis(
                shanghai, ConnectorTimeTravelSpec.timestamp("1700000000123", true)));

        long expectedUtcMidnight = Instant.parse("2026-11-24T00:00:00Z").toEpochMilli();
        Assertions.assertEquals(expectedUtcMidnight, DeltaLakeTimeTravel.parseTimestampMillis(
                utc, ConnectorTimeTravelSpec.timestamp("2026-11-24", false)));
        Assertions.assertEquals(Instant.parse("2026-11-23T16:00:00Z").toEpochMilli(),
                DeltaLakeTimeTravel.parseTimestampMillis(
                        shanghai, ConnectorTimeTravelSpec.timestamp("2026-11-24", false)));
        Assertions.assertEquals(Instant.parse("2026-01-05T00:00:00Z").toEpochMilli(),
                DeltaLakeTimeTravel.parseTimestampMillis(
                        utc, ConnectorTimeTravelSpec.timestamp("2026-1-5", false)));

        long expectedInstant = Instant.parse("2026-11-24T02:30:05.123Z").toEpochMilli();
        Assertions.assertEquals(expectedInstant, DeltaLakeTimeTravel.parseTimestampMillis(
                shanghai, ConnectorTimeTravelSpec.timestamp("2026-11-24 10:30:05.123", false)));
        Assertions.assertEquals(expectedInstant, DeltaLakeTimeTravel.parseTimestampMillis(
                shanghai, ConnectorTimeTravelSpec.timestamp(" 2026-11-24T10:30:05.123456 ", false)));
        Assertions.assertEquals(expectedInstant, DeltaLakeTimeTravel.parseTimestampMillis(
                shanghai, ConnectorTimeTravelSpec.timestamp("2026-11-24t10:30:05.123999999", false)));
        Assertions.assertEquals(Instant.parse("2026-11-24T02:30:00Z").toEpochMilli(),
                DeltaLakeTimeTravel.parseTimestampMillis(
                        shanghai, ConnectorTimeTravelSpec.timestamp("2026-11-24 10:30", false)));
    }

    @Test
    void honorsExplicitOffsetsIndependentOfSessionZone() {
        TestingConnectorSession newYork = new TestingConnectorSession("ny", "America/New_York");
        long expected = Instant.parse("2026-11-24T02:30:05.500Z").toEpochMilli();

        for (String literal : List.of(
                "2026-11-24T02:30:05.5Z",
                "2026-11-24 02:30:05.500z",
                "2026-11-24T10:30:05.500+08:00",
                "2026-11-24 10:30:05.500+08:00",
                "2026-11-24 10:30:05.500 +08:00",
                "2026-11-24 10:30:05.500+0800",
                "2026-11-24 10:30:05.500+08",
                "2026-11-24 10:30:05.500+08:00:00",
                "2026-11-23T19:30:05.500-07:00")) {
            Assertions.assertEquals(expected, DeltaLakeTimeTravel.parseTimestampMillis(
                    newYork, ConnectorTimeTravelSpec.timestamp(literal, false)), literal);
        }
    }

    @Test
    void rejectsMalformedOrOutOfRangeTimestamps() {
        TestingConnectorSession session = new TestingConnectorSession("bad-ts");
        for (ConnectorTimeTravelSpec spec : List.of(
                ConnectorTimeTravelSpec.timestamp("", false),
                ConnectorTimeTravelSpec.timestamp("   ", false),
                ConnectorTimeTravelSpec.timestamp("not_a_timestamp", false),
                ConnectorTimeTravelSpec.timestamp("-1", false),
                ConnectorTimeTravelSpec.timestamp("9223372036854775808", true),
                ConnectorTimeTravelSpec.timestamp("2026-02-30 10:00:00", false),
                ConnectorTimeTravelSpec.timestamp("2026-11-24 24:00:00", false),
                ConnectorTimeTravelSpec.timestamp("2026-11-24 10:60:00", false),
                ConnectorTimeTravelSpec.timestamp("2026-11-24T", false),
                ConnectorTimeTravelSpec.timestamp("2026-11-24  10:00:00", false),
                ConnectorTimeTravelSpec.timestamp("2026-11-24 10:00:00  +08:00", false),
                ConnectorTimeTravelSpec.timestamp("2026-11-24 10:00:00+08:00+08:00", false),
                ConnectorTimeTravelSpec.timestamp("2026-11-24 10:00:00.", false))) {
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> DeltaLakeTimeTravel.parseTimestampMillis(session, spec), spec.toString());
            Assertions.assertTrue(error.getMessage().contains("FOR TIME AS OF"), error.getMessage());
        }
    }
}

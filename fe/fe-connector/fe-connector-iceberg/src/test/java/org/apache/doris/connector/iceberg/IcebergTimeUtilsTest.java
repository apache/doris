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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorSession;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;

/**
 * Tests for {@link IcebergTimeUtils}, the self-contained session time-zone + datetime parsing shared by
 * the scan provider (timestamptz pushdown, T02) and the metadata layer ({@code FOR TIME AS OF}, T07).
 */
public class IcebergTimeUtilsTest {

    @Test
    public void datetimeToMillisInterpretsLocalTimeInSessionZone() {
        // WHY: FOR TIME AS OF '2023-06-15 10:30:00' must resolve the snapshot at that wall-clock time IN THE
        // SESSION ZONE; a wrong zone selects the WRONG snapshot (silently wrong rows). Asia/Shanghai is UTC+8,
        // so 10:30:00 local == 02:30:00Z. Oracle is an independent ISO-instant, not the same formatter.
        // MUTATION: applying UTC instead of the session zone -> 10:30:00Z != 02:30:00Z -> red.
        long expected = Instant.parse("2023-06-15T02:30:00Z").toEpochMilli();
        Assertions.assertEquals(expected,
                IcebergTimeUtils.datetimeToMillis("2023-06-15 10:30:00", ZoneId.of("Asia/Shanghai")));
    }

    @Test
    public void datetimeToMillisUtcZone() {
        long expected = Instant.parse("2023-06-15T10:30:00Z").toEpochMilli();
        Assertions.assertEquals(expected,
                IcebergTimeUtils.datetimeToMillis("2023-06-15 10:30:00", ZoneOffset.UTC));
    }

    @Test
    public void datetimeToMillisFailsLoudOnMalformedString() {
        // WHY: a malformed datetime is a user mistake; legacy returned -1 and the caller threw
        // DateTimeException("can't parse time"). Fail loud (never silently degrade to a wrong/0 snapshot).
        DateTimeException e = Assertions.assertThrows(DateTimeException.class,
                () -> IcebergTimeUtils.datetimeToMillis("not-a-time", ZoneOffset.UTC));
        Assertions.assertTrue(e.getMessage().contains("can't parse time"),
                "must surface the legacy 'can't parse time' message, was: " + e.getMessage());
    }

    @Test
    public void resolveSessionZoneHonorsCstDorisAlias() {
        // WHY: Doris stores SET time_zone='CST' verbatim; a plain ZoneId.of("CST") throws (CST is a SHORT_ID).
        // Legacy resolves CST -> Asia/Shanghai (NOT America/Chicago). MUTATION: dropping the alias map -> UTC
        // fallback -> not Asia/Shanghai -> red.
        Assertions.assertEquals(ZoneId.of("Asia/Shanghai"),
                IcebergTimeUtils.resolveSessionZone(session("CST")));
    }

    @Test
    public void resolveSessionZoneNullAndBlankFallBackToUtc() {
        Assertions.assertEquals(ZoneOffset.UTC, IcebergTimeUtils.resolveSessionZone(null));
        Assertions.assertEquals(ZoneOffset.UTC, IcebergTimeUtils.resolveSessionZone(session("")));
        Assertions.assertEquals(ZoneOffset.UTC, IcebergTimeUtils.resolveSessionZone(session("NOPE/ZZZ")));
    }

    private static ConnectorSession session(String tz) {
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "q";
            }

            @Override
            public String getUser() {
                return "u";
            }

            @Override
            public String getTimeZone() {
                return tz;
            }

            @Override
            public String getLocale() {
                return "en_US";
            }

            @Override
            public long getCatalogId() {
                return 0;
            }

            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return java.util.Collections.emptyMap();
            }
        };
    }
}

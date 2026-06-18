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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionExprUtilNormalizeTest {

    @Test
    public void testNormalizeTimestampTzWithExplicitTimeZone() throws AnalysisException {
        // A timezone-less local datetime string from the dynamic partition scheduler
        // meant to represent "2026-06-18 00:00:00" in Asia/Shanghai.
        // With the explicit timezone, normalization should produce the UTC equivalent:
        // 2026-06-17 16:00:00+00:00 (Asia/Shanghai is UTC+8).
        // TIMESTAMPTZ toString() always includes the UTC offset.
        String value = "2026-06-18 00:00:00";
        Type tsTzType = Type.TIMESTAMPTZ;
        String result = PartitionExprUtil.normalizePartitionValueString(value, tsTzType, "Asia/Shanghai");
        // The literal stores UTC time internally; toString() returns the UTC representation.
        Assertions.assertEquals("2026-06-17 16:00:00+00:00", result,
                "Asia/Shanghai midnight should normalize to 2026-06-17 16:00:00+00:00 UTC");

        // Verify with another timezone: America/New_York (UTC-4 on 2026-06-18).
        // 2026-06-18 00:00:00 EDT = 2026-06-18 04:00:00 UTC
        String resultNy = PartitionExprUtil.normalizePartitionValueString(value, tsTzType, "America/New_York");
        Assertions.assertEquals("2026-06-18 04:00:00+00:00", resultNy,
                "America/New_York midnight should normalize to 2026-06-18 04:00:00+00:00 UTC");
    }

    @Test
    public void testNormalizeTimestampTzWithoutTimeZoneFallsBackToUtc() throws AnalysisException {
        // Without an explicit timezone and without a ConnectContext,
        // the original normalizePartitionValueString falls back to UTC.
        // This is the existing behavior (session TZ = UTC when no ConnectContext).
        String value = "2026-06-18 00:00:00";
        Type tsTzType = Type.TIMESTAMPTZ;
        String result = PartitionExprUtil.normalizePartitionValueString(value, tsTzType);
        Assertions.assertEquals("2026-06-18 00:00:00+00:00", result,
                "Without explicit timezone, should fall back to UTC (session timezone)");
    }

    @Test
    public void testNormalizeTimestampTzWithNullTimeZoneDelegates() throws AnalysisException {
        // null timezone should delegate to the original method (same as no timezone).
        String value = "2026-06-18 00:00:00";
        Type tsTzType = Type.TIMESTAMPTZ;
        String result = PartitionExprUtil.normalizePartitionValueString(value, tsTzType, null);
        Assertions.assertEquals("2026-06-18 00:00:00+00:00", result,
                "Null timezone should delegate to original behavior (UTC fallback)");
    }

    @Test
    public void testNormalizeTimestampTzWithEmptyTimeZoneDelegates() throws AnalysisException {
        // Empty timezone should delegate to the original method.
        String value = "2026-06-18 00:00:00";
        Type tsTzType = Type.TIMESTAMPTZ;
        String result = PartitionExprUtil.normalizePartitionValueString(value, tsTzType, "");
        Assertions.assertEquals("2026-06-18 00:00:00+00:00", result,
                "Empty timezone should delegate to original behavior (UTC fallback)");
    }

    @Test
    public void testNormalizeNonTimestampTzWithTimeZonePassesThrough() throws AnalysisException {
        // For non-TIMESTAMPTZ types (e.g., DATETIME), the timezone parameter should be ignored
        // and the method should delegate to the original normalization.
        String value = "2026-06-18 00:00:00";
        Type dtType = Type.DATETIME;
        String result = PartitionExprUtil.normalizePartitionValueString(value, dtType, "Asia/Shanghai");
        Assertions.assertEquals("2026-06-18 00:00:00", result,
                "TimeZone parameter should be ignored for non-TIMESTAMPTZ types");
    }

    @Test
    public void testNormalizeDateTimeV2Unaffected() throws AnalysisException {
        // DATETIME V2 should still work correctly (no timezone semantics).
        String value = "2026-06-18 00:00:00";
        Type dtv2Type = Type.DATETIMEV2;
        String result = PartitionExprUtil.normalizePartitionValueString(value, dtv2Type, "Asia/Shanghai");
        Assertions.assertEquals("2026-06-18 00:00:00", result,
                "DATETIMEV2 should not be affected by timezone parameter");
    }

    @Test
    public void testNormalizeTimestampTzWithExplicitOffset() throws AnalysisException {
        // String with an explicit UTC offset should be parsed correctly.
        // The overload detects the existing timezone and delegates without appending.
        String value = "2026-06-18 00:00:00+08:00";
        Type tsTzType = Type.TIMESTAMPTZ;
        String result = PartitionExprUtil.normalizePartitionValueString(value, tsTzType, "Asia/Shanghai");
        // 2026-06-18 00:00:00+08:00 = 2026-06-17 16:00:00 UTC
        Assertions.assertEquals("2026-06-17 16:00:00+00:00", result,
                "Explicit offset in value should be preserved, timezone param should be ignored");
    }
}

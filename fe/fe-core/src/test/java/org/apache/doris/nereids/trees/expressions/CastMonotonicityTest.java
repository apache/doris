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

import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CastMonotonicityTest {
    private final Cast cast = new Cast(
            new SlotReference("ts", TimeStampTzType.of(6)), DateTimeV2Type.of(6));
    private ConnectContext previousContext;

    @BeforeEach
    void setUp() {
        previousContext = ConnectContext.get();
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    void testFixedOffsetTimeZoneIsMonotonicWithUnboundedRange() {
        setTimeZone("+00:00");

        Assertions.assertTrue(cast.isMonotonic(null, timestampTz("2024-11-03 06:00:00+00:00")));
        Assertions.assertTrue(cast.isMonotonic(timestampTz("2024-11-03 05:00:00+00:00"), null));
        Assertions.assertTrue(cast(6, 0).isMonotonic(
                timestampTz("2024-11-03 05:00:00.000000+00:00"),
                timestampTz("2024-11-03 05:59:59.900000+00:00")));
    }

    @Test
    void testDorisTimeZoneAliasIsMonotonic() {
        setTimeZone("CST");

        Assertions.assertTrue(cast.isMonotonic(
                timestampTz("2024-11-03 05:00:00+00:00"),
                timestampTz("2024-11-03 06:00:00+00:00")));
    }

    @Test
    void testFallbackAtUpperBoundaryIsNotMonotonic() {
        setTimeZone("America/New_York");

        Assertions.assertFalse(cast.isMonotonic(
                timestampTz("2024-11-03 05:00:00+00:00"),
                timestampTz("2024-11-03 06:00:00+00:00")));
    }

    @Test
    void testFallbackAtLowerBoundaryIsMonotonic() {
        setTimeZone("America/New_York");

        Assertions.assertTrue(cast.isMonotonic(
                timestampTz("2024-11-03 06:00:00+00:00"),
                timestampTz("2024-11-03 07:00:00+00:00")));
    }

    @Test
    void testSpringForwardIsMonotonic() {
        setTimeZone("America/New_York");

        Assertions.assertTrue(cast.isMonotonic(
                timestampTz("2024-03-10 06:00:00+00:00"),
                timestampTz("2024-03-10 07:00:00+00:00")));
    }

    @Test
    void testScaleReductionInDynamicTimeZoneIsNotMonotonic() {
        setTimeZone("America/New_York");

        Assertions.assertFalse(cast(6, 0).isMonotonic(
                timestampTz("2024-11-03 05:00:00.000000+00:00"),
                timestampTz("2024-11-03 05:59:59.900000+00:00")));
    }

    @Test
    void testUnboundedRangeInDynamicTimeZoneIsNotMonotonic() {
        setTimeZone("America/New_York");

        Assertions.assertFalse(cast.isMonotonic(null, timestampTz("2024-11-03 05:00:00+00:00")));
        Assertions.assertFalse(cast.isMonotonic(timestampTz("2024-11-03 07:00:00+00:00"), null));
    }

    private TimestampTzLiteral timestampTz(String value) {
        return new TimestampTzLiteral(TimeStampTzType.of(6), value);
    }

    private Cast cast(int sourceScale, int destinationScale) {
        return new Cast(new SlotReference("ts", TimeStampTzType.of(sourceScale)),
                DateTimeV2Type.of(destinationScale));
    }

    private void setTimeZone(String timeZone) {
        ConnectContext.get().getSessionVariable().setTimeZone(timeZone);
    }
}

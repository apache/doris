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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TimestampNsMonotonicityTest {
    private final SlotReference timestampNsSlot = new SlotReference("ts", TimeStampNsType.INSTANCE);
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
    void testExtractorsRejectFieldResetRanges() {
        Assertions.assertFalse(new Minute(timestampNsSlot).isMonotonic(
                timestampNs("2024-01-01 00:58:00.000000000"),
                timestampNs("2024-01-01 01:59:00.000000000")));
        Assertions.assertTrue(new Minute(timestampNsSlot).isMonotonic(
                timestampNs("2024-01-01 00:01:00.000000000"),
                timestampNs("2024-01-01 00:59:00.000000000")));

        Assertions.assertFalse(new Second(timestampNsSlot).isMonotonic(
                timestampNs("2024-01-01 00:00:58.000000000"),
                timestampNs("2024-01-01 00:01:59.000000000")));
        Assertions.assertFalse(new Microsecond(timestampNsSlot).isMonotonic(
                timestampNs("2024-01-01 00:00:00.999999000"),
                timestampNs("2024-01-01 00:00:01.000001000")));
    }

    @Test
    void testUnixTimestampRejectsSpringGap() {
        UnixTimestamp timestampNsUnixTimestamp = new UnixTimestamp(timestampNsSlot);

        setTimeZone("+00:00");
        Assertions.assertTrue(timestampNsUnixTimestamp.isMonotonic(null, null));

        setTimeZone("America/Los_Angeles");
        Assertions.assertFalse(timestampNsUnixTimestamp.isMonotonic(
                timestampNs("2024-03-10 02:00:00.999999999"),
                timestampNs("2024-03-10 03:30:00.000000000")));
        Assertions.assertTrue(timestampNsUnixTimestamp.isMonotonic(
                timestampNs("2024-03-10 03:00:00.000000000"),
                timestampNs("2024-03-10 04:00:00.000000000")));
        Assertions.assertFalse(timestampNsUnixTimestamp.isMonotonic(
                null, timestampNs("2024-03-10 04:00:00.000000000")));

        UnixTimestamp dateTimeV2UnixTimestamp = new UnixTimestamp(
                new SlotReference("dt", DateTimeV2Type.MAX));
        Assertions.assertFalse(dateTimeV2UnixTimestamp.isMonotonic(
                new DateTimeV2Literal("2024-03-10 02:00:00.999999"),
                new DateTimeV2Literal("2024-03-10 03:30:00.000000")));
    }

    private TimeStampNsLiteral timestampNs(String value) {
        return new TimeStampNsLiteral(value);
    }

    private void setTimeZone(String timeZone) {
        ConnectContext.get().getSessionVariable().setTimeZone(timeZone);
    }
}

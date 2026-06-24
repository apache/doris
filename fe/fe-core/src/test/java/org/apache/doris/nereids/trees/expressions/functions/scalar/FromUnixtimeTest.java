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
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FromUnixtimeTest {
    private final SlotReference timestampSlot = new SlotReference("ts", BigIntType.INSTANCE);
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
    void testIsMonotonicWithFixedOffsetTimeZone() {
        setTimeZone("+00:00");
        FromUnixtime fromUnixtime = new FromUnixtime(timestampSlot);

        Assertions.assertTrue(fromUnixtime.isMonotonic(
                new BigIntLiteral(1635638400L), new BigIntLiteral(1635642000L)));
    }

    @Test
    void testIsMonotonicWithoutDstTransition() {
        setTimeZone("Europe/Paris");
        FromUnixtime fromUnixtime = new FromUnixtime(timestampSlot);

        Assertions.assertTrue(fromUnixtime.isMonotonic(
                new BigIntLiteral(1610236800L), new BigIntLiteral(1610323200L)));
    }

    @Test
    void testIsMonotonicWithDstFallbackTransition() {
        setTimeZone("Europe/Paris");
        FromUnixtime fromUnixtime = new FromUnixtime(timestampSlot);

        Assertions.assertFalse(fromUnixtime.isMonotonic(
                new BigIntLiteral(1635638400L), new BigIntLiteral(1635642000L)));
    }

    @Test
    void testIsMonotonicWithFormatAndDstFallbackTransition() {
        setTimeZone("Europe/Paris");
        FromUnixtime fromUnixtime = new FromUnixtime(timestampSlot,
                new VarcharLiteral("%Y-%m-%d %H:%i:%s"));

        Assertions.assertFalse(fromUnixtime.isMonotonic(
                new BigIntLiteral(1635638400L), new BigIntLiteral(1635642000L)));
    }

    @Test
    void testIsMonotonicWithNonMonotonicFormat() {
        setTimeZone("+00:00");
        FromUnixtime fromUnixtime = new FromUnixtime(timestampSlot, new VarcharLiteral("%W"));

        Assertions.assertFalse(fromUnixtime.isMonotonic(
                new BigIntLiteral(1610236800L), new BigIntLiteral(1610323200L)));
    }

    private void setTimeZone(String timeZone) {
        ConnectContext.get().getSessionVariable().setTimeZone(timeZone);
    }
}

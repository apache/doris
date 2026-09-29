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
import org.apache.doris.nereids.trees.expressions.functions.FromSecondMonotonic;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class FromSecondMonotonicTest {
    private final SlotReference epochSlot = new SlotReference("epoch", BigIntType.INSTANCE);
    private ConnectContext previousContext;

    @BeforeEach
    void setUp() {
        previousContext = ConnectContext.get();
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    void testTimeZoneFallbackDisablesMonotonicity() {
        List<FromSecondMonotonic> functions = Arrays.asList(
                new FromSecond(epochSlot), new FromMillisecond(epochSlot), new FromMicrosecond(epochSlot));
        for (String timeZone : Arrays.asList("America/New_York", "Asia/Shanghai")) {
            ConnectContext.get().getSessionVariable().setTimeZone(timeZone);
            for (FromSecondMonotonic function : functions) {
                // A fallback anywhere in the zone's rules disables monotonicity for every input interval.
                Assertions.assertFalse(function.isMonotonic(new BigIntLiteral(1719792000),
                        new BigIntLiteral(1719795600)));
                Assertions.assertFalse(function.isMonotonic(new BigIntLiteral(1719792000), null));
            }
        }
    }

    @Test
    void testFixedOffsetPreservesMonotonicityForNonnegativeInput() {
        List<FromSecondMonotonic> functions = Arrays.asList(
                new FromSecond(epochSlot), new FromMillisecond(epochSlot), new FromMicrosecond(epochSlot));
        for (String timeZone : Arrays.asList("+00:00", "+08:00")) {
            ConnectContext.get().getSessionVariable().setTimeZone(timeZone);
            for (FromSecondMonotonic function : functions) {
                Assertions.assertTrue(function.isMonotonic(new BigIntLiteral(0), new BigIntLiteral(1)));
                Assertions.assertTrue(function.isMonotonic(new BigIntLiteral(0), null));
                Assertions.assertFalse(function.isMonotonic(new BigIntLiteral(-1), new BigIntLiteral(1)));
                Assertions.assertFalse(function.isMonotonic(null, new BigIntLiteral(1)));
            }
        }
    }
}

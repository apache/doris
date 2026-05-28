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
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateTimeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConvertTzTest {
    private final SlotReference timestampSlot = new SlotReference("ts", DateTimeType.INSTANCE);

    @Test
    void testIsMonotonicWithoutDstTransition() {
        ConvertTz convertTz = new ConvertTz(timestampSlot,
                new VarcharLiteral("UTC"), new VarcharLiteral("Europe/Paris"));

        Assertions.assertTrue(convertTz.isMonotonic(
                new DateTimeLiteral("2021-01-10 00:00:00"),
                new DateTimeLiteral("2021-01-11 00:00:00")));
    }

    @Test
    void testIsMonotonicWithDstFallbackInTargetZone() {
        ConvertTz convertTz = new ConvertTz(timestampSlot,
                new VarcharLiteral("UTC"), new VarcharLiteral("Europe/Paris"));

        Assertions.assertFalse(convertTz.isMonotonic(
                new DateTimeLiteral("2021-10-31 00:00:00"),
                new DateTimeLiteral("2021-10-31 01:00:00")));
    }

    @Test
    void testIsMonotonicWithDstFallbackAtLowerBoundaryInTargetZone() {
        ConvertTz convertTz = new ConvertTz(timestampSlot,
                new VarcharLiteral("UTC"), new VarcharLiteral("Europe/Paris"));

        Assertions.assertFalse(convertTz.isMonotonic(
                new DateTimeLiteral("2021-10-31 01:00:00"),
                new DateTimeLiteral("2021-10-31 02:00:00")));
    }

    @Test
    void testIsMonotonicWithDstGapInSourceZone() {
        ConvertTz convertTz = new ConvertTz(timestampSlot,
                new VarcharLiteral("Europe/Paris"), new VarcharLiteral("UTC"));

        Assertions.assertFalse(convertTz.isMonotonic(
                new DateTimeLiteral("2021-03-28 02:00:00"),
                new DateTimeLiteral("2021-03-28 03:00:00")));
    }

    @Test
    void testIsMonotonicWithDstGapAtUpperBoundaryInSourceZone() {
        ConvertTz convertTz = new ConvertTz(timestampSlot,
                new VarcharLiteral("Europe/Paris"), new VarcharLiteral("UTC"));

        Assertions.assertFalse(convertTz.isMonotonic(
                new DateTimeLiteral("2021-03-28 01:00:00"),
                new DateTimeLiteral("2021-03-28 02:00:00")));
    }
}

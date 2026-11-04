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

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
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

    @Test
    void testTimeStampTzToTimeStampNsRejectsFallback() {
        Cast timestampNsCast = new Cast(
                new SlotReference("tz", TimeStampTzType.of(6)), TimeStampNsType.INSTANCE);

        setTimeZone("America/Los_Angeles");
        Assertions.assertFalse(timestampNsCast.isMonotonic(
                timestampTz("2024-11-03 08:30:00+00:00"),
                timestampTz("2024-11-03 09:30:00+00:00")));

        setTimeZone("+00:00");
        Assertions.assertTrue(timestampNsCast.isMonotonic(
                timestampTz("2024-11-03 08:30:00+00:00"),
                timestampTz("2024-11-03 09:30:00+00:00")));
    }

    @Test
    void testTimeStampNsToTimeStampTzRejectsDynamicZoneRounding() {
        Cast timestampTzCast = new Cast(
                new SlotReference("ts", TimeStampNsType.INSTANCE), TimeStampTzType.of(6));

        setTimeZone("America/Los_Angeles");
        Assertions.assertFalse(timestampTzCast.isMonotonic(
                timestampNs("2024-03-10 02:00:00.000000000"),
                timestampNs("2024-03-10 02:01:00.000000000")));
        Assertions.assertTrue(timestampTzCast.isMonotonic(
                timestampNs("2024-03-10 03:00:00.000000000"),
                timestampNs("2024-03-10 04:00:00.000000000")));

        setTimeZone("+00:00");
        Assertions.assertTrue(timestampTzCast.isMonotonic(
                timestampNs("2024-03-10 02:00:00.000000000"),
                timestampNs("2024-03-10 02:01:00.000000000")));
    }

    @Test
    void testDateTimeV2ToTimeStampTzRejectsGap() {
        Cast localCast = new Cast(new SlotReference("d", DateTimeV2Type.of(6)), TimeStampTzType.of(6));
        DateTimeV2Literal lower = new DateTimeV2Literal("2024-03-10 02:00:00");
        DateTimeV2Literal upper = new DateTimeV2Literal("2024-03-10 03:00:00");

        setTimeZone("America/New_York");
        Assertions.assertFalse(localCast.isMonotonic(lower, upper));
        Assertions.assertFalse(localCast.isMonotonic(null, upper));
        Assertions.assertTrue(localCast.isMonotonic(upper, new DateTimeV2Literal("2024-03-10 04:00:00")));
        Cast roundedCast = new Cast(new SlotReference("d", DateTimeV2Type.of(6)), TimeStampTzType.of(0));
        Assertions.assertFalse(roundedCast.isMonotonic(
                new DateTimeV2Literal("2024-03-10 01:59:59.6"),
                new DateTimeV2Literal("2024-03-10 02:59:59.6")));

        setTimeZone("+00:00");
        Assertions.assertTrue(localCast.isMonotonic(lower, upper));
        Assertions.assertTrue(localCast.isMonotonic(null, null));
    }

    @Test
    void testDateTimeV2GapPartitionIsRetained() {
        setTimeZone("America/New_York");
        DateTimeV2Type source = DateTimeV2Type.of(6);
        TimeStampTzType target = TimeStampTzType.of(6);
        DateTimeV2Literal lower = new DateTimeV2Literal(source, 2024, 3, 10, 2, 0, 0, 0);
        DateTimeV2Literal upper = new DateTimeV2Literal(source, 2024, 3, 10, 3, 0, 0, 0);
        TimestampTzLiteral wanted = new TimestampTzLiteral(target, 2024, 3, 10, 7, 30, 0, 0);
        Assertions.assertEquals(lower.checkedCastTo(target), upper.checkedCastTo(target));
        Assertions.assertEquals(wanted,
                new DateTimeV2Literal(source, 2024, 3, 10, 2, 30, 0, 0).checkedCastTo(target));

        PartitionKey lowerKey = new PartitionKey();
        lowerKey.pushColumn(lower.toLegacyLiteral(), PrimitiveType.DATETIMEV2);
        PartitionKey upperKey = new PartitionKey();
        upperKey.pushColumn(upper.toLegacyLiteral(), PrimitiveType.DATETIMEV2);
        RangePartitionItem item = new RangePartitionItem(Range.closedOpen(lowerKey, upperKey));
        SlotReference d = new SlotReference("d", source);
        Expression predicate = new EqualTo(new Cast(d, target), wanted);
        Assertions.assertEquals(ImmutableList.of(1L), PartitionPruner.prune(
                ImmutableList.<Slot>of(d), predicate, ImmutableMap.<Long, PartitionItem>of(1L, item),
                CascadesContext.initTempContext(), PartitionPruner.PartitionTableType.OLAP).first);
    }

    private TimestampTzLiteral timestampTz(String value) {
        return new TimestampTzLiteral(TimeStampTzType.of(6), value);
    }

    private TimeStampNsLiteral timestampNs(String value) {
        return new TimeStampNsLiteral(value);
    }

    private Cast cast(int sourceScale, int destinationScale) {
        return new Cast(new SlotReference("ts", TimeStampTzType.of(sourceScale)),
                DateTimeV2Type.of(destinationScale));
    }

    private void setTimeZone(String timeZone) {
        ConnectContext.get().getSessionVariable().setTimeZone(timeZone);
    }
}

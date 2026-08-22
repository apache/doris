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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.CastException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

class TimeStampNsLiteralTest {

    @Test
    void testFactoriesAndAccessors() {
        TimeStampNsLiteral min = TimeStampNsLiteral.getMinValue();
        TimeStampNsLiteral max = TimeStampNsLiteral.getMaxValue();
        Assertions.assertEquals("1677-09-21 00:12:43.145224192", min.getStringValue());
        Assertions.assertEquals("2262-04-11 23:47:16.854775807", max.getStringValue());

        TimeStampNsLiteral endOfDay = TimeStampNsLiteral.createEndOfDay(2024, 1, 2);
        Assertions.assertEquals("2024-01-02 23:59:59.999999999", endOfDay.getStringValue());
        Assertions.assertFalse(endOfDay.isMidnight());

        LocalDateTime dateTime = LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123456789);
        TimeStampNsLiteral literal = TimeStampNsLiteral.fromJavaDateType(dateTime);
        Assertions.assertEquals(dateTime, literal.toJavaDateType());
        Assertions.assertSame(TimeStampNsType.INSTANCE, literal.getDataType());
        Assertions.assertEquals(20240102030405L, literal.getValue());
        Assertions.assertEquals(3, literal.getHour());
        Assertions.assertEquals(4, literal.getMinute());
        Assertions.assertEquals(5, literal.getSecond());
        Assertions.assertEquals(123456, literal.getMicroSecond());
        Assertions.assertEquals(123456789, literal.getNanoSecond());
        Assertions.assertEquals(TimeStampNsType.SCALE, literal.getScale());
        Assertions.assertEquals(11045123456789L, literal.getTimePartInNanoseconds());
        Assertions.assertEquals(123456789, literal.getFractionalSecondInNanoseconds());
        Assertions.assertEquals(20240102030405.123456789, literal.getDouble(), 0.001);
        Assertions.assertEquals(literal.getStringValue(), literal.toLegacyLiteral().getStringValue());

        TimeStampNsLiteral midnight = new TimeStampNsLiteral("2024-01-02 00:00:00.000000000");
        Assertions.assertTrue(midnight.isMidnight());
        Assertions.assertNotEquals(literal, literal.getStringValue());
    }

    @Test
    void testTimeZoneAndGuardDigitParsing() {
        TimeStampNsLiteral withTimeZone = new TimeStampNsLiteral(
                "2024-01-02 03:04:05.123456789+00:00");
        Assertions.assertEquals(123456789, withTimeZone.getNanoSecond());

        TimeStampNsLiteral rounded = new TimeStampNsLiteral(
                "2024-01-02 03:04:05.9999999995");
        Assertions.assertEquals(0, rounded.getNanoSecond());
        Assertions.assertEquals(6, rounded.getSecond());
    }

    @Test
    void testGuardDigitRoundsBeforeDstTimezoneConversion() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("America/New_York");
        context.setThreadLocalInfo();
        try {
            Assertions.assertEquals("2024-03-10 03:00:00.000000000",
                    new TimeStampNsLiteral(
                            "2024-03-10T06:59:59.9999999995Z").getStringValue());
            Assertions.assertEquals("2024-11-03 01:00:00.000000000",
                    new TimeStampNsLiteral(
                            "2024-11-03T05:59:59.9999999995Z").getStringValue());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void testRoundToDateTimeV2() {
        TimeStampNsLiteral literal = new TimeStampNsLiteral("2024-01-02 03:04:05.123456789");
        Assertions.assertEquals("2024-01-02 03:04:05.123456",
                literal.roundFloorToDateTimeV2(6).getStringValue());
        Assertions.assertEquals("2024-01-02 03:04:05.123457",
                literal.roundCeilingToDateTimeV2(6).getStringValue());

        TimeStampNsLiteral exact = new TimeStampNsLiteral("2024-01-02 03:04:05.123456000");
        Assertions.assertEquals("2024-01-02 03:04:05.123456",
                exact.roundCeilingToDateTimeV2(6).getStringValue());
    }

    @Test
    void testInvalidCivilFieldsAndEpochBounds() {
        Assertions.assertThrows(AnalysisException.class,
                () -> new TimeStampNsLiteral(2024, 13, 1, 0, 0, 0, 0));
        Assertions.assertThrows(AnalysisException.class,
                () -> new TimeStampNsLiteral(2024, 1, 1, 24, 0, 0, 0));
        Assertions.assertThrows(AnalysisException.class,
                () -> new TimeStampNsLiteral(2024, 1, 1, 0, 60, 0, 0));
        Assertions.assertThrows(AnalysisException.class,
                () -> new TimeStampNsLiteral(2024, 1, 1, 0, 0, 60, 0));
        Assertions.assertThrows(AnalysisException.class,
                () -> new TimeStampNsLiteral(2024, 1, 1, 0, 0, 0, 1_000_000_000));
        Assertions.assertThrows(AnalysisException.class,
                () -> new TimeStampNsLiteral("1677-09-21 00:12:43.145224191"));
        Assertions.assertThrows(AnalysisException.class,
                () -> new TimeStampNsLiteral("2262-04-11 23:47:16.854775808"));
    }

    @Test
    void testTimestampNsTypeContract() {
        Assertions.assertEquals("timestamp_ns", TimeStampNsType.INSTANCE.toSql());
        Assertions.assertTrue(TimeStampNsType.INSTANCE.toCatalogDataType().isTimeStampNs());
        Assertions.assertSame(TimeStampNsType.INSTANCE,
                TimeStampNsType.INSTANCE.scaleTypeForType(IntegerType.INSTANCE));
        Assertions.assertSame(TimeStampNsType.INSTANCE,
                TimeStampNsType.INSTANCE.forTypeFromString(new StringLiteral("2024-01-02")));
    }

    @Test
    void testLegacyConversionAndLiteralCasts() {
        org.apache.doris.analysis.TimeStampNsLiteral legacy =
                new org.apache.doris.analysis.TimeStampNsLiteral(
                        2024, 1, 2, 3, 4, 5, 123456789);
        TimeStampNsLiteral converted = (TimeStampNsLiteral) Literal.fromLegacyLiteral(
                legacy, Type.TIMESTAMP_NS);
        Assertions.assertEquals("2024-01-02 03:04:05.123456789", converted.getStringValue());

        Expression fromString = new StringLiteral("2024-01-02 03:04:05.123456789")
                .checkedCastTo(TimeStampNsType.INSTANCE);
        Assertions.assertInstanceOf(TimeStampNsLiteral.class, fromString);
        Assertions.assertThrows(CastException.class,
                () -> new StringLiteral("2024-02-30 03:04:05.123456789")
                        .checkedCastTo(TimeStampNsType.INSTANCE));

        Expression fromInteger = new BigIntLiteral(20240102030405L)
                .checkedCastTo(TimeStampNsType.INSTANCE);
        Assertions.assertEquals("2024-01-02 03:04:05.000000000",
                ((TimeStampNsLiteral) fromInteger).getStringValue());
    }

    @Test
    void testSupportedOutboundCastsAndRounding() {
        TimeStampNsLiteral lower = TimeStampNsLiteral.getMinValue();
        TimeStampNsLiteral carry = new TimeStampNsLiteral("1969-12-31 23:59:59.999999999");
        TimeStampNsLiteral normal = new TimeStampNsLiteral("2024-02-29 12:34:56.123456789");
        TimeStampNsLiteral upper = TimeStampNsLiteral.getMaxValue();

        Assertions.assertEquals("1677-09-21 00:12:43.145224",
                ((Literal) lower.checkedCastTo(DateTimeV2Type.MAX)).getStringValue());
        Assertions.assertEquals("1970-01-01 00:00:00.000000",
                ((Literal) carry.checkedCastTo(DateTimeV2Type.MAX)).getStringValue());
        Assertions.assertEquals("2024-02-29 12:34:56.123457",
                ((Literal) normal.checkedCastTo(DateTimeV2Type.MAX)).getStringValue());
        Assertions.assertEquals("2262-04-11 23:47:16.854776",
                ((Literal) upper.checkedCastTo(DateTimeV2Type.MAX)).getStringValue());

        Assertions.assertEquals("2024-02-29",
                ((Literal) normal.checkedCastTo(DateV2Type.INSTANCE)).getStringValue());
        Assertions.assertEquals("12:34:56.123457",
                ((Literal) normal.checkedCastTo(TimeV2Type.MAX)).getStringValue());
        Assertions.assertEquals(20240229123456L,
                ((BigIntLiteral) normal.checkedCastTo(BigIntType.INSTANCE)).getValue());
        Assertions.assertEquals("20240229123456",
                ((LargeIntLiteral) normal.checkedCastTo(LargeIntType.INSTANCE)).getValue().toString());
        Assertions.assertThrowsExactly(AnalysisException.class,
                () -> normal.checkedCastTo(TinyIntType.INSTANCE));
        Assertions.assertThrowsExactly(AnalysisException.class,
                () -> normal.checkedCastTo(SmallIntType.INSTANCE));
        Assertions.assertThrowsExactly(AnalysisException.class,
                () -> normal.checkedCastTo(IntegerType.INSTANCE));
    }

    @Test
    void testTimestampTzCastMatchesBeDstTransitionPolicy() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("America/New_York");
        context.setThreadLocalInfo();
        try {
            TimeStampNsLiteral gap = new TimeStampNsLiteral(
                    "2024-03-10 02:30:00.123456789");
            Assertions.assertEquals("2024-03-10 07:00:00.123457+00:00",
                    ((Literal) gap.checkedCastTo(TimeStampTzType.of(6))).getStringValue());

            TimeStampNsLiteral overlap = new TimeStampNsLiteral(
                    "2024-11-03 01:30:00.123456789");
            Assertions.assertEquals("2024-11-03 05:30:00.123457+00:00",
                    ((Literal) overlap.checkedCastTo(TimeStampTzType.of(6))).getStringValue());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void testSupportedInboundCastsCheckTimestampNsRange() {
        Assertions.assertThrows(CastException.class,
                () -> new DateV2Literal("1677-09-21").checkedCastTo(TimeStampNsType.INSTANCE));
        Assertions.assertEquals("1677-09-22 00:00:00.000000000",
                ((Literal) new DateV2Literal("1677-09-22")
                        .checkedCastTo(TimeStampNsType.INSTANCE)).getStringValue());

        Assertions.assertThrows(CastException.class,
                () -> new DateTimeV2Literal(DateTimeV2Type.MAX,
                        "1677-09-21 00:12:43.145224")
                        .checkedCastTo(TimeStampNsType.INSTANCE));
        Assertions.assertEquals("1677-09-21 00:12:43.145225000",
                ((Literal) new DateTimeV2Literal(DateTimeV2Type.MAX,
                        "1677-09-21 00:12:43.145225")
                        .checkedCastTo(TimeStampNsType.INSTANCE)).getStringValue());
        Assertions.assertEquals("2262-04-11 23:47:16.854775000",
                ((Literal) new DateTimeV2Literal(DateTimeV2Type.MAX,
                        "2262-04-11 23:47:16.854775")
                        .checkedCastTo(TimeStampNsType.INSTANCE)).getStringValue());
        Assertions.assertThrows(CastException.class,
                () -> new DateTimeV2Literal(DateTimeV2Type.MAX,
                        "2262-04-11 23:47:16.854776")
                        .checkedCastTo(TimeStampNsType.INSTANCE));
    }
}

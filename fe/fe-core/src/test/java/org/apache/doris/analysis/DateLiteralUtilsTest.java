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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.foundation.format.FormatOptions;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DateLiteralUtilsTest {

    @Test
    public void testTimestampTzInit() throws AnalysisException {
        String value;
        DateLiteral dateLiteral;
        String expectedResult;

        value = "2020-02-02 09:03:04.123456+08:00";
        dateLiteral = DateLiteralUtils.createDateLiteral(value, ScalarType.createTimeStampTzType(6));
        expectedResult = "'2020-02-02 01:03:04.123456+00:00'";
        Assertions.assertEquals(expectedResult, dateLiteral.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
    }

    @Test
    public void testTimestampTzStringForQuery() throws AnalysisException {
        try {
            ConnectContext context = new ConnectContext();
            context.setThreadLocalInfo();
            DateLiteral dateLiteral = DateLiteralUtils.createDateLiteral("2020-02-02 12:00:03.123456+00:00",
                    ScalarType.createTimeStampTzType(6));
            String timeZone;
            String expected;

            timeZone = "+08:00";
            context.getSessionVariable().setTimeZone(timeZone);
            expected = "2020-02-02 20:00:03.123456+08:00";
            Assertions.assertEquals(expected, dateLiteral.accept(ExprToStringValueVisitor.INSTANCE, StringValueContext.forQuery(FormatOptions.getDefault())));

            timeZone = "-08:00";
            context.getSessionVariable().setTimeZone(timeZone);
            expected = "2020-02-02 04:00:03.123456-08:00";
            Assertions.assertEquals(expected, dateLiteral.accept(ExprToStringValueVisitor.INSTANCE, StringValueContext.forQuery(FormatOptions.getDefault())));
        } finally {
            ConnectContext.remove();
        }

    }

    @Test
    public void testDatetimeV2Init() throws AnalysisException {
        String value;
        DateLiteral dateLiteral;
        String expectedResult;

        value = "2020-02-02 09:03:04.123456+08:00";
        dateLiteral = DateLiteralUtils.createDateLiteral(value, ScalarType.createDatetimeV2Type(6));
        expectedResult = "'2020-02-02 09:03:04.123456'";
        Assertions.assertEquals(expectedResult, dateLiteral.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));

        value = "2020-02-02 09:03:04.123456+09:00";
        dateLiteral = DateLiteralUtils.createDateLiteral(value, ScalarType.createDatetimeV2Type(6));
        expectedResult = "'2020-02-02 08:03:04.123456'";
        Assertions.assertEquals(expectedResult, dateLiteral.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
    }

    @Test
    public void testDateV2Init() throws AnalysisException {
        String value;
        DateLiteral dateLiteral;
        String expectedResult;

        value = "2020-02-02+08:00";
        dateLiteral = DateLiteralUtils.createDateLiteral(value, ScalarType.DATEV2);
        expectedResult = "'2020-02-02'";
        Assertions.assertEquals(expectedResult, dateLiteral.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
        value = "2020-02-02+09:00";
        dateLiteral = DateLiteralUtils.createDateLiteral(value, ScalarType.DATEV2);
        expectedResult = "'2020-02-01'";
        Assertions.assertEquals(expectedResult, dateLiteral.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
    }

    // ==================== Tests for DateLiteral(String, Type) constructor ====================

    @Test
    public void testDateInit() throws AnalysisException {
        // DATE type
        DateLiteral dl = DateLiteralUtils.createDateLiteral("2023-06-15", Type.DATE);
        Assertions.assertEquals(2023, dl.getYear());
        Assertions.assertEquals(6, dl.getMonth());
        Assertions.assertEquals(15, dl.getDay());
        Assertions.assertEquals(0, dl.getHour());
        Assertions.assertEquals(0, dl.getMinute());
        Assertions.assertEquals(0, dl.getSecond());
        Assertions.assertEquals(0, dl.getMicrosecond());
        Assertions.assertTrue(dl.getType().isDate());

        // DATEV2 type
        DateLiteral dlv2 = DateLiteralUtils.createDateLiteral("2023-06-15", ScalarType.DATEV2);
        Assertions.assertEquals(2023, dlv2.getYear());
        Assertions.assertEquals(6, dlv2.getMonth());
        Assertions.assertEquals(15, dlv2.getDay());
        Assertions.assertTrue(dlv2.getType().isDateV2());

        // Single-digit month and day
        DateLiteral dlSingle = DateLiteralUtils.createDateLiteral("2023-1-5", ScalarType.DATEV2);
        Assertions.assertEquals(2023, dlSingle.getYear());
        Assertions.assertEquals(1, dlSingle.getMonth());
        Assertions.assertEquals(5, dlSingle.getDay());

        // Leap year date
        DateLiteral dlLeap = DateLiteralUtils.createDateLiteral("2024-02-29", ScalarType.DATEV2);
        Assertions.assertEquals(2024, dlLeap.getYear());
        Assertions.assertEquals(2, dlLeap.getMonth());
        Assertions.assertEquals(29, dlLeap.getDay());

        // Boundary: first day of year
        DateLiteral dlFirst = DateLiteralUtils.createDateLiteral("2023-01-01", Type.DATE);
        Assertions.assertEquals(2023, dlFirst.getYear());
        Assertions.assertEquals(1, dlFirst.getMonth());
        Assertions.assertEquals(1, dlFirst.getDay());

        // Boundary: last day of year
        DateLiteral dlLast = DateLiteralUtils.createDateLiteral("2023-12-31", Type.DATE);
        Assertions.assertEquals(2023, dlLast.getYear());
        Assertions.assertEquals(12, dlLast.getMonth());
        Assertions.assertEquals(31, dlLast.getDay());
    }

    @Test
    public void testDatetimeInit() throws AnalysisException {
        DateLiteral dl = DateLiteralUtils.createDateLiteral("2023-06-15 14:30:45", Type.DATETIME);
        Assertions.assertEquals(2023, dl.getYear());
        Assertions.assertEquals(6, dl.getMonth());
        Assertions.assertEquals(15, dl.getDay());
        Assertions.assertEquals(14, dl.getHour());
        Assertions.assertEquals(30, dl.getMinute());
        Assertions.assertEquals(45, dl.getSecond());
        Assertions.assertEquals(0, dl.getMicrosecond());
        Assertions.assertTrue(dl.getType().isDatetime());

        // Midnight
        DateLiteral dlMidnight = DateLiteralUtils.createDateLiteral("2023-06-15 00:00:00", Type.DATETIME);
        Assertions.assertEquals(0, dlMidnight.getHour());
        Assertions.assertEquals(0, dlMidnight.getMinute());
        Assertions.assertEquals(0, dlMidnight.getSecond());

        // End of day
        DateLiteral dlEnd = DateLiteralUtils.createDateLiteral("2023-06-15 23:59:59", Type.DATETIME);
        Assertions.assertEquals(23, dlEnd.getHour());
        Assertions.assertEquals(59, dlEnd.getMinute());
        Assertions.assertEquals(59, dlEnd.getSecond());
    }

    @Test
    public void testDatetimeV2WithMicroseconds() throws AnalysisException {
        // Full 6-digit microsecond
        DateLiteral dl6 = DateLiteralUtils.createDateLiteral("2023-06-15 14:30:45.123456", ScalarType.createDatetimeV2Type(6));
        Assertions.assertEquals(2023, dl6.getYear());
        Assertions.assertEquals(6, dl6.getMonth());
        Assertions.assertEquals(15, dl6.getDay());
        Assertions.assertEquals(14, dl6.getHour());
        Assertions.assertEquals(30, dl6.getMinute());
        Assertions.assertEquals(45, dl6.getSecond());
        Assertions.assertEquals(123456, dl6.getMicrosecond());
        Assertions.assertTrue(dl6.getType().isDatetimeV2());
        Assertions.assertEquals("2023-06-15 14:30:45.123456", dl6.getStringValue());

        // 3-digit microsecond (millisecond precision)
        DateLiteral dl3 = DateLiteralUtils.createDateLiteral("2023-06-15 14:30:45.123", ScalarType.createDatetimeV2Type(3));
        Assertions.assertEquals(123000, dl3.getMicrosecond());
        Assertions.assertEquals("2023-06-15 14:30:45.123", dl3.getStringValue());

        // 1-digit microsecond
        DateLiteral dl1 = DateLiteralUtils.createDateLiteral("2023-06-15 14:30:45.1", ScalarType.createDatetimeV2Type(1));
        Assertions.assertEquals(100000, dl1.getMicrosecond());
        Assertions.assertEquals("2023-06-15 14:30:45.1", dl1.getStringValue());

        // Scale-0 DATETIMEV2 without microsecond fraction
        DateLiteral dl0 = DateLiteralUtils.createDateLiteral("2023-06-15 14:30:45", ScalarType.createDatetimeV2Type(0));
        Assertions.assertEquals(0, dl0.getMicrosecond());
        Assertions.assertEquals(2023, dl0.getYear());
        Assertions.assertTrue(dl0.getType().isDatetimeV2());
    }

    @Test
    public void testCompactDateFormats() throws AnalysisException {
        // YYYYMMDD compact date
        DateLiteral dlDate = DateLiteralUtils.createDateLiteral("20230615", ScalarType.DATEV2);
        Assertions.assertEquals(2023, dlDate.getYear());
        Assertions.assertEquals(6, dlDate.getMonth());
        Assertions.assertEquals(15, dlDate.getDay());

        // YYYYMMDDHHmmss compact datetime
        DateLiteral dlDatetime = DateLiteralUtils.createDateLiteral("20230615143045", ScalarType.createDatetimeV2Type(0));
        Assertions.assertEquals(2023, dlDatetime.getYear());
        Assertions.assertEquals(6, dlDatetime.getMonth());
        Assertions.assertEquals(15, dlDatetime.getDay());
        Assertions.assertEquals(14, dlDatetime.getHour());
        Assertions.assertEquals(30, dlDatetime.getMinute());
        Assertions.assertEquals(45, dlDatetime.getSecond());

        // YYYYMMDD with DATE type
        DateLiteral dlDateCompact = DateLiteralUtils.createDateLiteral("20231231", Type.DATE);
        Assertions.assertEquals(2023, dlDateCompact.getYear());
        Assertions.assertEquals(12, dlDateCompact.getMonth());
        Assertions.assertEquals(31, dlDateCompact.getDay());

        // YYYYMMDDHHmmss with DATETIME type
        DateLiteral dlDtCompact = DateLiteralUtils.createDateLiteral("20230615143045", Type.DATETIME);
        Assertions.assertEquals(2023, dlDtCompact.getYear());
        Assertions.assertEquals(14, dlDtCompact.getHour());
        Assertions.assertEquals(30, dlDtCompact.getMinute());
        Assertions.assertEquals(45, dlDtCompact.getSecond());

        // Compact datetime with microseconds
        DateLiteral dlCompactMicro = DateLiteralUtils.createDateLiteral("20230615143045.123456", ScalarType.createDatetimeV2Type(6));
        Assertions.assertEquals(123456, dlCompactMicro.getMicrosecond());
        Assertions.assertEquals(14, dlCompactMicro.getHour());
    }

    @Test
    public void testTwoDigitYear() throws AnalysisException {
        // Year >= 70 maps to 19xx
        DateLiteral dl70 = DateLiteralUtils.createDateLiteral("70-01-01", ScalarType.DATEV2);
        Assertions.assertEquals(1970, dl70.getYear());

        DateLiteral dl99 = DateLiteralUtils.createDateLiteral("99-12-31", ScalarType.DATEV2);
        Assertions.assertEquals(1999, dl99.getYear());

        // Year < 70 maps to 20xx
        DateLiteral dl00 = DateLiteralUtils.createDateLiteral("00-01-01", ScalarType.DATEV2);
        Assertions.assertEquals(2000, dl00.getYear());

        DateLiteral dl69 = DateLiteralUtils.createDateLiteral("69-06-15", ScalarType.DATEV2);
        Assertions.assertEquals(2069, dl69.getYear());

        // Two-digit year with datetime
        DateLiteral dlDt = DateLiteralUtils.createDateLiteral("23-06-15 14:30:45", ScalarType.createDatetimeV2Type(0));
        Assertions.assertEquals(2023, dlDt.getYear());
        Assertions.assertEquals(14, dlDt.getHour());
    }

    @Test
    public void testDatetimeAutoUpgrade() throws AnalysisException {
        // DATETIME type with microseconds auto-upgrades to DATETIMEV2
        DateLiteral dl = DateLiteralUtils.createDateLiteral("2023-06-15 14:30:45.123456", Type.DATETIME);
        Assertions.assertTrue(dl.getType().isDatetimeV2());
        Assertions.assertEquals(123456, dl.getMicrosecond());
        Assertions.assertEquals(2023, dl.getYear());

        // 3-digit fraction: auto-upgrade with scale=3
        DateLiteral dl3 = DateLiteralUtils.createDateLiteral("2023-06-15 14:30:45.123", Type.DATETIME);
        Assertions.assertTrue(dl3.getType().isDatetimeV2());
        Assertions.assertEquals(123000, dl3.getMicrosecond());

        // No microseconds: stays as DATETIME
        DateLiteral dlNoMicro = DateLiteralUtils.createDateLiteral("2023-06-15 14:30:45", Type.DATETIME);
        Assertions.assertTrue(dlNoMicro.getType().isDatetime());
        Assertions.assertEquals(0, dlNoMicro.getMicrosecond());
    }

    @Test
    public void testTimezoneOffsetWithDatetimeV2() throws AnalysisException {
        // Without ConnectContext, DateUtils.getTimeZone() returns system default.
        // We verify the conversion is consistent: create two literals with different offsets
        // representing the same instant and check they produce the same local time.
        DateLiteral dlUtc = DateLiteralUtils.createDateLiteral("2023-06-15 12:00:00+00:00", ScalarType.createDatetimeV2Type(0));
        DateLiteral dlPlus8 = DateLiteralUtils.createDateLiteral("2023-06-15 20:00:00+08:00", ScalarType.createDatetimeV2Type(0));
        // Both represent the same instant, so should produce the same local time after conversion
        Assertions.assertEquals(dlUtc.getYear(), dlPlus8.getYear());
        Assertions.assertEquals(dlUtc.getMonth(), dlPlus8.getMonth());
        Assertions.assertEquals(dlUtc.getDay(), dlPlus8.getDay());
        Assertions.assertEquals(dlUtc.getHour(), dlPlus8.getHour());
        Assertions.assertEquals(dlUtc.getMinute(), dlPlus8.getMinute());
        Assertions.assertEquals(dlUtc.getSecond(), dlPlus8.getSecond());

        // Negative offset
        DateLiteral dlMinus5 = DateLiteralUtils.createDateLiteral("2023-06-15 07:00:00-05:00", ScalarType.createDatetimeV2Type(0));
        Assertions.assertEquals(dlUtc.getHour(), dlMinus5.getHour());
        Assertions.assertEquals(dlUtc.getDay(), dlMinus5.getDay());

        // Timezone offset with microseconds
        DateLiteral dlTzMicro = DateLiteralUtils.createDateLiteral("2023-06-15 20:00:00.123456+08:00",
                ScalarType.createDatetimeV2Type(6));
        DateLiteral dlUtcMicro = DateLiteralUtils.createDateLiteral("2023-06-15 12:00:00.123456+00:00",
                ScalarType.createDatetimeV2Type(6));
        Assertions.assertEquals(dlUtcMicro.getHour(), dlTzMicro.getHour());
        Assertions.assertEquals(dlUtcMicro.getMicrosecond(), dlTzMicro.getMicrosecond());
    }

    @Test
    public void testInvalidDateFormat() {
        // Completely invalid string
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("not-a-date", Type.DATE));

        // Empty string triggers haveTimeZoneOffset precondition failure
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("", Type.DATE));

        // Invalid month
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-13-01", ScalarType.DATEV2));

        // Invalid day
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-02-30", ScalarType.DATEV2));

        // Too many date parts
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-06-15-01", ScalarType.DATEV2));

        // Non-leap year Feb 29
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-02-29", ScalarType.DATEV2));

        // Too many time parts (HH:MM:SS:XX)
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-06-15 14:30:45:99", ScalarType.createDatetimeV2Type(0)));
    }

    @Test
    public void testDateWithTimePart() {
        // DATE type with time part should fail
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-06-15 14:30:45", Type.DATE));

        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-06-15 00:00:00", ScalarType.DATEV2));
    }

    @Test
    public void testDatetimeWithoutTimePart() {
        // DATETIME type without time part should fail
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-06-15", Type.DATETIME));

        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-06-15", Type.DATETIMEV2));
    }

    @Test
    public void testOutOfRange() {
        // Year out of range (>9999)
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("10000-01-01", ScalarType.DATEV2));

        // Month 0 is invalid (STRICT resolver rejects it)
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-00-15", ScalarType.DATEV2));

        // Day 0 is invalid
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-06-00", ScalarType.DATEV2));

        // Day 32 is invalid for any month
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-01-32", ScalarType.DATEV2));

        // Hour 24 is invalid
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-06-15 24:00:00", ScalarType.createDatetimeV2Type(0)));

        // Minute 60 is invalid
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-06-15 14:60:00", ScalarType.createDatetimeV2Type(0)));

        // Second 60 is invalid
        Assertions.assertThrows(AnalysisException.class,
                () -> DateLiteralUtils.createDateLiteral("2023-06-15 14:30:60", ScalarType.createDatetimeV2Type(0)));
    }

    @Test
    public void testTimestampTzConversion() throws AnalysisException {
        // TimestampTz always converts to UTC regardless of ConnectContext
        DateLiteral dl = DateLiteralUtils.createDateLiteral("2023-06-15 20:00:00.000000+08:00",
                ScalarType.createTimeStampTzType(6));
        // +08:00 means source is 8 hours ahead of UTC, so UTC = 20:00 - 8 = 12:00
        Assertions.assertEquals(2023, dl.getYear());
        Assertions.assertEquals(6, dl.getMonth());
        Assertions.assertEquals(15, dl.getDay());
        Assertions.assertEquals(12, dl.getHour());
        Assertions.assertEquals(0, dl.getMinute());
        Assertions.assertEquals(0, dl.getSecond());
        Assertions.assertEquals(0, dl.getMicrosecond());

        // Negative offset: -05:00 means source is 5 hours behind UTC, so UTC = 07:00 + 5 = 12:00
        DateLiteral dlNeg = DateLiteralUtils.createDateLiteral("2023-06-15 07:00:00.000000-05:00",
                ScalarType.createTimeStampTzType(6));
        Assertions.assertEquals(12, dlNeg.getHour());
        Assertions.assertEquals(15, dlNeg.getDay());

        // Date rollover: converting from +12:00 at early morning should roll back a day in UTC
        DateLiteral dlRollback = DateLiteralUtils.createDateLiteral("2023-06-16 02:00:00.000000+12:00",
                ScalarType.createTimeStampTzType(6));
        Assertions.assertEquals(14, dlRollback.getHour());
        Assertions.assertEquals(15, dlRollback.getDay());

        // Microsecond preservation through timezone conversion
        DateLiteral dlMicro = DateLiteralUtils.createDateLiteral("2023-06-15 20:00:00.654321+08:00",
                ScalarType.createTimeStampTzType(6));
        Assertions.assertEquals(12, dlMicro.getHour());
        Assertions.assertEquals(654321, dlMicro.getMicrosecond());

        // Verify getStringValue includes +00:00 suffix for TimestampTz
        Assertions.assertTrue(dl.getStringValue().endsWith("+00:00"));
    }
}

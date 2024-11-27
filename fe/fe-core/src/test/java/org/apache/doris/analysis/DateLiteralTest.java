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
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.InvalidFormatException;
import org.apache.doris.common.jmockit.Deencapsulation;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneOffset;
import java.util.TimeZone;

public class DateLiteralTest {

    @Test
    public void testGetStringInFe() throws AnalysisException {
        DateLiteral literal = new DateLiteral("1997-10-07", Type.DATE);
        String s = literal.getStringValueInFe(FormatOptions.getDefault());
        Assert.assertEquals(s, "1997-10-07");
        Assert.assertEquals(literal.getStringValueInFe(FormatOptions.getForPresto()), "1997-10-07");
    }

    @Test
    public void twoDigitYear() {
        boolean hasException = false;
        try {
            DateLiteral literal = new DateLiteral("1997-10-07", Type.DATE);
            Assert.assertEquals(1997, literal.getYear());

            DateLiteral literal2 = new DateLiteral("97-10-07", Type.DATE);
            Assert.assertEquals(1997, literal2.getYear());

            DateLiteral literal3 = new DateLiteral("0097-10-07", Type.DATE);
            Assert.assertEquals(97, literal3.getYear());

            DateLiteral literal4 = new DateLiteral("99-10-07", Type.DATE);
            Assert.assertEquals(1999, literal4.getYear());

            DateLiteral literal5 = new DateLiteral("70-10-07", Type.DATE);
            Assert.assertEquals(1970, literal5.getYear());

            DateLiteral literal6 = new DateLiteral("69-10-07", Type.DATE);
            Assert.assertEquals(2069, literal6.getYear());

            DateLiteral literal7 = new DateLiteral("00-10-07", Type.DATE);
            Assert.assertEquals(2000, literal7.getYear());

        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void testDateFormat() {
        boolean hasException = false;
        try {
            DateLiteral literal = new DateLiteral("1997-10-7", Type.DATE);
            Assert.assertEquals(1997, literal.getYear());

            literal = new DateLiteral("2021-06-1", Type.DATE);
            Assert.assertEquals(2021, literal.getYear());
            Assert.assertEquals(6, literal.getMonth());
            Assert.assertEquals(1, literal.getDay());

            literal = new DateLiteral("2022-6-01", Type.DATE);
            Assert.assertEquals(2022, literal.getYear());
            Assert.assertEquals(6, literal.getMonth());
            Assert.assertEquals(1, literal.getDay());

            literal = new DateLiteral("2023-6-1", Type.DATE);
            Assert.assertEquals(2023, literal.getYear());
            Assert.assertEquals(6, literal.getMonth());
            Assert.assertEquals(1, literal.getDay());

            literal = new DateLiteral("20230601", Type.DATE);
            Assert.assertEquals(2023, literal.getYear());
            Assert.assertEquals(6, literal.getMonth());
            Assert.assertEquals(1, literal.getDay());
        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void testParseDateTimeToHourORMinute() throws Exception {
        String s = "2020-12-13 12:13:14";
        Type type = Type.DATETIME;
        DateLiteral literal = new DateLiteral(s, type);
        Assert.assertTrue(literal.toSql().contains("2020-12-13 12:13:14"));
        s = "2020-12-13 12:13";
        literal = new DateLiteral(s, type);
        Assert.assertTrue(literal.toSql().contains("2020-12-13 12:13:00"));
        s = "2020-12-13 12";
        literal = new DateLiteral(s, type);
        Assert.assertTrue(literal.toSql().contains("2020-12-13 12:00:00"));
    }

    @Test
    public void testParseDateTimeV2ToHourORMinute() throws Exception {
        String s = "2020-12-13 12:13:14.123";
        Type type = ScalarType.createDatetimeV2Type(6);
        DateLiteral literal = new DateLiteral(s, type);
        Assert.assertTrue(literal.toSql().contains("2020-12-13 12:13:14.123"));
        s = "2020-12-13 12:13";
        literal = new DateLiteral(s, type);
        Assert.assertTrue(literal.toSql().contains("2020-12-13 12:13:00"));
        s = "2020-12-13 12";
        literal = new DateLiteral(s, type);
        Assert.assertTrue(literal.toSql().contains("2020-12-13 12:00:00"));

        String s2 = "2020-12-13 12:13:14.123456";
        DateLiteral literal2 = new DateLiteral(s2, type);
        Assert.assertTrue(literal2.toSql().contains("2020-12-13 12:13:14.123456"));
    }

    @Test
    public void uncheckedCastTo() {
        boolean hasException = false;
        try {
            DateLiteral literal = new DateLiteral("1997-10-07", Type.DATE);
            Expr castToExpr = literal.uncheckedCastTo(Type.DATETIME);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATETIME);

            DateLiteral literal2 = new DateLiteral("1997-10-07 12:23:23", Type.DATETIME);
            Expr castToExpr2 = literal2.uncheckedCastTo(Type.DATETIME);
            Assert.assertTrue(castToExpr2 instanceof DateLiteral);
        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void testCheckDate() {
        boolean hasException = false;
        try {
            DateLiteral dateLiteral = new DateLiteral();
            dateLiteral.fromDateFormatStr("%Y%m%d", "19971007", false);
            Assert.assertFalse(Deencapsulation.invoke(dateLiteral, "checkDate"));

            dateLiteral.fromDateFormatStr("%Y%m%d", "19970007", false);
            Assert.assertFalse(Deencapsulation.invoke(dateLiteral, "checkDate"));

            dateLiteral.fromDateFormatStr("%Y%m%d", "19971000", false);
            Assert.assertFalse(Deencapsulation.invoke(dateLiteral, "checkDate"));

            dateLiteral.fromDateFormatStr("%Y%m%d", "20000229", false);
            Assert.assertFalse(Deencapsulation.invoke(dateLiteral, "checkDate"));

        } catch (InvalidFormatException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
        try {
            DateLiteral literal = new DateLiteral("10000-10-07", Type.DATE);
            Assert.assertEquals(10000, literal.getYear());
            Assert.assertTrue(false);
        } catch (AnalysisException e) {
            // pass
        }
    }

    @Test
    public void testCheckRange() {
        boolean hasException = false;
        try {
            DateLiteral dateLiteral = new DateLiteral();
            dateLiteral.fromDateFormatStr("%Y%m%d%H%i%s%f", "20201209123456123456", false);
            Assert.assertFalse(Deencapsulation.invoke(dateLiteral, "checkRange"));

        } catch (InvalidFormatException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void testTwoDigitYearForDateV2() {
        boolean hasException = false;
        try {
            DateLiteral literal = new DateLiteral("1997-10-07", Type.DATEV2);
            Assert.assertEquals(1997, literal.getYear());

            DateLiteral literal2 = new DateLiteral("97-10-07", Type.DATEV2);
            Assert.assertEquals(1997, literal2.getYear());

            DateLiteral literal3 = new DateLiteral("0097-10-07", Type.DATEV2);
            Assert.assertEquals(97, literal3.getYear());

            DateLiteral literal4 = new DateLiteral("99-10-07", Type.DATEV2);
            Assert.assertEquals(1999, literal4.getYear());

            DateLiteral literal5 = new DateLiteral("70-10-07", Type.DATEV2);
            Assert.assertEquals(1970, literal5.getYear());

            DateLiteral literal6 = new DateLiteral("69-10-07", Type.DATEV2);
            Assert.assertEquals(2069, literal6.getYear());

            DateLiteral literal7 = new DateLiteral("00-10-07", Type.DATEV2);
            Assert.assertEquals(2000, literal7.getYear());

        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void testDateFormatForDateV2() {
        boolean hasException = false;
        try {
            DateLiteral literal = new DateLiteral("1997-10-7", Type.DATEV2);
            Assert.assertEquals(1997, literal.getYear());

            literal = new DateLiteral("2021-06-1", Type.DATEV2);
            Assert.assertEquals(2021, literal.getYear());
            Assert.assertEquals(6, literal.getMonth());
            Assert.assertEquals(1, literal.getDay());

            literal = new DateLiteral("2022-6-01", Type.DATEV2);
            Assert.assertEquals(2022, literal.getYear());
            Assert.assertEquals(6, literal.getMonth());
            Assert.assertEquals(1, literal.getDay());

            literal = new DateLiteral("2023-6-1", Type.DATEV2);
            Assert.assertEquals(2023, literal.getYear());
            Assert.assertEquals(6, literal.getMonth());
            Assert.assertEquals(1, literal.getDay());

            literal = new DateLiteral("20230601", Type.DATEV2);
            Assert.assertEquals(2023, literal.getYear());
            Assert.assertEquals(6, literal.getMonth());
            Assert.assertEquals(1, literal.getDay());

            literal = new DateLiteral("2020-02-29", Type.DATEV2);
            Assert.assertEquals(2020, literal.getYear());
            Assert.assertEquals(2, literal.getMonth());
            Assert.assertEquals(29, literal.getDay());
        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);

        try {
            new DateLiteral("2022-02-29", Type.DATEV2);
        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);
    }

    @Test
    public void testDateFormatForDatetimeV2() {
        boolean hasException = false;
        try {
            DateLiteral literal = new DateLiteral("1997-10-7 00:00:00.123456", ScalarType.createDatetimeV2Type(6));
            Assert.assertEquals(1997, literal.getYear());
            Assert.assertEquals(123456, literal.getMicrosecond());

            literal = new DateLiteral("2021-06-1 00:00:00.123456", ScalarType.createDatetimeV2Type(6));
            Assert.assertEquals(2021, literal.getYear());
            Assert.assertEquals(6, literal.getMonth());
            Assert.assertEquals(1, literal.getDay());

            literal = new DateLiteral("2022-6-01 00:00:00.123456", ScalarType.createDatetimeV2Type(6));
            Assert.assertEquals(2022, literal.getYear());
            Assert.assertEquals(6, literal.getMonth());
            Assert.assertEquals(1, literal.getDay());
        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void testParseDateTimeToHourORMinuteForDateV2() throws Exception {
        String s = "2020-12-13 12:13:14";
        Type type = Type.DATETIMEV2;
        DateLiteral literal = new DateLiteral(s, type);
        Assert.assertTrue(literal.toSql().contains("2020-12-13 12:13:14"));
        s = "2020-12-13 12:13";
        literal = new DateLiteral(s, type);
        Assert.assertTrue(literal.toSql().contains("2020-12-13 12:13:00"));
        s = "2020-12-13 12";
        literal = new DateLiteral(s, type);
        Assert.assertTrue(literal.toSql().contains("2020-12-13 12:00:00"));
    }

    @Test
    public void testUncheckedCastToForDateV2() {
        boolean hasException = false;
        try {
            // DATEV2 -> DATE/DATETIME/DATETIMEV2
            DateLiteral literal = new DateLiteral("1997-10-07", Type.DATEV2);
            Expr castToExpr = literal.uncheckedCastTo(Type.DATE);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATE);
            castToExpr = literal.uncheckedCastTo(Type.DATETIME);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATETIME);
            castToExpr = literal.uncheckedCastTo(Type.DATETIMEV2);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATETIMEV2);

            // DATE -> DATEV2/DATETIME/DATETIMEV2
            literal = new DateLiteral("1997-10-07", Type.DATE);
            castToExpr = literal.uncheckedCastTo(Type.DATEV2);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATEV2);
            castToExpr = literal.uncheckedCastTo(Type.DATETIME);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATETIME);
            castToExpr = literal.uncheckedCastTo(Type.DATETIMEV2);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATETIMEV2);

            // DATETIME -> DATEV2/DATE/DATETIMEV2
            literal = new DateLiteral("1997-10-07 12:23:23", Type.DATETIME);
            castToExpr = literal.uncheckedCastTo(Type.DATEV2);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATEV2);
            castToExpr = literal.uncheckedCastTo(Type.DATE);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATE);
            castToExpr = literal.uncheckedCastTo(Type.DATETIMEV2);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATETIMEV2);

            // DATETIMEV2 -> DATEV2/DATE/DATETIME
            literal = new DateLiteral("1997-10-07 12:23:23", Type.DATETIMEV2);
            castToExpr = literal.uncheckedCastTo(Type.DATEV2);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATEV2);
            castToExpr = literal.uncheckedCastTo(Type.DATE);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATE);
            castToExpr = literal.uncheckedCastTo(Type.DATETIME);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATETIME);
            Type t = ScalarType.createDatetimeV2Type(6);
            castToExpr = literal.uncheckedCastTo(t);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, t);
        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void testCheckDateForDateV2() {
        boolean hasException = false;
        try {
            DateLiteral dateLiteral = new DateLiteral();
            dateLiteral.fromDateFormatStr("%Y%m%d", "19971007", false, Type.DATEV2);
            Assert.assertFalse(Deencapsulation.invoke(dateLiteral, "checkDate"));

            dateLiteral.fromDateFormatStr("%Y%m%d", "19970007", false, Type.DATEV2);
            Assert.assertFalse(Deencapsulation.invoke(dateLiteral, "checkDate"));

            dateLiteral.fromDateFormatStr("%Y%m%d", "19971000", false, Type.DATEV2);
            Assert.assertFalse(Deencapsulation.invoke(dateLiteral, "checkDate"));

            dateLiteral.fromDateFormatStr("%Y%m%d", "20000229", false, Type.DATEV2);
            Assert.assertFalse(Deencapsulation.invoke(dateLiteral, "checkDate"));

        } catch (InvalidFormatException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void testCheckRangeForDateV2() {
        boolean hasException = false;
        try {
            DateLiteral dateLiteral = new DateLiteral();
            dateLiteral.fromDateFormatStr("%Y%m%d%H%i%s%f", "20201209123456123456", false, Type.DATETIMEV2);
            Assert.assertFalse(Deencapsulation.invoke(dateLiteral, "checkRange"));

        } catch (InvalidFormatException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void testUnixTimestampWithMilliMicroSecond() throws AnalysisException {
        String s = "2020-12-13 12:13:14.123456";
        Type type = Type.DATETIMEV2;
        DateLiteral literal = new DateLiteral(s, type);
        long l = literal.getUnixTimestampWithMillisecond(TimeZone.getTimeZone(ZoneOffset.UTC));
        Assert.assertEquals(123, l % 1000);

        long l2 = literal.getUnixTimestampWithMicroseconds(TimeZone.getTimeZone(ZoneOffset.UTC));
        Assert.assertEquals(123456, l2 % 1000000);
    }
}

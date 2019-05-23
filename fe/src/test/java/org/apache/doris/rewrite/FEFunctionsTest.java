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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.TimeZone;

import static org.junit.Assert.fail;

/*
 * Author: Chenmingyu
 * Date: Mar 13, 2019
 */

public class FEFunctionsTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void unixtimestampTest() {
        try {
            IntLiteral timestamp = FEFunctions.unix_timestamp(new DateLiteral("2018-01-01", Type.DATE));
            Assert.assertEquals(1514736000, timestamp.getValue());
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dateAddTest() throws AnalysisException {
        DateLiteral actualResult = FEFunctions.dateAdd(new StringLiteral("2018-08-08"), new IntLiteral(1));
        DateLiteral expectedResult = new DateLiteral("2018-08-09", Type.DATE);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.dateAdd(new StringLiteral("2018-08-08"), new IntLiteral(-1));
        expectedResult = new DateLiteral("2018-08-07", Type.DATE);
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void fromUnixTimeTest() throws AnalysisException {
        StringLiteral actualResult = FEFunctions.fromUnixTime(new IntLiteral(100000));
        StringLiteral expectedResult = new StringLiteral("1970-01-02 11:46:40");
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.fromUnixTime(new IntLiteral(100000), new StringLiteral("yyyy-MM-dd"));
        expectedResult = new StringLiteral("1970-01-02");
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.fromUnixTime(new IntLiteral(0));
        expectedResult = new StringLiteral("1970-01-01 08:00:00");
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void fromUnixTimeTestException() throws AnalysisException {
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("unixtime should larger than zero");
        FEFunctions.fromUnixTime(new IntLiteral(-100));
    }

    @Test
    public void dateFormatUtilTest() {
        try {
            Assert.assertEquals("19670102,196701,196701,0101", FEFunctions.dateFormat(new DateLiteral("1967-01-02 13:04:05", Type.DATETIME), new StringLiteral("%Y%m%d,%X%V,%x%v,%U%u")).getStringValue());
            Assert.assertEquals("19960105,199553,199601,0001", FEFunctions.dateFormat(new DateLiteral("1996-01-05 13:04:05", Type.DATETIME), new StringLiteral("%Y%m%d,%X%V,%x%v,%U%u")).getStringValue());

            Assert.assertEquals("2017-01-01,01,00", FEFunctions.dateFormat(new DateLiteral("2017-01-01 13:04:05", Type.DATETIME), new StringLiteral("%Y-%m-%d,%U,%u")).getStringValue());
            Assert.assertEquals("201753,201752,5352", FEFunctions.dateFormat(new DateLiteral("2017-12-31 13:04:05", Type.DATETIME),new StringLiteral("%X%V,%x%v,%U%u")).getStringValue());

            DateLiteral testDate = new DateLiteral("2001-01-09 13:04:05", Type.DATETIME);
            Assert.assertEquals("Tue", FEFunctions.dateFormat(testDate, new StringLiteral("%a")).getStringValue());
            Assert.assertEquals("Jan", FEFunctions.dateFormat(testDate, new StringLiteral("%b")).getStringValue());
            Assert.assertEquals("1", FEFunctions.dateFormat(testDate, new StringLiteral("%c")).getStringValue());
            Assert.assertEquals("09", FEFunctions.dateFormat(testDate, new StringLiteral("%d")).getStringValue());
            Assert.assertEquals("9", FEFunctions.dateFormat(testDate, new StringLiteral("%e")).getStringValue());
            Assert.assertEquals("13", FEFunctions.dateFormat(testDate, new StringLiteral("%H")).getStringValue());
            Assert.assertEquals("01", FEFunctions.dateFormat(testDate, new StringLiteral("%h")).getStringValue());
            Assert.assertEquals("01", FEFunctions.dateFormat(testDate, new StringLiteral("%I")).getStringValue());
            Assert.assertEquals("04", FEFunctions.dateFormat(testDate, new StringLiteral("%i")).getStringValue());
            Assert.assertEquals("009", FEFunctions.dateFormat(testDate, new StringLiteral("%j")).getStringValue());
            Assert.assertEquals("13", FEFunctions.dateFormat(testDate, new StringLiteral("%k")).getStringValue());
            Assert.assertEquals("1", FEFunctions.dateFormat(testDate, new StringLiteral("%l")).getStringValue());
            Assert.assertEquals("January", FEFunctions.dateFormat(testDate, new StringLiteral("%M")).getStringValue());
            Assert.assertEquals( "01", FEFunctions.dateFormat(testDate, new StringLiteral("%m")).getStringValue());
            Assert.assertEquals("PM", FEFunctions.dateFormat(testDate, new StringLiteral("%p")).getStringValue());
            Assert.assertEquals("01:04:05 PM", FEFunctions.dateFormat(testDate, new StringLiteral("%r")).getStringValue());
            Assert.assertEquals("05", FEFunctions.dateFormat(testDate, new StringLiteral("%S")).getStringValue());
            Assert.assertEquals("05", FEFunctions.dateFormat(testDate, new StringLiteral("%s")).getStringValue());
            Assert.assertEquals("13:04:05", FEFunctions.dateFormat(testDate, new StringLiteral("%T")).getStringValue());
            Assert.assertEquals("02", FEFunctions.dateFormat(testDate, new StringLiteral("%v")).getStringValue());
            Assert.assertEquals("Tuesday", FEFunctions.dateFormat(testDate, new StringLiteral("%W")).getStringValue());
            Assert.assertEquals("2", FEFunctions.dateFormat(testDate, new StringLiteral("%w")).getStringValue());
            Assert.assertEquals("2001", FEFunctions.dateFormat(testDate, new StringLiteral("%Y")).getStringValue());
            Assert.assertEquals("01", FEFunctions.dateFormat(testDate, new StringLiteral("%y")).getStringValue());
            Assert.assertEquals("%", FEFunctions.dateFormat(testDate, new StringLiteral("%%")).getStringValue());
            Assert.assertEquals("foo", FEFunctions.dateFormat(testDate, new StringLiteral("foo")).getStringValue());
            Assert.assertEquals("g", FEFunctions.dateFormat(testDate, new StringLiteral("%g")).getStringValue());
            Assert.assertEquals("4", FEFunctions.dateFormat(testDate, new StringLiteral("%4")).getStringValue());
            Assert.assertEquals("2001 02" ,FEFunctions.dateFormat(testDate, new StringLiteral("%x %v")).getStringValue());
            Assert.assertEquals("9th" ,FEFunctions.dateFormat(testDate, new StringLiteral("%D")).getStringValue());
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dateParseTest() {
        TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");
        TimeZone.setDefault(tz);
        try {
            Assert.assertEquals("2013-05-10", FEFunctions.dateParse(new StringLiteral("2013,05,10"), new StringLiteral("%Y,%m,%d")).getStringValue());
            Assert.assertEquals("2013-05-17 00:35:10", FEFunctions.dateParse(new StringLiteral("2013-05-17 12:35:10"), new StringLiteral("%Y-%m-%d %h:%i:%s")).getStringValue());
            Assert.assertEquals("2013-05-17 00:35:10", FEFunctions.dateParse(new StringLiteral("2013-05-17 00:35:10"), new StringLiteral("%Y-%m-%d %H:%i:%s")).getStringValue());
            Assert.assertEquals("2013-05-17 00:35:10", FEFunctions.dateParse(new StringLiteral("2013-05-17 12:35:10 AM"), new StringLiteral("%Y-%m-%d %h:%i:%s %p")).getStringValue());
            Assert.assertEquals("2013-05-17 12:35:10", FEFunctions.dateParse(new StringLiteral("2013-05-17 12:35:10 PM"), new StringLiteral("%Y-%m-%d %h:%i:%s %p")).getStringValue());
            Assert.assertEquals("2013-05-17 23:35:10", FEFunctions.dateParse(new StringLiteral("abc 2013-05-17 fff 23:35:10 xyz"), new StringLiteral("abc %Y-%m-%d fff %H:%i:%s xyz")).getStringValue());
            Assert.assertEquals("2016-01-28 23:45:46", FEFunctions.dateParse(new StringLiteral("28-JAN-16 11.45.46 PM"), new StringLiteral("%d-%b-%y %l.%i.%s %p")).getStringValue());
            Assert.assertEquals("2019-05-09", FEFunctions.dateParse(new StringLiteral("2019/May/9"), new StringLiteral("%Y/%b/%d")).getStringValue());
            Assert.assertEquals("2019-05-09", FEFunctions.dateParse(new StringLiteral("2019,129"), new StringLiteral("%Y,%j")).getStringValue());
            Assert.assertEquals("2019-05-09", FEFunctions.dateParse(new StringLiteral("2019,19,Thursday"), new StringLiteral("%x,%v,%W")).getStringValue());
            Assert.assertEquals("2019-05-09 12:10:45", FEFunctions.dateParse(new StringLiteral("12:10:45-20190509"), new StringLiteral("%T-%Y%m%d")).getStringValue());
            Assert.assertEquals("2019-05-09 09:10:45", FEFunctions.dateParse(new StringLiteral("20190509-9:10:45"), new StringLiteral("%Y%m%d-%k:%i:%S")).getStringValue());
        } catch (AnalysisException e) {
            fail("Junit test dateParse fail");
            e.printStackTrace();
        }

        try {
            FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%D"));
            fail("Junit test dateParse fail");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(), "%D not supported in date format string");
        }
        try {
            FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%U"));
            fail("Junit test dateParse fail");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(), "%U not supported in date format string");
        }
        try {
            FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%u"));
            fail("Junit test dateParse fail");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(), "%u not supported in date format string");
        }
        try {
            FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%V"));
            fail("Junit test dateParse fail");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(), "%V not supported in date format string");
        }
        try {
            FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%w"));
            fail("Junit test dateParse fail");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(), "%w not supported in date format string");
        }
        try {
            FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%X"));
            fail("Junit test dateParse fail");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(), "%X not supported in date format string");
        }
    }
}

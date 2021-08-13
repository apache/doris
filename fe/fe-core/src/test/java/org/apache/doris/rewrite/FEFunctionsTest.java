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

import mockit.Expectations;
import mockit.Mocked;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.TimeUtils;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;

import static org.junit.Assert.fail;

public class FEFunctionsTest {

    @Mocked
    TimeUtils timeUtils;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() {
        TimeZone tz = TimeZone.getTimeZone(ZoneId.of("Asia/Shanghai"));
        new Expectations(timeUtils) {
            {
                TimeUtils.getTimeZone();
                minTimes = 0;
                result = tz;
            }
        };
    }

    @Test
    public void unixtimestampTest() {
        try {
            IntLiteral timestamp = FEFunctions.unixTimestamp(new DateLiteral("2018-01-01", Type.DATE));
            Assert.assertEquals(1514736000, timestamp.getValue());
            timestamp = FEFunctions.unixTimestamp(new DateLiteral("1970-01-01 08:00:00", Type.DATETIME));
            Assert.assertEquals(0, timestamp.getValue());
            timestamp = FEFunctions.unixTimestamp(new DateLiteral("1970-01-01 00:00:00", Type.DATETIME));
            Assert.assertEquals(0, timestamp.getValue());
            timestamp = FEFunctions.unixTimestamp(new DateLiteral("1969-01-01 00:00:00", Type.DATETIME));
            Assert.assertEquals(0, timestamp.getValue());
            timestamp = FEFunctions.unixTimestamp(new DateLiteral("2038-01-19 03:14:07", Type.DATETIME));
            // CST time zone
            Assert.assertEquals(Integer.MAX_VALUE - 8 * 3600, timestamp.getValue());

        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void dateDiffTest() throws AnalysisException {
        IntLiteral actualResult = FEFunctions.dateDiff(new DateLiteral("2010-11-30 23:59:59", Type.DATETIME), new DateLiteral("2010-12-31", Type.DATE));
        IntLiteral expectedResult = new IntLiteral(-31);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.dateDiff(new DateLiteral("2010-11-30 23:59:50", Type.DATETIME), new DateLiteral("2010-12-30 23:59:59", Type.DATETIME));
        expectedResult = new IntLiteral(-30);
        Assert.assertEquals(expectedResult, actualResult);
    }
    
    @Test
    public void dateAddTest() throws AnalysisException {
        DateLiteral actualResult = FEFunctions.dateAdd(new DateLiteral("2018-08-08", Type.DATE), new IntLiteral(1));
        DateLiteral expectedResult = new DateLiteral("2018-08-09 00:00:00", Type.DATETIME);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.dateAdd(new DateLiteral("2018-08-08", Type.DATE), new IntLiteral(-1));
        expectedResult = new DateLiteral("2018-08-07 00:00:00", Type.DATETIME);
        Assert.assertEquals(expectedResult, actualResult);
    }
    
    @Test
    public void addDateTest() throws AnalysisException {
        DateLiteral actualResult = FEFunctions.addDate(new DateLiteral("2018-08-08", Type.DATE), new IntLiteral(1));
        DateLiteral expectedResult = new DateLiteral("2018-08-09 00:00:00", Type.DATETIME);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.addDate(new DateLiteral("2018-08-08", Type.DATE), new IntLiteral(-1));
        expectedResult = new DateLiteral("2018-08-07 00:00:00", Type.DATETIME);
        Assert.assertEquals(expectedResult, actualResult);

    }
    
    @Test
    public void daysAddTest() throws AnalysisException {
        DateLiteral actualResult = FEFunctions.daysAdd(new DateLiteral("2018-08-08", Type.DATE), new IntLiteral(1));
        DateLiteral expectedResult = new DateLiteral("2018-08-09", Type.DATE);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.daysAdd(new DateLiteral("2018-08-08", Type.DATE), new IntLiteral(-1));
        expectedResult = new DateLiteral("2018-08-07", Type.DATE);
        Assert.assertEquals(expectedResult, actualResult);
    }
    
    @Test
    public void fromUnixTimeTest() throws AnalysisException {
        StringLiteral actualResult = FEFunctions.fromUnixTime(new IntLiteral(100000));
        StringLiteral expectedResult = new StringLiteral("1970-01-02 11:46:40");
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.fromUnixTime(new IntLiteral(100000), new StringLiteral("%Y-%m-%d"));
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
            Locale.setDefault(Locale.ENGLISH);
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
            Assert.assertEquals("2001", FEFunctions.dateFormat(testDate, new StringLiteral("%Y")).getStringValue());
            Assert.assertEquals("01", FEFunctions.dateFormat(testDate, new StringLiteral("%y")).getStringValue());
            Assert.assertEquals("%", FEFunctions.dateFormat(testDate, new StringLiteral("%%")).getStringValue());
            Assert.assertEquals("foo", FEFunctions.dateFormat(testDate, new StringLiteral("foo")).getStringValue());
            Assert.assertEquals("g", FEFunctions.dateFormat(testDate, new StringLiteral("%g")).getStringValue());
            Assert.assertEquals("4", FEFunctions.dateFormat(testDate, new StringLiteral("%4")).getStringValue());
            Assert.assertEquals("2001 02" ,FEFunctions.dateFormat(testDate, new StringLiteral("%x %v")).getStringValue());
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dateParseTest() {
        try {
            Assert.assertEquals("2019-05-09 00:00:00", FEFunctions.dateParse(new StringLiteral("2019-05-09"), new StringLiteral("%Y-%m-%d %H:%i:%s")).getStringValue());
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
            Assert.assertEquals("0000-00-20", FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%D")).getStringValue());
            Assert.assertEquals("0000-00-00", FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%U")).getStringValue());
            Assert.assertEquals("0000-00-00", FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%u")).getStringValue());
            Assert.assertEquals("0000-00-00", FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%V")).getStringValue());
            Assert.assertEquals("0000-00-00", FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%w")).getStringValue());
            Assert.assertEquals("0000-00-00", FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%x")).getStringValue());
            Assert.assertEquals("0000-00-00", FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%X")).getStringValue());
            Assert.assertEquals("2013-05-17 20:07:05", FEFunctions.dateParse(new StringLiteral("2013-05-17 08:07:05 PM"), new StringLiteral("%Y-%m-%d %r")).getStringValue());
            Assert.assertEquals("2013-05-17 08:07:05", FEFunctions.dateParse(new StringLiteral("2013-05-17 08:07:05"), new StringLiteral("%Y-%m-%d %T")).getStringValue());
        } catch (AnalysisException e) {
            e.printStackTrace();
            fail("Junit test dateParse fail");
        }

        try {
            FEFunctions.dateParse(new StringLiteral("2013-05-17"), new StringLiteral("%W"));
            fail("Junit test dateParse fail");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(), "errCode = 2, detailMessage = '' is invalid");
        }
    }

    @Test
    public void dateSubTest() throws AnalysisException {
        DateLiteral actualResult = FEFunctions.dateSub(new DateLiteral("2018-08-08", Type.DATE), new IntLiteral(1));
        DateLiteral expectedResult = new DateLiteral("2018-08-07", Type.DATE);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.dateSub(new DateLiteral("2018-08-08", Type.DATE), new IntLiteral(-1));
        expectedResult = new DateLiteral("2018-08-09", Type.DATE);
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void yearTest() throws AnalysisException {
        IntLiteral actualResult = FEFunctions.year(new DateLiteral("2018-08-08", Type.DATE));
        IntLiteral expectedResult = new IntLiteral(2018, Type.INT);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.year(new DateLiteral("1970-01-02 11:46:40", Type.DATETIME));
        expectedResult = new IntLiteral(1970, Type.INT);
        Assert.assertEquals(expectedResult, actualResult);
    }
    @Test
    public void monthTest() throws AnalysisException {
        IntLiteral actualResult = FEFunctions.month(new DateLiteral("2018-08-08", Type.DATE));
        IntLiteral expectedResult = new IntLiteral(8, Type.INT);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.month(new DateLiteral("1970-01-02 11:46:40", Type.DATETIME));
        expectedResult = new IntLiteral(1, Type.INT);
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void dayTest() throws AnalysisException {
        IntLiteral actualResult = FEFunctions.day(new DateLiteral("2018-08-08", Type.DATE));
        IntLiteral expectedResult = new IntLiteral(8, Type.INT);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.day(new DateLiteral("1970-01-02 11:46:40", Type.DATETIME));
        expectedResult = new IntLiteral(2, Type.INT);
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void floorTest() throws AnalysisException {
        IntLiteral actualResult = FEFunctions.floor(new FloatLiteral(3.3));
        IntLiteral expectedResult = new IntLiteral(3);
        Assert.assertEquals(expectedResult, actualResult);


        actualResult = FEFunctions.floor(new FloatLiteral(-3.3));
        expectedResult = new IntLiteral(-4);
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void addIntTest() throws AnalysisException {
        IntLiteral actualResult = FEFunctions.addInt(new IntLiteral(1234), new IntLiteral(4321));
        IntLiteral expectedResult = new IntLiteral(5555);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.addInt(new IntLiteral(-1111), new IntLiteral(3333));
        expectedResult = new IntLiteral(2222);
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void addDoubleTest() throws AnalysisException {
        FloatLiteral actualResult = FEFunctions.addDouble(new FloatLiteral(1.1), new FloatLiteral(1.1));
        FloatLiteral expectedResult = new FloatLiteral(2.2);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.addDouble(new FloatLiteral(-1.1), new FloatLiteral(1.1));
        expectedResult = new FloatLiteral(0.0);
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void addDecimalV2Test() throws AnalysisException {
        DecimalLiteral actualResult = FEFunctions.addDecimalV2(new DecimalLiteral("2.2"), new DecimalLiteral("3.3"));
        DecimalLiteral expectedResult = new DecimalLiteral("5.5");
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.addDecimalV2(new DecimalLiteral("-2.2"), new DecimalLiteral("3.3"));
        expectedResult = new DecimalLiteral("1.1");
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void addBigIntTest() throws AnalysisException {
        LargeIntLiteral actualResult = FEFunctions.addBigInt(new LargeIntLiteral("170141183460469231731687303715884105727"), new LargeIntLiteral("-170141183460469231731687303715884105728"));
        LargeIntLiteral expectedResult = new LargeIntLiteral("-1");
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.addBigInt(new LargeIntLiteral("10"), new LargeIntLiteral("-1"));
        expectedResult = new LargeIntLiteral("9");
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void subtractIntTest() throws AnalysisException {
        IntLiteral actualResult = FEFunctions.subtractInt(new IntLiteral(5555), new IntLiteral(4321));
        IntLiteral expectedResult = new IntLiteral(1234);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.subtractInt(new IntLiteral(-3333), new IntLiteral(1111));
        expectedResult = new IntLiteral(-4444);
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void subtractDoubleTest() throws AnalysisException {
        FloatLiteral actualResult = FEFunctions.subtractDouble(new FloatLiteral(1.1), new FloatLiteral(1.1));
        FloatLiteral expectedResult = new FloatLiteral(0.0);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.subtractDouble(new FloatLiteral(-1.1), new FloatLiteral(1.1));
        expectedResult = new FloatLiteral(-2.2);
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void subtractDecimalV2Test() throws AnalysisException {
        DecimalLiteral actualResult = FEFunctions.subtractDecimalV2(new DecimalLiteral("2.2"), new DecimalLiteral("3.3"));
        DecimalLiteral expectedResult = new DecimalLiteral("-1.1");
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.subtractDecimalV2(new DecimalLiteral("5.5"), new DecimalLiteral("3.3"));
        expectedResult = new DecimalLiteral("2.2");
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void subtractBigIntTest() throws AnalysisException {
        LargeIntLiteral actualResult = FEFunctions.subtractBigInt(new LargeIntLiteral("170141183460469231731687303715884105727"), new LargeIntLiteral("170141183460469231731687303715884105728"));
        LargeIntLiteral expectedResult = new LargeIntLiteral("-1");
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.subtractBigInt(new LargeIntLiteral("10"), new LargeIntLiteral("-1"));
        expectedResult = new LargeIntLiteral("11");
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void multiplyIntTest() throws AnalysisException {
        IntLiteral actualResult = FEFunctions.multiplyInt(new IntLiteral(100), new IntLiteral(2));
        IntLiteral expectedResult = new IntLiteral(200);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.multiplyInt(new IntLiteral(-100), new IntLiteral(1));
        expectedResult = new IntLiteral(-100);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.multiplyInt(new IntLiteral(-100), new IntLiteral(-2));
        expectedResult = new IntLiteral(200);
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void multiplyDoubleTest() throws AnalysisException {
        FloatLiteral actualResult = FEFunctions.multiplyDouble(new FloatLiteral(-1.1), new FloatLiteral(1.0));
        FloatLiteral expectedResult = new FloatLiteral(-1.1);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.multiplyDouble(new FloatLiteral(-1.1), new FloatLiteral(-10.0));
        expectedResult = new FloatLiteral(11.0);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.multiplyDouble(new FloatLiteral(-1.1), new FloatLiteral(1.1));
        expectedResult = new FloatLiteral(-1.2100000000000002);
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void multiplyDecimalV2Test() throws AnalysisException {
        DecimalLiteral actualResult = FEFunctions.multiplyDecimalV2(new DecimalLiteral("1.1"), new DecimalLiteral("1.0"));
        DecimalLiteral expectedResult = new DecimalLiteral("1.1");
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.multiplyDecimalV2(new DecimalLiteral("-1.1"), new DecimalLiteral("-10.0"));
        expectedResult = new DecimalLiteral("11.0");
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.multiplyDecimalV2(new DecimalLiteral("-1.1"), new DecimalLiteral("-1.1"));
        expectedResult = new DecimalLiteral("1.21");
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void multiplyBigIntTest() throws AnalysisException {
        LargeIntLiteral actualResult = FEFunctions.multiplyBigInt(new LargeIntLiteral("-170141183460469231731687303715884105727"), new LargeIntLiteral("1"));
        LargeIntLiteral expectedResult = new LargeIntLiteral("-170141183460469231731687303715884105727");
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.multiplyBigInt(new LargeIntLiteral("-10"), new LargeIntLiteral("-1"));
        expectedResult = new LargeIntLiteral("10");
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void divideDoubleTest() throws AnalysisException {
        FloatLiteral actualResult = FEFunctions.divideDouble(new FloatLiteral(-1.1), new FloatLiteral(1.0));
        FloatLiteral expectedResult = new FloatLiteral(-1.1);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.divideDouble(new FloatLiteral(-1.1), new FloatLiteral(-10.0));
        expectedResult = new FloatLiteral(0.11000000000000001);
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.divideDouble(new FloatLiteral(-1.1), new FloatLiteral(1.1));
        expectedResult = new FloatLiteral(-1.0);
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void divideDecimalV2Test() throws AnalysisException {
        DecimalLiteral actualResult = FEFunctions.divideDecimalV2(new DecimalLiteral("1.1"), new DecimalLiteral("1.0"));
        DecimalLiteral expectedResult = new DecimalLiteral("1.1");
        Assert.assertEquals(expectedResult, actualResult);

        actualResult = FEFunctions.divideDecimalV2(new DecimalLiteral("-1.1"), new DecimalLiteral("-10.0"));
        expectedResult = new DecimalLiteral("0.11");
        Assert.assertEquals(expectedResult, actualResult);
    }

    @Test
    public void timeDiffTest() throws AnalysisException {
        DateLiteral d1 = new DateLiteral("1019-02-28 00:00:00", Type.DATETIME);
        DateLiteral d2 = new DateLiteral("2019-02-28 00:00:00", Type.DATETIME);
        DateLiteral d3 = new DateLiteral("2019-03-28 00:00:00", Type.DATETIME);
        Assert.assertEquals(31556995543L, FEFunctions.timeDiff(d2, d1).getLongValue());
        Assert.assertEquals(31559414743L, FEFunctions.timeDiff(d3, d1).getLongValue());
        Assert.assertEquals(2419200, FEFunctions.timeDiff(d3, d2).getLongValue());
    }

    @Test
    public void timeNowTest() throws AnalysisException {
        String curTimeString = FEFunctions.curTime().toSqlImpl().replace("'", "");
        String currentTimestampString = FEFunctions.currentTimestamp().toSqlImpl().replace("'", "");

        String nowTimestampString = new DateTime().toString("yyyy-MM-dd HH:mm:ss");
        Assert.assertTrue(nowTimestampString.compareTo(currentTimestampString) >= 0);
        String nowTimeString = nowTimestampString.substring(nowTimestampString.indexOf(" ") + 1);
        Assert.assertTrue(nowTimeString.compareTo(curTimeString) >= 0);
    }

    @Test
    public void datePlusAndSubTest() throws AnalysisException {
        DateLiteral dateLiteral = new DateLiteral("2019-11-11 00:00:00", Type.DATETIME);

        Assert.assertEquals(new DateLiteral("2020-11-11 00:00:00", Type.DATETIME),
                FEFunctions.yearsAdd(dateLiteral, new IntLiteral(1)));

        Assert.assertEquals(new DateLiteral("2018-11-11 00:00:00", Type.DATETIME),
                FEFunctions.yearsSub(dateLiteral, new IntLiteral(1)));

        Assert.assertEquals(new DateLiteral("2019-12-11 00:00:00", Type.DATETIME),
                FEFunctions.monthsAdd(dateLiteral, new IntLiteral(1)));

        Assert.assertEquals(new DateLiteral("2019-10-11 00:00:00", Type.DATETIME),
                FEFunctions.monthsSub(dateLiteral, new IntLiteral(1)));

        Assert.assertEquals(new DateLiteral("2019-11-12 00:00:00", Type.DATETIME),
                FEFunctions.daysAdd(dateLiteral, new IntLiteral(1)));

        Assert.assertEquals(new DateLiteral("2019-11-10 00:00:00", Type.DATETIME),
                FEFunctions.daysSub(dateLiteral, new IntLiteral(1)));

        Assert.assertEquals(new DateLiteral("2019-11-11 01:00:00", Type.DATETIME),
                FEFunctions.hoursAdd(dateLiteral, new IntLiteral(1)));

        Assert.assertEquals(new DateLiteral("2019-11-10 23:00:00", Type.DATETIME),
                FEFunctions.hoursSub(dateLiteral, new IntLiteral(1)));

        Assert.assertEquals(new DateLiteral("2019-11-11 00:01:00", Type.DATETIME),
                FEFunctions.minutesAdd(dateLiteral, new IntLiteral(1)));

        Assert.assertEquals(new DateLiteral("2019-11-10 23:59:00", Type.DATETIME),
                FEFunctions.minutesSub(dateLiteral, new IntLiteral(1)));

        Assert.assertEquals(new DateLiteral("2019-11-11 00:00:01", Type.DATETIME),
                FEFunctions.secondsAdd(dateLiteral, new IntLiteral(1)));

        Assert.assertEquals(new DateLiteral("2019-11-10 23:59:59", Type.DATETIME),
                FEFunctions.secondsSub(dateLiteral, new IntLiteral(1)));
    }
}

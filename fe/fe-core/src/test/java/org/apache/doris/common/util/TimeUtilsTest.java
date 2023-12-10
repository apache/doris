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

package org.apache.doris.common.util;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

public class TimeUtilsTest {

    @Mocked
    TimeUtils timeUtils;

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
    public void testNormal() {
        Assert.assertNotNull(TimeUtils.getCurrentFormatTime());
        Assert.assertNotNull(TimeUtils.getStartTimeMs());
        Assert.assertTrue(TimeUtils.getElapsedTimeMs(0L) > 0);

        Assert.assertEquals(-62135625600000L, TimeUtils.MIN_DATE.getTime());
        Assert.assertEquals(253402185600000L, TimeUtils.MAX_DATE.getTime());
        Assert.assertEquals(-62135625600000L, TimeUtils.MIN_DATETIME.getTime());
        Assert.assertEquals(253402271999000L, TimeUtils.MAX_DATETIME.getTime());
    }

    @Test
    public void testDateParse() {
        // date
        List<String> validDateList = new LinkedList<>();
        validDateList.add("2013-12-02");
        validDateList.add("2013-12-02");
        validDateList.add("2013-12-2");
        validDateList.add("2013-12-2");
        validDateList.add("9999-12-31");
        validDateList.add("1900-01-01");
        validDateList.add("2013-2-28");
        validDateList.add("0001-01-01");
        for (String validDate : validDateList) {
            try {
                TimeUtils.parseDate(validDate, PrimitiveType.DATE);
            } catch (AnalysisException e) {
                e.printStackTrace();
                System.out.println(validDate);
                Assert.fail();
            }
        }

        List<String> invalidDateList = new LinkedList<>();
        invalidDateList.add("2013-12-02 ");
        invalidDateList.add(" 2013-12-02");
        invalidDateList.add("20131-2-28");
        invalidDateList.add("a2013-2-28");
        invalidDateList.add("2013-22-28");
        invalidDateList.add("2013-2-29");
        invalidDateList.add("2013-2-28 2:3:4");
        for (String invalidDate : invalidDateList) {
            try {
                TimeUtils.parseDate(invalidDate, PrimitiveType.DATE);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Invalid"));
            }
        }

        // datetime
        List<String> validDateTimeList = new LinkedList<>();
        validDateTimeList.add("2013-12-02 13:59:59");
        validDateTimeList.add("2013-12-2 13:59:59");
        validDateTimeList.add("2013-12-2 1:59:59");
        validDateTimeList.add("2013-12-2 3:1:1");
        validDateTimeList.add("9999-12-31 23:59:59");
        validDateTimeList.add("1900-01-01 00:00:00");
        validDateTimeList.add("2013-2-28 23:59:59");
        validDateTimeList.add("2013-2-28 2:3:4");
        validDateTimeList.add("2014-05-07 19:8:50");
        validDateTimeList.add("0001-01-01 00:00:00");
        for (String validDateTime : validDateTimeList) {
            try {
                TimeUtils.parseDate(validDateTime, PrimitiveType.DATETIME);
            } catch (AnalysisException e) {
                e.printStackTrace();
                System.out.println(validDateTime);
                Assert.fail();
            }
        }

        List<String> invalidDateTimeList = new LinkedList<>();
        invalidDateTimeList.add("2013-12-02  12:12:10");
        invalidDateTimeList.add(" 2013-12-02 12:12:10 ");
        invalidDateTimeList.add("20131-2-28 12:12:10");
        invalidDateTimeList.add("a2013-2-28 12:12:10");
        invalidDateTimeList.add("2013-22-28 12:12:10");
        invalidDateTimeList.add("2013-2-29 12:12:10");
        invalidDateTimeList.add("2013-2-28");
        invalidDateTimeList.add("2013-13-01 12:12:12");
        for (String invalidDateTime : invalidDateTimeList) {
            try {
                TimeUtils.parseDate(invalidDateTime, PrimitiveType.DATETIME);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Invalid"));
            }
        }
    }

    @Test
    public void testDateTrans() throws AnalysisException {
        Assert.assertEquals(FeConstants.null_string, TimeUtils.longToTimeString(-2));

        long timestamp = 1426125600000L;
        Assert.assertEquals("2015-03-12 10:00:00", TimeUtils.longToTimeString(timestamp));

        DateLiteral date = new DateLiteral("2015-03-01", ScalarType.DATE);
        Assert.assertEquals(1031777L, date.getRealValue());

        DateLiteral datetime = new DateLiteral("2015-03-01 12:00:00", ScalarType.DATETIME);
        Assert.assertEquals(20150301120000L, datetime.getRealValue());
    }

    @Test
    public void testTimezone() throws AnalysisException {
        try {
            Assert.assertEquals("CST", TimeUtils.checkTimeZoneValidAndStandardize("CST"));
            Assert.assertEquals("EST", TimeUtils.checkTimeZoneValidAndStandardize("EST"));
            Assert.assertEquals("GMT+08:00", TimeUtils.checkTimeZoneValidAndStandardize("GMT+8:00"));
            Assert.assertEquals("UTC+08:00", TimeUtils.checkTimeZoneValidAndStandardize("UTC+8:00"));
            Assert.assertEquals("+08:00", TimeUtils.checkTimeZoneValidAndStandardize("+08:00"));
            Assert.assertEquals("+08:00", TimeUtils.checkTimeZoneValidAndStandardize("+8:00"));
            Assert.assertEquals("-08:00", TimeUtils.checkTimeZoneValidAndStandardize("-8:00"));
            Assert.assertEquals("+08:00", TimeUtils.checkTimeZoneValidAndStandardize("8:00"));
        } catch (DdlException ex) {
            Assert.assertTrue(ex.getMessage(), false);
        }
        try {
            TimeUtils.checkTimeZoneValidAndStandardize("FOO");
            Assert.fail();
        } catch (DdlException ex) {
            Assert.assertTrue(ex.getMessage().contains("Unknown or incorrect time zone: 'FOO'"));
        }
    }

    @Test
    public void testGetHourAsDate() {
        Calendar calendar = Calendar.getInstance();
        Date date = TimeUtils.getHourAsDate("1");
        calendar.setTime(date);
        Assert.assertEquals(1, calendar.get(Calendar.HOUR_OF_DAY));
        date = TimeUtils.getHourAsDate("10");
        calendar.setTime(date);
        Assert.assertEquals(10, calendar.get(Calendar.HOUR_OF_DAY));
        date = TimeUtils.getHourAsDate("24");
        calendar.setTime(date);
        Assert.assertEquals(0, calendar.get(Calendar.HOUR_OF_DAY));
        date = TimeUtils.getHourAsDate("05");
        calendar.setTime(date);
        Assert.assertEquals(5, calendar.get(Calendar.HOUR_OF_DAY));
        date = TimeUtils.getHourAsDate("0");
        calendar.setTime(date);
        Assert.assertEquals(0, calendar.get(Calendar.HOUR_OF_DAY));
        date = TimeUtils.getHourAsDate("13");
        calendar.setTime(date);
        Assert.assertEquals(13, calendar.get(Calendar.HOUR_OF_DAY));
        Assert.assertNull(TimeUtils.getHourAsDate("111"));
        Assert.assertNull(TimeUtils.getHourAsDate("-1"));
    }
}

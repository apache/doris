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

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class TimeUtilsTest {

    @Test
    public void testNormal() {
        Assert.assertNotNull(TimeUtils.getCurrentFormatTime());
        Assert.assertNotNull(TimeUtils.getStartTime());
        Assert.assertTrue(TimeUtils.getEstimatedTime(0L) > 0);

        Assert.assertEquals(-2209017600000L, TimeUtils.MIN_DATE.getTime());
        Assert.assertEquals(253402185600000L, TimeUtils.MAX_DATE.getTime());
        Assert.assertEquals(-2209017600000L, TimeUtils.MIN_DATETIME.getTime());
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
        Assert.assertEquals("N/A", TimeUtils.longToTimeString(-2));

        long timestamp = 1426125600000L;
        Assert.assertEquals("2015-03-12 10:00:00", TimeUtils.longToTimeString(timestamp));

        DateLiteral date = new DateLiteral("2015-03-01", ScalarType.DATE);
        Assert.assertEquals(1031777L, date.getRealValue());

        DateLiteral datetime = new DateLiteral("2015-03-01 12:00:00", ScalarType.DATETIME);
        Assert.assertEquals(20150301120000L, datetime.getRealValue());

        Assert.assertEquals("2015-03-01", TimeUtils.format(date.getValue(), date.getType()));
        Assert.assertEquals("2015-03-01 12:00:00", TimeUtils.format(datetime.getValue(), datetime.getType()));
    }

}

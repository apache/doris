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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.TimeSource;
import org.apache.doris.catalog.TimeSourceFactory;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class DynamicPartitionUtilTest {
    private static final String FORMAT = "yyyy-MM-dd";

    static class TestCase {
        private static final String FORMAT = "yyyy-MM-dd";

        private static Map<String, String> getDynamProp(String timeUnit, int start, int end, int startOfWeek,
                                                        int startOfMonth, TimeSource timeSource) {
            Map<String, String> prop = Maps.newHashMap();
            prop.put(DynamicPartitionProperty.ENABLE, "true");
            prop.put(DynamicPartitionProperty.TIME_UNIT, timeUnit);
            prop.put(DynamicPartitionProperty.START, String.valueOf(start));
            prop.put(DynamicPartitionProperty.END, String.valueOf(end));
            prop.put(DynamicPartitionProperty.PREFIX, "p");
            prop.put(DynamicPartitionProperty.BUCKETS, "1");
            if (startOfWeek > 0) {
                prop.put(DynamicPartitionProperty.START_DAY_OF_WEEK, String.valueOf(startOfWeek));
            }
            if (startOfMonth > 0) {
                prop.put(DynamicPartitionProperty.START_DAY_OF_MONTH, String.valueOf(startOfMonth));
            }
            prop.put(DynamicPartitionProperty.TIME_SOURCE, timeSource.getName());
            return prop;
        }

        private static ZonedDateTime getZonedDateTimeFromStr(String dateStr) throws DateTimeException {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FORMAT);
            return LocalDate.parse(dateStr, formatter).atStartOfDay(
                    TimeUtils.getOrSystemTimeZone(TimeUtils.DEFAULT_TIME_ZONE).toZoneId());
        }

        private static TimeZone getCSTTimeZone() {
            return TimeUtils.getOrSystemTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
        }

        private DynamicPartitionProperty prop;

        public TestCase(String timeUnit, int start, int end, int startOfWeek, int startOfMonth, String timeSourceStr) {
            TimeSource timeSource = TimeSourceFactory.get(timeSourceStr);
            prop = new DynamicPartitionProperty(
                    getDynamProp(timeUnit, start, end, startOfWeek, startOfMonth, timeSource));
        }

        public String getPartitionRangeString(String dateStr, int offset) {
            ZonedDateTime zonedDateTime = getZonedDateTimeFromStr(dateStr);
            return DynamicPartitionUtil.getPartitionRangeString(prop, zonedDateTime, offset, FORMAT);
        }

        public String getPartName(String dateStr) {
            return DynamicPartitionUtil.getFormattedPartitionName(getCSTTimeZone(), prop.getTimeSource(), dateStr,
                    prop.getTimeUnit());
        }
    }


    @Test
    public void testGetPartitionRangeString() throws DateTimeException {
        TestCase testCase;
        String res;
        // TODO(Drogon): rewrite by TestCase[] and expected results[]
        // TimeUnit: DAY
        // 1. 2020-05-25, offset -7
        // emptyTimeSource
        testCase = new TestCase("DAY", -3, 3, -1, -1, "");
        res = testCase.getPartitionRangeString("2020-05-25", -7);
        Assert.assertEquals("2020-05-18", res);
        Assert.assertEquals("20200518", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("DAY", -3, 3, -1, -1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-05-25", -7);
        Assert.assertEquals("1589731200", res);
        Assert.assertEquals("20200518", testCase.getPartName(res));

        // 2. 2020-05-25, offset 0
        // emptyTimeSource
        testCase = new TestCase("DAY", -3, 3, -1, -1, "");
        res = testCase.getPartitionRangeString("2020-05-25", 0);
        Assert.assertEquals("2020-05-25", res);
        Assert.assertEquals("20200525", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("DAY", -3, 3, -1, -1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-05-25", 0);
        Assert.assertEquals("1590336000", res);
        Assert.assertEquals("20200525", testCase.getPartName(res));

        // 3. 2020-05-25, offset 7
        // emptyTimeSource
        testCase = new TestCase("DAY", -3, 3, -1, -1, "");
        res = testCase.getPartitionRangeString("2020-05-25", 7);
        Assert.assertEquals("2020-06-01", res);
        Assert.assertEquals("20200601", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("DAY", -3, 3, -1, -1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-05-25", 7);
        Assert.assertEquals("1590940800", res);
        Assert.assertEquals("20200601", testCase.getPartName(res));

        // 4. 2020-02-28, offset 3
        // emptyTimeSource
        testCase = new TestCase("DAY", -3, 3, -1, -1, "");
        res = testCase.getPartitionRangeString("2020-02-28", 3);
        Assert.assertEquals("2020-03-02", res);
        Assert.assertEquals("20200302", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("DAY", -3, 3, -1, -1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-02-28", 3);
        Assert.assertEquals("1583078400", res);
        Assert.assertEquals("20200302", testCase.getPartName(res));

        // TimeUnit: WEEK
        // 1. 2020-05-25, start day: MONDAY, offset 0
        // emptyTimeSource
        testCase = new TestCase("WEEK", -3, 3, 1, -1, "");
        res = testCase.getPartitionRangeString("2020-05-25", 0);
        Assert.assertEquals("2020-05-25", res);
        Assert.assertEquals("2020_22", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("WEEK", -3, 3, 1, -1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-05-25", 0);
        Assert.assertEquals("1590336000", res);
        Assert.assertEquals("2020_22", testCase.getPartName(res));

        // 2. 2020-05-28, start day: MONDAY, offset 0
        // emptyTimeSource
        testCase = new TestCase("WEEK", -3, 3, 1, -1, "");
        res = testCase.getPartitionRangeString("2020-05-28", 0);
        Assert.assertEquals("2020-05-25", res);
        Assert.assertEquals("2020_22", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("WEEK", -3, 3, 1, -1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-05-28", 0);
        Assert.assertEquals("1590336000", res);
        Assert.assertEquals("2020_22", testCase.getPartName(res));

        // 3. 2020-05-25, start day: SUNDAY, offset 0
        // emptyTimeSource
        testCase = new TestCase("WEEK", -3, 3, 7, -1, "");
        res = testCase.getPartitionRangeString("2020-05-25", 0);
        Assert.assertEquals("2020-05-31", res);
        Assert.assertEquals("2020_23", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("WEEK", -3, 3, 7, -1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-05-25", 0);
        Assert.assertEquals("1590854400", res);
        Assert.assertEquals("2020_23", testCase.getPartName(res));

        // 4. 2020-05-25, start day: MONDAY, offset -2
        // emptyTimeSource
        testCase = new TestCase("WEEK", -3, 3, 1, -1, "");
        res = testCase.getPartitionRangeString("2020-05-25", -2);
        Assert.assertEquals("2020-05-11", res);
        Assert.assertEquals("2020_20", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("WEEK", -3, 3, 1, -1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-05-25", -2);
        Assert.assertEquals("1589126400", res);
        Assert.assertEquals("2020_20", testCase.getPartName(res));

        // 5. 2020-02-29, start day: WED, offset 0
        // emptyTimeSource
        testCase = new TestCase("WEEK", -3, 3, 3, -1, "");
        res = testCase.getPartitionRangeString("2020-02-29", 0);
        Assert.assertEquals("2020-02-26", res);
        Assert.assertEquals("2020_09", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("WEEK", -3, 3, 3, -1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-02-29", 0);
        Assert.assertEquals("1582646400", res);
        Assert.assertEquals("2020_09", testCase.getPartName(res));

        // 6. 2020-02-29, start day: TUS, offset 1
        // emptyTimeSource
        testCase = new TestCase("WEEK", -3, 3, 2, -1, "");
        res = testCase.getPartitionRangeString("2020-02-29", 1);
        Assert.assertEquals("2020-03-03", res);
        Assert.assertEquals("2020_10", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("WEEK", -3, 3, 2, -1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-02-29", 1);
        Assert.assertEquals("1583164800", res);
        Assert.assertEquals("2020_10", testCase.getPartName(res));

        // 6. 2020-01-01, start day: MONDAY, offset -1
        // emptyTimeSource
        testCase = new TestCase("WEEK", -3, 3, 1, -1, "");
        res = testCase.getPartitionRangeString("2020-01-01", -1);
        Assert.assertEquals("2019-12-23", res);
        Assert.assertEquals("2019_52", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("WEEK", -3, 3, 1, -1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-01-01", -1);
        Assert.assertEquals("1577030400", res);
        Assert.assertEquals("2019_52", testCase.getPartName(res));

        // 6. 2020-01-01, start day: MONDAY, offset 0
        // emptyTimeSource
        testCase = new TestCase("WEEK", -3, 3, 1, -1, "");
        res = testCase.getPartitionRangeString("2020-01-01", 0);
        Assert.assertEquals("2019-12-30", res);
        Assert.assertEquals("2019_53", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("WEEK", -3, 3, 1, -1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-01-01", 0);
        Assert.assertEquals("1577635200", res);
        Assert.assertEquals("2019_53", testCase.getPartName(res));

        // TimeUnit: MONTH
        // 1. 2020-05-25, start day: 1, offset 0
        // emptyTimeSource
        testCase = new TestCase("MONTH", -3, 3, -1, 1, "");
        res = testCase.getPartitionRangeString("2020-05-25", 0);
        Assert.assertEquals("2020-05-01", res);
        Assert.assertEquals("202005", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("MONTH", -3, 3, -1, 1, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-05-25", 0);
        Assert.assertEquals("1588262400", res);
        Assert.assertEquals("202005", testCase.getPartName(res));
        // 2. 2020-05-25, start day: 26, offset 0
        // emptyTimeSource
        testCase = new TestCase("MONTH", -3, 3, -1, 26, "");
        res = testCase.getPartitionRangeString("2020-05-25", 0);
        Assert.assertEquals("2020-04-26", res);
        Assert.assertEquals("202004", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("MONTH", -3, 3, -1, 26, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-05-25", 0);
        Assert.assertEquals("1587830400", res);
        Assert.assertEquals("202004", testCase.getPartName(res));

        // 3. 2020-05-25, start day: 26, offset -1
        // emptyTimeSource
        testCase = new TestCase("MONTH", -3, 3, -1, 26, "");
        res = testCase.getPartitionRangeString("2020-05-25", -1);
        Assert.assertEquals("2020-03-26", res);
        Assert.assertEquals("202003", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("MONTH", -3, 3, -1, 26, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-05-25", -1);
        Assert.assertEquals("1585152000", res);
        Assert.assertEquals("202003", testCase.getPartName(res));

        // 4. 2020-02-29, start day: 26, offset 3
        // emptyTimeSource
        testCase = new TestCase("MONTH", -3, 3, -1, 26, "");
        res = testCase.getPartitionRangeString("2020-02-29", 3);
        Assert.assertEquals("2020-05-26", res);
        Assert.assertEquals("202005", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("MONTH", -3, 3, -1, 26, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-02-29", 3);
        Assert.assertEquals("1590422400", res);
        Assert.assertEquals("202005", testCase.getPartName(res));

        // 5. 2020-02-29, start day: 27, offset 0
        // emptyTimeSource
        testCase = new TestCase("MONTH", -3, 3, -1, 27, "");
        res = testCase.getPartitionRangeString("2020-02-29", 0);
        Assert.assertEquals("2020-02-27", res);
        Assert.assertEquals("202002", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("MONTH", -3, 3, -1, 27, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-02-29", 0);
        Assert.assertEquals("1582732800", res);
        Assert.assertEquals("202002", testCase.getPartName(res));

        // 6. 2020-02-29, start day: 27, offset -3
        // emptyTimeSource
        testCase = new TestCase("MONTH", -3, 3, -1, 27, "");
        res = testCase.getPartitionRangeString("2020-02-29", -3);
        Assert.assertEquals("2019-11-27", res);
        Assert.assertEquals("201911", testCase.getPartName(res));
        // unixTimeStampTimeSource
        testCase = new TestCase("MONTH", -3, 3, -1, 27, "unix_timestamp");
        res = testCase.getPartitionRangeString("2020-02-29", -3);
        Assert.assertEquals("1574784000", res);
        Assert.assertEquals("201911", testCase.getPartName(res));
    }

    @Test
    public void testCheckTimeUnit() {
        DynamicPartitionUtil dynamicPartitionUtil = new DynamicPartitionUtil();
        RangePartitionInfo rangePartitionInfo = new RangePartitionInfo();
        Deencapsulation.setField(rangePartitionInfo, "isMultiColumnPartition", false);
        List<Column> partitionColumnList = Lists.newArrayList();
        Column partitionColumn = new Column();
        partitionColumn.setType(Type.DATE);
        Deencapsulation.setField(rangePartitionInfo, partitionColumnList);
        try {
            Deencapsulation.invoke(dynamicPartitionUtil, "checkTimeUnit", "HOUR", rangePartitionInfo);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

}

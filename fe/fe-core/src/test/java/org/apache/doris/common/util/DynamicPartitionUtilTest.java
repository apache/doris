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

    private static Map<String, String> getDynamProp(String timeUnit, int start, int end, int startOfWeek,
                                                    int startOfMonth) {
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
        return prop;
    }

    private static ZonedDateTime getZonedDateTimeFromStr(String dateStr) throws DateTimeException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FORMAT);
        return LocalDate.parse(dateStr, formatter).atStartOfDay(
                TimeUtils.getOrSystemTimeZone(TimeUtils.DEFAULT_TIME_ZONE).toZoneId());
    }

    private static TimeZone getCTSTimeZone() {
        return TimeUtils.getOrSystemTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
    }

    @Test
    public void testGetPartitionRangeString() throws DateTimeException {
        // TimeUnit: DAY

        // 1. 2020-05-25, offset -7
        DynamicPartitionProperty property = new DynamicPartitionProperty(getDynamProp("DAY", -3, 3, -1, -1));
        String res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-05-25"), -7,
                FORMAT);
        Assert.assertEquals("2020-05-18", res);
        String partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "DAY");
        Assert.assertEquals("20200518", partName);
        // 2. 2020-05-25, offset 0
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-05-25"), 0,
                FORMAT);
        Assert.assertEquals("2020-05-25", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "DAY");
        Assert.assertEquals("20200525", partName);
        // 3. 2020-05-25, offset 7
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-05-25"), 7,
                FORMAT);
        Assert.assertEquals("2020-06-01", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "DAY");
        Assert.assertEquals("20200601", partName);
        // 4. 2020-02-28, offset 3
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-02-28"), 3,
                FORMAT);
        Assert.assertEquals("2020-03-02", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "DAY");
        Assert.assertEquals("20200302", partName);

        // TimeUnit: WEEK
        // 1. 2020-05-25, start day: MONDAY, offset 0
        property = new DynamicPartitionProperty(getDynamProp("WEEK", -3, 3, 1, -1));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-05-25"), 0,
                FORMAT);
        Assert.assertEquals("2020-05-25", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "WEEK");
        Assert.assertEquals("2020_22", partName);

        // 2. 2020-05-28, start day: MONDAY, offset 0
        property = new DynamicPartitionProperty(getDynamProp("WEEK", -3, 3, 1, -1));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-05-28"), 0,
                FORMAT);
        Assert.assertEquals("2020-05-25", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "WEEK");
        Assert.assertEquals("2020_22", partName);

        // 3. 2020-05-25, start day: SUNDAY, offset 0
        property = new DynamicPartitionProperty(getDynamProp("WEEK", -3, 3, 7, -1));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-05-25"), 0,
                FORMAT);
        Assert.assertEquals("2020-05-31", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "WEEK");
        Assert.assertEquals("2020_23", partName);

        // 4. 2020-05-25, start day: MONDAY, offset -2
        property = new DynamicPartitionProperty(getDynamProp("WEEK", -3, 3, 1, -1));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-05-25"), -2,
                FORMAT);
        Assert.assertEquals("2020-05-11", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "WEEK");
        Assert.assertEquals("2020_20", partName);

        // 5. 2020-02-29, start day: WED, offset 0
        property = new DynamicPartitionProperty(getDynamProp("WEEK", -3, 3, 3, -1));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-02-29"), 0,
                FORMAT);
        Assert.assertEquals("2020-02-26", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "WEEK");
        Assert.assertEquals("2020_09", partName);

        // 6. 2020-02-29, start day: TUS, offset 1
        property = new DynamicPartitionProperty(getDynamProp("WEEK", -3, 3, 2, -1));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-02-29"), 1,
                FORMAT);
        Assert.assertEquals("2020-03-03", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "WEEK");
        Assert.assertEquals("2020_10", partName);

        // 6. 2020-01-01, start day: MONDAY, offset -1
        property = new DynamicPartitionProperty(getDynamProp("WEEK", -3, 3, 1, -1));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-01-01"), -1,
                FORMAT);
        Assert.assertEquals("2019-12-23", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "WEEK");
        Assert.assertEquals("2019_52", partName);

        // 6. 2020-01-01, start day: MONDAY, offset 0
        property = new DynamicPartitionProperty(getDynamProp("WEEK", -3, 3, 1, -1));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-01-01"), 0,
                FORMAT);
        Assert.assertEquals("2019-12-30", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "WEEK");
        Assert.assertEquals("2019_53", partName);

        // TimeUnit: MONTH
        // 1. 2020-05-25, start day: 1, offset 0
        property = new DynamicPartitionProperty(getDynamProp("MONTH", -3, 3, -1, 1));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-05-25"), 0,
                FORMAT);
        Assert.assertEquals("2020-05-01", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "MONTH");
        Assert.assertEquals("202005", partName);

        // 2. 2020-05-25, start day: 26, offset 0
        property = new DynamicPartitionProperty(getDynamProp("MONTH", -3, 3, -1, 26));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-05-25"), 0,
                FORMAT);
        Assert.assertEquals("2020-04-26", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "MONTH");
        Assert.assertEquals("202004", partName);

        // 3. 2020-05-25, start day: 26, offset -1
        property = new DynamicPartitionProperty(getDynamProp("MONTH", -3, 3, -1, 26));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-05-25"), -1,
                FORMAT);
        Assert.assertEquals("2020-03-26", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "MONTH");
        Assert.assertEquals("202003", partName);

        // 4. 2020-02-29, start day: 26, offset 3
        property = new DynamicPartitionProperty(getDynamProp("MONTH", -3, 3, -1, 26));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-02-29"), 3,
                FORMAT);
        Assert.assertEquals("2020-05-26", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "MONTH");
        Assert.assertEquals("202005", partName);

        // 5. 2020-02-29, start day: 27, offset 0
        property = new DynamicPartitionProperty(getDynamProp("MONTH", -3, 3, -1, 27));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-02-29"), 0,
                FORMAT);
        Assert.assertEquals("2020-02-27", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "MONTH");
        Assert.assertEquals("202002", partName);

        // 6. 2020-02-29, start day: 27, offset -3
        property = new DynamicPartitionProperty(getDynamProp("MONTH", -3, 3, -1, 27));
        res = DynamicPartitionUtil.getPartitionRangeString(property, getZonedDateTimeFromStr("2020-02-29"), -3,
                FORMAT);
        Assert.assertEquals("2019-11-27", res);
        partName = DynamicPartitionUtil.getFormattedPartitionName(getCTSTimeZone(), res, "MONTH");
        Assert.assertEquals("201911", partName);
    }

    @Test
    public void testCheckTimeUnit() {
        DynamicPartitionUtil dynamicPartitionUtil = new DynamicPartitionUtil();
        RangePartitionInfo rangePartitionInfo = new RangePartitionInfo();
        Deencapsulation.setField(rangePartitionInfo, "isMultiColumnPartition", false);
        List<Column> partitionColumnList = Lists.newArrayList();
        Column partitionColumn = new Column();
        partitionColumn.setType(Type.DATE);
        Deencapsulation.setField(rangePartitionInfo, "partitionColumns", partitionColumnList);
        try {
            Deencapsulation.invoke(dynamicPartitionUtil, "checkTimeUnit", "HOUR", rangePartitionInfo);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

}

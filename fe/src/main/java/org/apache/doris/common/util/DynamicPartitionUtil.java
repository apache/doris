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

import org.apache.doris.analysis.TimestampArithmeticExpr.TimeUnit;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.Month;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class DynamicPartitionUtil {
    private static final Logger LOG = LogManager.getLogger(DynamicPartitionUtil.class);

    private static final String TIMESTAMP_FORMAT = "yyyyMMdd";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static void checkTimeUnit(String timeUnit) throws DdlException {
        if (Strings.isNullOrEmpty(timeUnit)
                || !(timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.WEEK.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString()))) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_TIME_UNIT, timeUnit);
        }
    }

    private static void checkPrefix(String prefix) throws DdlException {
        try {
            FeNameFormat.checkPartitionName(prefix);
        } catch (AnalysisException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_PREFIX, prefix);
        }
    }

    private static void checkStart(String start) throws DdlException {
        try {
            if (Integer.parseInt(start) >= 0) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_START_ZERO, start);
            }
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_START_FORMAT, start);
        }
    }

    private static void checkEnd(String end) throws DdlException {
        if (Strings.isNullOrEmpty(end)) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_END_EMPTY);
        }
        try {
            if (Integer.parseInt(end) <= 0) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_END_ZERO, end);
            }
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_END_FORMAT, end);
        }
    }

    private static void checkBuckets(String buckets) throws DdlException {
        if (Strings.isNullOrEmpty(buckets)) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_BUCKETS_EMPTY);
        }
        try {
            if (Integer.parseInt(buckets) <= 0) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_BUCKETS_ZERO, buckets);
            }
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_BUCKETS_FORMAT, buckets);
        }
    }

    private static void checkEnable(String enable) throws DdlException {
        if (Strings.isNullOrEmpty(enable)
                || (!Boolean.TRUE.toString().equalsIgnoreCase(enable) && !Boolean.FALSE.toString().equalsIgnoreCase(enable))) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_ENABLE, enable);
        }
    }

    private static void checkStartDayOfMonth(String val) throws DdlException {
        if (Strings.isNullOrEmpty(val)) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.START_DAY_OF_MONTH);
        }
        try {
            int dayOfMonth = Integer.parseInt(val);
            // only support from 1st to 28th, not allow 29th, 30th and 31th to avoid problems
            // caused by lunar year and lunar month
            if (dayOfMonth < 1 || dayOfMonth > 28) {
                throw new DdlException(DynamicPartitionProperty.START_DAY_OF_MONTH + " should between 1 and 28");
            }
        } catch (NumberFormatException e) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.START_DAY_OF_MONTH);
        }
    }

    private static void checkStartDayOfWeek(String val) throws DdlException {
        if (Strings.isNullOrEmpty(val)) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.START_DAY_OF_WEEK);
        }
        try {
            int dayOfWeek= Integer.parseInt(val);
            if (dayOfWeek< 1 || dayOfWeek > 7) {
                throw new DdlException(DynamicPartitionProperty.START_DAY_OF_WEEK + " should between 1 and 7");
            }
        } catch (NumberFormatException e) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.START_DAY_OF_WEEK);
        }
    }

    private static void checkReplicationNum(String val) throws DdlException {
        if (Strings.isNullOrEmpty(val)) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.REPLICATION_NUM);
        }
        try {
            if (Integer.parseInt(val) <= 0) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_REPLICATION_NUM_ZERO);
            }
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_REPLICATION_NUM_FORMAT, val);
        }
    }

    public static boolean checkDynamicPartitionPropertiesExist(Map<String, String> properties) {
        if (properties == null) {
            return false;
        }
        return properties.containsKey(DynamicPartitionProperty.TIME_UNIT) ||
                properties.containsKey(DynamicPartitionProperty.TIME_ZONE) ||
                properties.containsKey(DynamicPartitionProperty.START) ||
                properties.containsKey(DynamicPartitionProperty.END) ||
                properties.containsKey(DynamicPartitionProperty.PREFIX) ||
                properties.containsKey(DynamicPartitionProperty.BUCKETS) ||
                properties.containsKey(DynamicPartitionProperty.REPLICATION_NUM) ||
                properties.containsKey(DynamicPartitionProperty.ENABLE) ||
                properties.containsKey(DynamicPartitionProperty.START_DAY_OF_WEEK) ||
                properties.containsKey(DynamicPartitionProperty.START_DAY_OF_MONTH);
    }

    private static boolean checkInputDynamicPartitionProperties(Map<String, String> properties, PartitionInfo partitionInfo) throws DdlException{
        if (properties == null || properties.isEmpty()) {
            return false;
        }
        if (partitionInfo.getType() != PartitionType.RANGE || partitionInfo.isMultiColumnPartition()) {
            throw new DdlException("Dynamic partition only support single-column range partition");
        }
        String timeUnit = properties.get(DynamicPartitionProperty.TIME_UNIT);
        String prefix = properties.get(DynamicPartitionProperty.PREFIX);
        String start = properties.get(DynamicPartitionProperty.START);
        String timeZone = properties.get(DynamicPartitionProperty.TIME_ZONE);
        String end = properties.get(DynamicPartitionProperty.END);
        String buckets = properties.get(DynamicPartitionProperty.BUCKETS);
        String replicationNum = properties.get(DynamicPartitionProperty.REPLICATION_NUM);
        String enable = properties.get(DynamicPartitionProperty.ENABLE);
        if (!((Strings.isNullOrEmpty(enable) &&
                Strings.isNullOrEmpty(timeUnit) &&
                Strings.isNullOrEmpty(timeZone) &&
                Strings.isNullOrEmpty(prefix) &&
                Strings.isNullOrEmpty(start) &&
                Strings.isNullOrEmpty(end) &&
                Strings.isNullOrEmpty(buckets)))) {
            if (Strings.isNullOrEmpty(enable)) {
                properties.put(DynamicPartitionProperty.ENABLE, "true");
            }
            if (Strings.isNullOrEmpty(timeUnit)) {
                throw new DdlException("Must assign dynamic_partition.time_unit properties");
            }
            if (Strings.isNullOrEmpty(prefix)) {
                throw new DdlException("Must assign dynamic_partition.prefix properties");
            }
            if (Strings.isNullOrEmpty(start)) {
                properties.put(DynamicPartitionProperty.START, String.valueOf(Integer.MIN_VALUE));
            }
            if (Strings.isNullOrEmpty(end)) {
                throw new DdlException("Must assign dynamic_partition.end properties");
            }
            if (Strings.isNullOrEmpty(buckets)) {
                throw new DdlException("Must assign dynamic_partition.buckets properties");
            }
            if (Strings.isNullOrEmpty(timeZone)) {
                properties.put(DynamicPartitionProperty.TIME_ZONE, TimeUtils.getSystemTimeZone().getID());
            }
        }
        return true;
    }

    public static void registerOrRemoveDynamicPartitionTable(long dbId, OlapTable olapTable) {
        if (olapTable.getTableProperty() != null
                && olapTable.getTableProperty().getDynamicPartitionProperty() != null) {
            if (olapTable.getTableProperty().getDynamicPartitionProperty().getEnable()) {
                Catalog.getCurrentCatalog().getDynamicPartitionScheduler().registerDynamicPartitionTable(dbId, olapTable.getId());
            } else {
                Catalog.getCurrentCatalog().getDynamicPartitionScheduler().removeDynamicPartitionTable(dbId, olapTable.getId());
            }
        }
    }

    public static Map<String, String> analyzeDynamicPartition(Map<String, String> properties) throws DdlException {
        // properties should not be empty, check properties before call this function
        Map<String, String> analyzedProperties = new HashMap<>();
        if (properties.containsKey(DynamicPartitionProperty.TIME_UNIT)) {
            String timeUnitValue = properties.get(DynamicPartitionProperty.TIME_UNIT);
            checkTimeUnit(timeUnitValue);
            properties.remove(DynamicPartitionProperty.TIME_UNIT);
            analyzedProperties.put(DynamicPartitionProperty.TIME_UNIT, timeUnitValue);
        }
        if (properties.containsKey(DynamicPartitionProperty.PREFIX)) {
            String prefixValue = properties.get(DynamicPartitionProperty.PREFIX);
            checkPrefix(prefixValue);
            properties.remove(DynamicPartitionProperty.PREFIX);
            analyzedProperties.put(DynamicPartitionProperty.PREFIX, prefixValue);
        }
        if (properties.containsKey(DynamicPartitionProperty.END)) {
            String endValue = properties.get(DynamicPartitionProperty.END);
            checkEnd(endValue);
            properties.remove(DynamicPartitionProperty.END);
            analyzedProperties.put(DynamicPartitionProperty.END, endValue);
        }
        if (properties.containsKey(DynamicPartitionProperty.BUCKETS)) {
            String bucketsValue = properties.get(DynamicPartitionProperty.BUCKETS);
            checkBuckets(bucketsValue);
            properties.remove(DynamicPartitionProperty.BUCKETS);
            analyzedProperties.put(DynamicPartitionProperty.BUCKETS, bucketsValue);
        }
        if (properties.containsKey(DynamicPartitionProperty.ENABLE)) {
            String enableValue = properties.get(DynamicPartitionProperty.ENABLE);
            checkEnable(enableValue);
            properties.remove(DynamicPartitionProperty.ENABLE);
            analyzedProperties.put(DynamicPartitionProperty.ENABLE, enableValue);
        }
        // If dynamic property is not specified.Use Integer.MIN_VALUE as default
        if (properties.containsKey(DynamicPartitionProperty.START)) {
            String startValue = properties.get(DynamicPartitionProperty.START);
            checkStart(startValue);
            properties.remove(DynamicPartitionProperty.START);
            analyzedProperties.put(DynamicPartitionProperty.START, startValue);
        }
        
        if (properties.containsKey(DynamicPartitionProperty.START_DAY_OF_MONTH)) {
            String val = properties.get(DynamicPartitionProperty.START_DAY_OF_MONTH);
            checkStartDayOfMonth(val);
            properties.remove(DynamicPartitionProperty.START_DAY_OF_MONTH);
            analyzedProperties.put(DynamicPartitionProperty.START_DAY_OF_MONTH, val);
        }
        
        if (properties.containsKey(DynamicPartitionProperty.START_DAY_OF_WEEK)) {
            String val = properties.get(DynamicPartitionProperty.START_DAY_OF_WEEK);
            checkStartDayOfWeek(val);
            properties.remove(DynamicPartitionProperty.START_DAY_OF_WEEK);
            analyzedProperties.put(DynamicPartitionProperty.START_DAY_OF_WEEK, val);
        }

        if (properties.containsKey(DynamicPartitionProperty.TIME_ZONE)) {
            String val = properties.get(DynamicPartitionProperty.TIME_ZONE);
            TimeUtils.checkTimeZoneValidAndStandardize(val);
            properties.remove(DynamicPartitionProperty.TIME_ZONE);
            analyzedProperties.put(DynamicPartitionProperty.TIME_ZONE, val);
        }
        if (properties.containsKey(DynamicPartitionProperty.REPLICATION_NUM)) {
            String val = properties.get(DynamicPartitionProperty.REPLICATION_NUM);
            checkReplicationNum(val);
            properties.remove(DynamicPartitionProperty.REPLICATION_NUM);
            analyzedProperties.put(DynamicPartitionProperty.REPLICATION_NUM, val);
        }
        return analyzedProperties;
    }

    public static void checkAlterAllowed(OlapTable olapTable) throws DdlException {
        TableProperty tableProperty = olapTable.getTableProperty();
        if (tableProperty != null && tableProperty.getDynamicPartitionProperty() != null &&
                tableProperty.getDynamicPartitionProperty().isExist() &&
                tableProperty.getDynamicPartitionProperty().getEnable()) {
            throw new DdlException("Cannot add/drop partition on a Dynamic Partition Table, set `dynamic_partition.enable` to false firstly.");
        }
    }

    public static boolean isDynamicPartitionTable(Table table) {
        if (!(table instanceof OlapTable) ||
                !(((OlapTable) table).getPartitionInfo().getType().equals(PartitionType.RANGE))) {
            return false;
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) ((OlapTable) table).getPartitionInfo();
        TableProperty tableProperty = ((OlapTable) table).getTableProperty();
        if (tableProperty == null || !tableProperty.getDynamicPartitionProperty().isExist()) {
            return false;
        }

        return rangePartitionInfo.getPartitionColumns().size() == 1 && tableProperty.getDynamicPartitionProperty().getEnable();
    }

    /**
     * properties should be checked before call this method
     */
    public static void checkAndSetDynamicPartitionProperty(OlapTable olapTable, Map<String, String> properties) throws DdlException {
        if (DynamicPartitionUtil.checkInputDynamicPartitionProperties(properties, olapTable.getPartitionInfo())) {
            Map<String, String> dynamicPartitionProperties = DynamicPartitionUtil.analyzeDynamicPartition(properties);
            TableProperty tableProperty = olapTable.getTableProperty();
            if (tableProperty != null) {
                tableProperty.modifyTableProperties(dynamicPartitionProperties);
                tableProperty.buildDynamicProperty();
            } else {
                olapTable.setTableProperty(new TableProperty(dynamicPartitionProperties).buildDynamicProperty());
            }
        }
    }

    public static String getPartitionFormat(Column column) throws DdlException {
        if (column.getDataType().equals(PrimitiveType.DATE)) {
            return DATE_FORMAT;
        } else if (column.getDataType().equals(PrimitiveType.DATETIME)) {
            return DATETIME_FORMAT;
        } else if (PrimitiveType.getIntegerTypes().contains(column.getDataType())) {
            // TODO: For Integer Type, only support format it as yyyyMMdd now
            return TIMESTAMP_FORMAT;
        } else {
            throw new DdlException("Dynamic Partition Only Support DATE, DATETIME and INTEGER Type Now.");
        }
    }

    public static String getFormattedPartitionName(TimeZone tz, String formattedDateStr, String timeUnit) {
        formattedDateStr = formattedDateStr.replace("-", "").replace(":", "").replace(" ", "");
        if (timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())) {
            return formattedDateStr.substring(0, 8);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString())) {
            return formattedDateStr.substring(0, 6);
        } else {
            formattedDateStr = formattedDateStr.substring(0, 8);
            Calendar calendar = Calendar.getInstance(tz);
            try {
                calendar.setTime(new SimpleDateFormat("yyyyMMdd").parse(formattedDateStr));
            } catch (ParseException e) {
                LOG.warn("Format dynamic partition name error. Error={}", e.getMessage());
                return formattedDateStr;
            }
            int weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR);
            if (weekOfYear <= 1 && calendar.get(Calendar.MONTH) >= 11) {
                // eg: JDK think 2019-12-30 as the first week of year 2020, we need to handle this.
                // to make it as the 53rd week of year 2019.
                weekOfYear += 52;
            }
            return String.format("%s_%02d", calendar.get(Calendar.YEAR), weekOfYear);
        }
    }

    // return the partition range date string formatted as yyyy-MM-dd[ HH:mm::ss]
    // TODO: support HOUR and YEAR
    public static String getPartitionRangeString(DynamicPartitionProperty property, ZonedDateTime current,
            int offset, String format) {
        String timeUnit = property.getTimeUnit();
        if (timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())) {
            return getPartitionRangeOfDay(current, offset, format);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.WEEK.toString())) {
            return getPartitionRangeOfWeek(current, offset, property.getStartOfWeek(), format);
        } else { // MONTH
            return getPartitionRangeOfMonth(current, offset, property.getStartOfMonth(), format);
        }
    }
    
    /**
     * return formatted string of partition range in DAY granularity.
     * offset: The offset from the current day. 0 means current day, 1 means tomorrow, -1 means yesterday.
     * format: the format of the return date string.
     * 
     * Eg:
     *  Today is 2020-05-24, offset = -1
     *  It will return 2020-05-23
     */
    private static String getPartitionRangeOfDay(ZonedDateTime current, int offset, String format) {
        return getFormattedTimeWithoutHourMinuteSecond(current.plusDays(offset), format);
    }

    /**
     * return formatted string of partition range in WEEK granularity.
     * offset: The offset from the current week. 0 means current week, 1 means next week, -1 means last week.
     * startOf: Define the start day of each week. 1 means MONDAY, 7 means SUNDAY.
     * format: the format of the return date string.
     * 
     * Eg:
     *  Today is 2020-05-24, offset = -1, startOf.dayOfWeek = 3
     *  It will return 2020-05-20  (Wednesday of last week)
     */
    private static String getPartitionRangeOfWeek(ZonedDateTime current, int offset, StartOfDate startOf, String format) {
        Preconditions.checkArgument(startOf.isStartOfWeek());
        // 1. get the offset week
        ZonedDateTime offsetWeek = current.plusWeeks(offset);
        // 2. get the date of `startOf` week
        int day = offsetWeek.getDayOfWeek().getValue();
        ZonedDateTime resultTime = offsetWeek.plusDays(startOf.dayOfWeek - day);
        return getFormattedTimeWithoutHourMinuteSecond(resultTime, format);
    }

    /**
     * return formatted string of partition range in MONTH granularity.
     * offset: The offset from the current month. 0 means current month, 1 means next month, -1 means last month.
     * startOf: Define the start date of each month. 1 means start on the 1st of every month.
     * format: the format of the return date string.
     * 
     * Eg:
     *  Today is 2020-05-24, offset = 1, startOf.month = 3
     *  It will return 2020-06-03 
     */
    private static String getPartitionRangeOfMonth(ZonedDateTime current, int offset, StartOfDate startOf, String format) {
        Preconditions.checkArgument(startOf.isStartOfMonth());
        // 1. Get the offset date.
        int realOffset = offset;
        int currentDay = current.getDayOfMonth();
        if (currentDay < startOf.day) {
            // eg: today is 2020-05-20, `startOf.day` is 25, and offset is 0.
            // we should return 2020-04-25, which is the last month.
            realOffset -= 1;
        }
        ZonedDateTime resultTime = current.plusMonths(realOffset).withDayOfMonth(startOf.day);
        return getFormattedTimeWithoutHourMinuteSecond(resultTime, format);
    }

    private static String getFormattedTimeWithoutHourMinuteSecond(ZonedDateTime zonedDateTime, String format) {
        ZonedDateTime timeWithoutHourMinuteSecond = zonedDateTime.withHour(0).withMinute(0).withSecond(0);
        return DateTimeFormatter.ofPattern(format).format(timeWithoutHourMinuteSecond);
    }

    /**
     * Used to indicate the start date.
     * Taking the year as the granularity, it can indicate the month and day as the start date.
     * Taking the month as the granularity, it can indicate the date of as the start date.
     * Taking the week as the granularity, it can indicate the day of the week as the starting date.
     */
    public static class StartOfDate {
        public int month;
        public int day;
        public int dayOfWeek;

        public StartOfDate(int month, int day, int dayOfWeek) {
            this.month = month;
            this.day = day;
            this.dayOfWeek = dayOfWeek;
        }

        public boolean isStartOfYear() {
            return this.month != -1 && this.day != -1 && this.dayOfWeek == -1;
        }

        public boolean isStartOfMonth() {
            return this.month == -1 && this.day != -1 && this.dayOfWeek == -1;
        }

        public boolean isStartOfWeek() {
            return this.month == -1 && this.day == -1 && this.dayOfWeek != -1;
        }

        public String toDisplayInfo() {
            if (isStartOfWeek()) {
                return DayOfWeek.of(dayOfWeek).name();
            } else if (isStartOfMonth()) {
                return Util.ordinal(day);
            } else if (isStartOfYear()) {
                return Month.of(month) + " " + Util.ordinal(day);
            } else {
                return FeConstants.null_string;
            }
        }

        @Override
        public String toString() {
            // TODO Auto-generated method stub
            return super.toString();
        }
    }
}

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
import org.apache.doris.catalog.AutomaticPartitionProperty;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AutomaticPartitionUtil {
    private static final Logger LOG = LogManager.getLogger(AutomaticPartitionUtil.class);

    public static final String TIMESTAMP_FORMAT = "yyyyMMdd";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static void checkTimeUnit(String timeUnit, PartitionInfo partitionInfo) throws DdlException {
        if (Strings.isNullOrEmpty(timeUnit)
                || !(timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.WEEK.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.YEAR.toString()))) {
            ErrorReport.reportDdlException("Unsupported time unit %s. Expect HOUR/DAY/WEEK/MONTH/YEAR.", timeUnit);
        }
        Preconditions.checkState(partitionInfo instanceof RangePartitionInfo);
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        Preconditions.checkState(!rangePartitionInfo.isMultiColumnPartition());
        Column partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
        if ((partitionColumn.getDataType() == PrimitiveType.DATE
                || partitionColumn.getDataType() == PrimitiveType.DATEV2)
                && (timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString()))) {
            ErrorReport.reportDdlException(AutomaticPartitionProperty.TIME_UNIT + " could not be "
                    + TimeUnit.HOUR + " when type of partition column "
                    + partitionColumn.getDisplayName() + " is " + PrimitiveType.DATE + " or " + PrimitiveType.DATEV2);
        } else if (PrimitiveType.getIntegerTypes().contains(partitionColumn.getDataType())
                && timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())) {
            // The partition column's type is INT, not support HOUR
            ErrorReport.reportDdlException(AutomaticPartitionProperty.TIME_UNIT + " could not be "
                    + TimeUnit.HOUR.toString() + " when type of partition column "
                    + partitionColumn.getDisplayName() + " is Integer");
        }
    }

    public static void checkPrefix(String prefix) throws DdlException {
        try {
            FeNameFormat.checkPartitionName(prefix);
        } catch (AnalysisException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_PREFIX, prefix);
        }
    }

    private static void checkEnable(String enable) throws DdlException {
        if (Strings.isNullOrEmpty(enable)
                || (!Boolean.TRUE.toString().equalsIgnoreCase(enable)
                && !Boolean.FALSE.toString().equalsIgnoreCase(enable))) {
            ErrorReport.reportDdlException("Invalid automatic partition enable: %s. Expected true or false", enable);
        }
    }

    public static void checkAndSetAutomaticPartitionProperty(OlapTable olapTable, Map<String, String> properties,
                                                           Database db) throws UserException {
        if (AutomaticPartitionUtil.checkInputAutomaticPartitionProperties(properties, olapTable)) {
            Map<String, String> automaticPartitionProperties =
                    AutomaticPartitionUtil.analyzeAutomaticPartition(properties, olapTable, db);
            TableProperty tableProperty = olapTable.getTableProperty();
            if (tableProperty != null) {
                tableProperty.modifyTableProperties(automaticPartitionProperties);
                tableProperty.buildAutomaticProperty();
            } else {
                olapTable.setTableProperty(new TableProperty(automaticPartitionProperties).buildAutomaticProperty());
            }
        }
    }

    public static boolean checkAutomaticPartitionPropertiesExist(Map<String, String> properties) {
        if (properties == null) {
            return false;
        }

        for (String key : properties.keySet()) {
            if (key.startsWith(AutomaticPartitionProperty.AUTOMATIC_PARTITION_PROPERTY_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAutomaticPartitionTable(Table table) {
        if (!(table instanceof OlapTable)
                || !(((OlapTable) table).getPartitionInfo().getType().equals(PartitionType.RANGE))) {
            return false;
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) ((OlapTable) table).getPartitionInfo();
        TableProperty tableProperty = ((OlapTable) table).getTableProperty();
        if (tableProperty == null || !tableProperty.getAutomaticPartitionProperty().isExist()) {
            return false;
        }

        return rangePartitionInfo.getPartitionColumns().size() == 1
            && tableProperty.getAutomaticPartitionProperty().getEnable();
    }

    // Check if all requried properties has been set.
    // And also check all optional properties, if not set, set them to default value.
    public static boolean checkInputAutomaticPartitionProperties(Map<String, String> properties,
            OlapTable olapTable) throws DdlException {
        if (properties == null || properties.isEmpty()) {
            return false;
        }

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.getType() != PartitionType.RANGE || partitionInfo.isMultiColumnPartition()) {
            throw new DdlException("automatic partition only support single-column range partition");
        }
        String timeUnit = properties.get(AutomaticPartitionProperty.TIME_UNIT);
        String prefix = properties.get(AutomaticPartitionProperty.PREFIX);
        String enable = properties.get(AutomaticPartitionProperty.ENABLE);

        if (!(Strings.isNullOrEmpty(enable)
                && Strings.isNullOrEmpty(timeUnit)
                && Strings.isNullOrEmpty(prefix))) {
            if (Strings.isNullOrEmpty(enable)) {
                properties.put(AutomaticPartitionProperty.ENABLE, "true");
            }
            if (Strings.isNullOrEmpty(timeUnit)) {
                throw new DdlException("Must assign automatic_partition.time_unit properties");
            }
            if (Strings.isNullOrEmpty(prefix)) {
                throw new DdlException("Must assign automatic_partition.prefix properties");
            }
        }
        return true;
    }

    // Analyze all properties to check their validation
    public static Map<String, String> analyzeAutomaticPartition(Map<String, String> properties,
            OlapTable olapTable, Database db) throws UserException {
        // properties should not be empty, check properties before call this function
        Map<String, String> analyzedProperties = new HashMap<>();
        if (properties.containsKey(AutomaticPartitionProperty.TIME_UNIT)) {
            String timeUnitValue = properties.get(AutomaticPartitionProperty.TIME_UNIT);
            checkTimeUnit(timeUnitValue, olapTable.getPartitionInfo());
            properties.remove(AutomaticPartitionProperty.TIME_UNIT);
            analyzedProperties.put(AutomaticPartitionProperty.TIME_UNIT, timeUnitValue);
        }
        if (properties.containsKey(AutomaticPartitionProperty.PREFIX)) {
            String prefixValue = properties.get(AutomaticPartitionProperty.PREFIX);
            checkPrefix(prefixValue);
            properties.remove(AutomaticPartitionProperty.PREFIX);
            analyzedProperties.put(AutomaticPartitionProperty.PREFIX, prefixValue);
        }

        if (properties.containsKey(AutomaticPartitionProperty.ENABLE)) {
            String enableValue = properties.get(AutomaticPartitionProperty.ENABLE);
            checkEnable(enableValue);
            properties.remove(AutomaticPartitionProperty.ENABLE);
            analyzedProperties.put(AutomaticPartitionProperty.ENABLE, enableValue);
        }

        return analyzedProperties;
    }

    public static String getPartitionFormat(Column column) throws DdlException {
        if (column.getDataType().equals(PrimitiveType.DATE) || column.getDataType().equals(PrimitiveType.DATEV2)) {
            return DATE_FORMAT;
        } else if (column.getDataType().equals(PrimitiveType.DATETIME)
                || column.getDataType().equals(PrimitiveType.DATETIMEV2)) {
            return DATETIME_FORMAT;
        } else {
            throw new DdlException("Automatic Partition Only Support DATE, DATETIME Now.");
        }
    }

    public static String getFormattedPartitionName(String formattedDateStr, String timeUnit) {
        formattedDateStr = formattedDateStr.replace("-", "").replace(":", "").replace(" ", "");
        if (timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())) {
            return formattedDateStr.substring(0, 8);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString())) {
            return formattedDateStr.substring(0, 6);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())) {
            return formattedDateStr.substring(0, 10);
        } else { // YEAR
            return formattedDateStr.substring(0, 4);
        }
    }

    public static String getPartitionRangeString(AutomaticPartitionProperty property, LocalDateTime current,
                                                 int offset, String format) {
        String timeUnit = property.getTimeUnit();
        if (timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())) {
            return getPartitionRangeOfDay(current, offset, format);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())) {
            return getPartitionRangeOfHour(current, offset, format);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString())) {
            return getPartitionRangeOfMonth(current, offset, format);
        } else { // YEAR
            return getPartitionRangeOfYEAR(current, offset, format);
        }
    }

    public static String getPartitionRangeOfHour(LocalDateTime current, int offset, String format) {
        return getFormattedTimeWithoutMinuteSecond(current.plusHours(offset), format);
    }

    private static String getPartitionRangeOfDay(LocalDateTime current, int offset, String format) {
        return getFormattedTimeWithoutHourMinuteSecond(current.plusDays(offset), format);
    }

    private static String getPartitionRangeOfMonth(LocalDateTime current, int offset, String format) {
        LocalDateTime resultTime = current.plusMonths(offset).withDayOfMonth(1);
        return getFormattedTimeWithoutHourMinuteSecond(resultTime, format);
    }

    private static String getPartitionRangeOfYEAR(LocalDateTime current, int offset, String format) {
        LocalDateTime resultTime = current.plusYears(offset).withMonth(1).withDayOfMonth(1);
        return getFormattedTimeWithoutHourMinuteSecond(resultTime, format);
    }

    private static String getFormattedTimeWithoutHourMinuteSecond(LocalDateTime zonedDateTime, String format) {
        LocalDateTime timeWithoutHourMinuteSecond = zonedDateTime.withHour(0).withMinute(0).withSecond(0);
        return DateTimeFormatter.ofPattern(format).format(timeWithoutHourMinuteSecond);
    }

    private static String getFormattedTimeWithoutMinuteSecond(LocalDateTime zonedDateTime, String format) {
        LocalDateTime timeWithoutMinuteSecond = zonedDateTime.withMinute(0).withSecond(0);
        return DateTimeFormatter.ofPattern(format).format(timeWithoutMinuteSecond);
    }

    public static List<Range> convertStringToPeriodsList(String reservedHistoryPeriods, String timeUnit)
            throws DdlException {
        List<Range> reservedHistoryPeriodsToRangeList = new ArrayList<Range>();
        if (AutomaticPartitionProperty.NOT_SET_RESERVED_HISTORY_PERIODS.equals(reservedHistoryPeriods)) {
            return reservedHistoryPeriodsToRangeList;
        }

        Pattern pattern = getPattern(timeUnit);
        Matcher matcher = pattern.matcher(reservedHistoryPeriods);
        while (matcher.find()) {
            String lowerBorderOfReservedHistory = matcher.group(1);
            String upperBorderOfReservedHistory = matcher.group(2);
            if (lowerBorderOfReservedHistory.compareTo(upperBorderOfReservedHistory) > 0) {
                ErrorReport.reportDdlException(
                        ErrorCode.ERROR_DYNAMIC_PARTITION_RESERVED_HISTORY_PERIODS_START_LARGER_THAN_ENDS,
                        lowerBorderOfReservedHistory, upperBorderOfReservedHistory);
            } else {
                reservedHistoryPeriodsToRangeList.add(
                        Range.closed(lowerBorderOfReservedHistory, upperBorderOfReservedHistory));
            }
        }
        return reservedHistoryPeriodsToRangeList;
    }

    private static Pattern getPattern(String timeUnit) {
        if (timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())) {
            return Pattern.compile("\\[([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"
                + ",([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})\\]");
        } else {
            return Pattern.compile("\\[([0-9]{4}-[0-9]{2}-[0-9]{2}),([0-9]{4}-[0-9]{2}-[0-9]{2})\\]");
        }
    }
}

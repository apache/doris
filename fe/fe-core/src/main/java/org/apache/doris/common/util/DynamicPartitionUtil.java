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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.policy.StoragePolicy;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DynamicPartitionUtil {
    private static final Logger LOG = LogManager.getLogger(DynamicPartitionUtil.class);

    public static final String TIMESTAMP_FORMAT = "yyyyMMdd";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static void checkTimeUnit(String timeUnit, PartitionInfo partitionInfo) throws DdlException {
        if (Strings.isNullOrEmpty(timeUnit)
                || !(timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.WEEK.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString()))) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_TIME_UNIT, timeUnit);
        }
        Preconditions.checkState(partitionInfo instanceof RangePartitionInfo);
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        Preconditions.checkState(!rangePartitionInfo.isMultiColumnPartition());
        Column partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
        if ((partitionColumn.getDataType() == PrimitiveType.DATE)
                && (timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString()))) {
            ErrorReport.reportDdlException(DynamicPartitionProperty.TIME_UNIT + " could not be "
                    + TimeUnit.HOUR.toString() + " when type of partition column "
                    + partitionColumn.getDisplayName() + " is " + PrimitiveType.DATE.toString());
        } else if (PrimitiveType.getIntegerTypes().contains(partitionColumn.getDataType())
                && timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())) {
            // The partition column's type is INT, not support HOUR
            ErrorReport.reportDdlException(DynamicPartitionProperty.TIME_UNIT + " could not be "
                    + TimeUnit.HOUR.toString() + " when type of partition column "
                    + partitionColumn.getDisplayName() + " is Integer");
        }
    }

    private static void checkPrefix(String prefix) throws DdlException {
        try {
            FeNameFormat.checkPartitionName(prefix);
        } catch (AnalysisException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_PREFIX, prefix);
        }
    }

    private static int checkStart(String start) throws DdlException {
        try {
            int startInt = Integer.parseInt(start);
            if (startInt >= 0) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_START_ZERO, start);
            }
            return startInt;
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_START_FORMAT, start);
        }
        return DynamicPartitionProperty.MIN_START_OFFSET;
    }

    private static int checkEnd(String end) throws DdlException {
        if (Strings.isNullOrEmpty(end)) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_END_EMPTY);
        }
        try {
            int endInt = Integer.parseInt(end);
            if (endInt <= 0) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_END_ZERO, end);
            }
            return endInt;
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_END_FORMAT, end);
        }
        return DynamicPartitionProperty.MAX_END_OFFSET;
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
                || (!Boolean.TRUE.toString().equalsIgnoreCase(enable)
                && !Boolean.FALSE.toString().equalsIgnoreCase(enable))) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_ENABLE, enable);
        }
    }

    private static boolean checkCreateHistoryPartition(String create) throws DdlException {
        if (Strings.isNullOrEmpty(create)
                || (!Boolean.TRUE.toString().equalsIgnoreCase(create)
                && !Boolean.FALSE.toString().equalsIgnoreCase(create))) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_CREATE_HISTORY_PARTITION, create);
        }
        return Boolean.valueOf(create);
    }

    private static void checkHistoryPartitionNum(String val) throws DdlException {
        if (Strings.isNullOrEmpty(val)) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.HISTORY_PARTITION_NUM);
        }
        try {
            int historyPartitionNum = Integer.parseInt(val);
            if (historyPartitionNum < 0
                    && historyPartitionNum != DynamicPartitionProperty.NOT_SET_HISTORY_PARTITION_NUM) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_HISTORY_PARTITION_NUM_ZERO);
            }
        } catch (NumberFormatException e) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.HISTORY_PARTITION_NUM);
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
            int dayOfWeek = Integer.parseInt(val);
            if (dayOfWeek < 1 || dayOfWeek > 7) {
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

    private static void checkReplicaAllocation(ReplicaAllocation replicaAlloc, Database db) throws DdlException {
        if (replicaAlloc.getTotalReplicaNum() <= 0) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_REPLICATION_NUM_ZERO);
        }
        Env.getCurrentSystemInfo().selectBackendIdsForReplicaCreation(replicaAlloc, db.getClusterName(), null);
    }

    private static void checkHotPartitionNum(String val) throws DdlException {
        if (Strings.isNullOrEmpty(val)) {
            throw new DdlException("Invalid properties: " + DynamicPartitionProperty.HOT_PARTITION_NUM);
        }
        try {
            if (Integer.parseInt(val) < 0) {
                throw new DdlException(DynamicPartitionProperty.HOT_PARTITION_NUM + " must larger than 0.");
            }
        } catch (NumberFormatException e) {
            throw new DdlException("Invalid " + DynamicPartitionProperty.HOT_PARTITION_NUM + " value");
        }
    }

    public static List<Range> convertStringToPeriodsList(String reservedHistoryPeriods, String timeUnit)
            throws DdlException {
        List<Range> reservedHistoryPeriodsToRangeList = new ArrayList<Range>();
        if (DynamicPartitionProperty.NOT_SET_RESERVED_HISTORY_PERIODS.equals(reservedHistoryPeriods)) {
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

    public static String sortedListedToString(String reservedHistoryPeriods, String timeUnit) throws DdlException {
        if (DynamicPartitionProperty.NOT_SET_RESERVED_HISTORY_PERIODS.equals(reservedHistoryPeriods)) {
            return reservedHistoryPeriods;
        }
        List<Range> reservedHistoryPeriodsToRangeList = convertStringToPeriodsList(reservedHistoryPeriods, timeUnit);
        reservedHistoryPeriodsToRangeList.sort(new Comparator<Range>() {
            @Override
            public int compare(Range o1, Range o2) {
                return o1.lowerEndpoint().compareTo(o2.lowerEndpoint());
            }
        });
        List<String> sortedReservedHistoryPeriods = reservedHistoryPeriodsToRangeList.stream()
                .map(e -> "[" + e.lowerEndpoint() + "," + e.upperEndpoint() + "]").collect(Collectors.toList());

        return String.join(",", sortedReservedHistoryPeriods);
    }

    private static void checkReservedHistoryPeriodValidate(String reservedHistoryPeriods,
            String timeUnit) throws DdlException {
        if (Strings.isNullOrEmpty(reservedHistoryPeriods)) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_RESERVED_HISTORY_PERIODS_EMPTY);
        }
        if (DynamicPartitionProperty.NOT_SET_RESERVED_HISTORY_PERIODS.equals(reservedHistoryPeriods)) {
            return;
        }
        // it has 5 kinds of situation
        // 1. "dynamic_partition.reserved_history_periods" = "[2021-07-01,]," invalid one
        // 2. "dynamic_partition.reserved_history_periods" = "2021-07-01", invalid. It must be surrounded by []
        // 1. "dynamic_partition.reserved_history_periods" = "[2021-07-01,]" invalid one, needs pairs of values
        // 2. "dynamic_partition.reserved_history_periods" = "[,2021-08-01]" invalid one, needs pairs of values
        // 3. "dynamic_partition.reserved_history_periods" = "[2021-07-01,2020-08-01,]" invalid format
        if (!reservedHistoryPeriods.startsWith("[") || !reservedHistoryPeriods.endsWith("]")) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_RESERVED_HISTORY_PERIODS_INVALID,
                    DynamicPartitionProperty.RESERVED_HISTORY_PERIODS, reservedHistoryPeriods);
        }

        List<Range> reservedHistoryPeriodsToRangeList = convertStringToPeriodsList(reservedHistoryPeriods, timeUnit);
        Integer sizeOfPeriods = reservedHistoryPeriods.split("],\\[").length;
        SimpleDateFormat sdf = getSimpleDateFormat(timeUnit);

        if (reservedHistoryPeriodsToRangeList.size() != sizeOfPeriods) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_RESERVED_HISTORY_PERIODS_INVALID,
                    DynamicPartitionProperty.RESERVED_HISTORY_PERIODS, reservedHistoryPeriods);
        } else {
            try {
                for (Range range : reservedHistoryPeriodsToRangeList) {
                    String formattedLowerBound = sdf.format(sdf.parse(range.lowerEndpoint().toString()));
                    String formattedUpperBound = sdf.format(sdf.parse(range.upperEndpoint().toString()));
                    if (!range.lowerEndpoint().toString().equals(formattedLowerBound)
                            || !range.upperEndpoint().toString().equals(formattedUpperBound)) {
                        throw new DdlException("Invalid " + DynamicPartitionProperty.RESERVED_HISTORY_PERIODS
                                + " value. It must be correct DATE value \"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\""
                                + " while time_unit is DAY/WEEK/MONTH or"
                                + " \"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR.");
                    }
                }
            } catch (ParseException e) {
                throw new DdlException("Invalid " + DynamicPartitionProperty.RESERVED_HISTORY_PERIODS
                        + " value. It must be like \"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\""
                        + " while time_unit is DAY/WEEK/MONTH "
                        + "or \"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR.");
            }
        }
    }

    private static void checkRemoteStoragePolicy(String policyName) throws DdlException {
        if (Strings.isNullOrEmpty(policyName)) {
            LOG.info(DynamicPartitionProperty.REMOTE_STORAGE_POLICY + " is null, remove this key");
            return;
        }
        if (policyName.isEmpty()) {
            throw new DdlException(DynamicPartitionProperty.REMOTE_STORAGE_POLICY + " is empty.");
        }
        StoragePolicy checkedPolicyCondition = StoragePolicy.ofCheck(policyName);
        if (!Env.getCurrentEnv().getPolicyMgr().existPolicy(checkedPolicyCondition)) {
            throw new DdlException(
                    DynamicPartitionProperty.REMOTE_STORAGE_POLICY + ": " + policyName + " doesn't exist.");
        }
        StoragePolicy storagePolicy = (StoragePolicy) Env.getCurrentEnv().getPolicyMgr()
                .getPolicy(checkedPolicyCondition);
        if (Strings.isNullOrEmpty(storagePolicy.getCooldownTtl())) {
            throw new DdlException("Storage policy cooldown type need to be cooldownTtl for properties "
                    + DynamicPartitionProperty.REMOTE_STORAGE_POLICY + ": " + policyName);
        }
    }

    private static SimpleDateFormat getSimpleDateFormat(String timeUnit) {
        if (timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())) {
            return new SimpleDateFormat(DATETIME_FORMAT);
        } else {
            return new SimpleDateFormat(DATE_FORMAT);
        }
    }

    public static boolean checkDynamicPartitionPropertiesExist(Map<String, String> properties) {
        if (properties == null) {
            return false;
        }

        for (String key : properties.keySet()) {
            if (key.startsWith(DynamicPartitionProperty.DYNAMIC_PARTITION_PROPERTY_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    // Check if all requried properties has been set.
    // And also check all optional properties, if not set, set them to default value.
    public static boolean checkInputDynamicPartitionProperties(Map<String, String> properties,
            PartitionInfo partitionInfo) throws DdlException {
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
        String enable = properties.get(DynamicPartitionProperty.ENABLE);
        String createHistoryPartition = properties.get(DynamicPartitionProperty.CREATE_HISTORY_PARTITION);
        String historyPartitionNum = properties.get(DynamicPartitionProperty.HISTORY_PARTITION_NUM);
        String reservedHistoryPeriods = properties.get(DynamicPartitionProperty.RESERVED_HISTORY_PERIODS);

        if (!(Strings.isNullOrEmpty(enable)
                && Strings.isNullOrEmpty(timeUnit)
                && Strings.isNullOrEmpty(timeZone)
                && Strings.isNullOrEmpty(prefix)
                && Strings.isNullOrEmpty(start)
                && Strings.isNullOrEmpty(end)
                && Strings.isNullOrEmpty(buckets)
                && Strings.isNullOrEmpty(createHistoryPartition)
                && Strings.isNullOrEmpty(historyPartitionNum)
                && Strings.isNullOrEmpty(reservedHistoryPeriods))) {
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
            if (Strings.isNullOrEmpty(createHistoryPartition)) {
                properties.put(DynamicPartitionProperty.CREATE_HISTORY_PARTITION, "false");
            }
            if (Strings.isNullOrEmpty(historyPartitionNum)) {
                properties.put(DynamicPartitionProperty.HISTORY_PARTITION_NUM,
                        String.valueOf(DynamicPartitionProperty.NOT_SET_HISTORY_PARTITION_NUM));
            }
            if (Strings.isNullOrEmpty(reservedHistoryPeriods)) {
                properties.put(DynamicPartitionProperty.RESERVED_HISTORY_PERIODS,
                        String.valueOf(DynamicPartitionProperty.NOT_SET_RESERVED_HISTORY_PERIODS));
            }
        }
        return true;
    }

    public static void registerOrRemoveDynamicPartitionTable(long dbId, OlapTable olapTable, boolean isReplay) {
        if (olapTable.getTableProperty() != null
                && olapTable.getTableProperty().getDynamicPartitionProperty() != null) {
            if (olapTable.getTableProperty().getDynamicPartitionProperty().getEnable()) {
                if (!isReplay) {
                    // execute create partition first time only in master of FE, So no need execute
                    // when it's replay
                    Env.getCurrentEnv().getDynamicPartitionScheduler()
                            .executeDynamicPartitionFirstTime(dbId, olapTable.getId());
                }
                Env.getCurrentEnv().getDynamicPartitionScheduler()
                        .registerDynamicPartitionTable(dbId, olapTable.getId());
            } else {
                Env.getCurrentEnv().getDynamicPartitionScheduler()
                        .removeDynamicPartitionTable(dbId, olapTable.getId());
            }
        }
    }

    // Analyze all properties to check their validation
    public static Map<String, String> analyzeDynamicPartition(Map<String, String> properties,
            OlapTable olapTable, Database db) throws UserException {
        // properties should not be empty, check properties before call this function
        Map<String, String> analyzedProperties = new HashMap<>();
        if (properties.containsKey(DynamicPartitionProperty.TIME_UNIT)) {
            String timeUnitValue = properties.get(DynamicPartitionProperty.TIME_UNIT);
            checkTimeUnit(timeUnitValue, olapTable.getPartitionInfo());
            properties.remove(DynamicPartitionProperty.TIME_UNIT);
            analyzedProperties.put(DynamicPartitionProperty.TIME_UNIT, timeUnitValue);
        }
        if (properties.containsKey(DynamicPartitionProperty.PREFIX)) {
            String prefixValue = properties.get(DynamicPartitionProperty.PREFIX);
            checkPrefix(prefixValue);
            properties.remove(DynamicPartitionProperty.PREFIX);
            analyzedProperties.put(DynamicPartitionProperty.PREFIX, prefixValue);
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

        // If dynamic property "start" is not specified, use Integer.MIN_VALUE as default
        int start = DynamicPartitionProperty.MIN_START_OFFSET;
        if (properties.containsKey(DynamicPartitionProperty.START)) {
            String startValue = properties.get(DynamicPartitionProperty.START);
            start = checkStart(startValue);
            properties.remove(DynamicPartitionProperty.START);
            analyzedProperties.put(DynamicPartitionProperty.START, startValue);
        }

        int end = DynamicPartitionProperty.MAX_END_OFFSET;
        boolean hasEnd = false;
        if (properties.containsKey(DynamicPartitionProperty.END)) {
            String endValue = properties.get(DynamicPartitionProperty.END);
            end = checkEnd(endValue);
            properties.remove(DynamicPartitionProperty.END);
            analyzedProperties.put(DynamicPartitionProperty.END, endValue);
            hasEnd = true;
        }

        boolean createHistoryPartition = false;
        if (properties.containsKey(DynamicPartitionProperty.CREATE_HISTORY_PARTITION)) {
            String val = properties.get(DynamicPartitionProperty.CREATE_HISTORY_PARTITION);
            createHistoryPartition = checkCreateHistoryPartition(val);
            properties.remove(DynamicPartitionProperty.CREATE_HISTORY_PARTITION);
            analyzedProperties.put(DynamicPartitionProperty.CREATE_HISTORY_PARTITION, val);
        }

        if (properties.containsKey(DynamicPartitionProperty.HISTORY_PARTITION_NUM)) {
            String val = properties.get(DynamicPartitionProperty.HISTORY_PARTITION_NUM);
            checkHistoryPartitionNum(val);
            properties.remove(DynamicPartitionProperty.HISTORY_PARTITION_NUM);
            analyzedProperties.put(DynamicPartitionProperty.HISTORY_PARTITION_NUM, val);
        }

        // Check the number of dynamic partitions that need to be created to avoid creating too many partitions at once.
        // If create_history_partition is false, history partition is not considered.
        // If create_history_partition is true, will pre-create history partition according the valid value from
        // start and history_partition_num.
        //
        int expectCreatePartitionNum = 0;
        if (!createHistoryPartition) {
            start = 0;
            expectCreatePartitionNum = end - start;
        } else {
            int historyPartitionNum = Integer.parseInt(analyzedProperties.getOrDefault(
                    DynamicPartitionProperty.HISTORY_PARTITION_NUM,
                    String.valueOf(DynamicPartitionProperty.NOT_SET_HISTORY_PARTITION_NUM)));
            if (historyPartitionNum != DynamicPartitionProperty.NOT_SET_HISTORY_PARTITION_NUM) {
                expectCreatePartitionNum = end - Math.max(start, -historyPartitionNum);
            } else {
                if (start == Integer.MIN_VALUE) {
                    throw new DdlException("Provide start or history_partition_num property"
                            + " when creating history partition");
                }
                expectCreatePartitionNum = end - start;
            }
        }
        if (hasEnd && (expectCreatePartitionNum > Config.max_dynamic_partition_num)
                && Boolean.parseBoolean(analyzedProperties.getOrDefault(DynamicPartitionProperty.ENABLE, "true"))) {
            throw new DdlException("Too many dynamic partitions: "
                    + expectCreatePartitionNum + ". Limit: " + Config.max_dynamic_partition_num);
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
            analyzedProperties.put(DynamicPartitionProperty.REPLICATION_ALLOCATION,
                    new ReplicaAllocation(Short.valueOf(val)).toCreateStmt());
        }

        if (properties.containsKey(DynamicPartitionProperty.REPLICATION_ALLOCATION)) {
            ReplicaAllocation replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "dynamic_partition");
            checkReplicaAllocation(replicaAlloc, db);
            properties.remove(DynamicPartitionProperty.REPLICATION_ALLOCATION);
            analyzedProperties.put(DynamicPartitionProperty.REPLICATION_ALLOCATION, replicaAlloc.toCreateStmt());
        } else {
            checkReplicaAllocation(olapTable.getDefaultReplicaAllocation(), db);
        }

        if (properties.containsKey(DynamicPartitionProperty.HOT_PARTITION_NUM)) {
            String val = properties.get(DynamicPartitionProperty.HOT_PARTITION_NUM);
            checkHotPartitionNum(val);
            properties.remove(DynamicPartitionProperty.HOT_PARTITION_NUM);
            analyzedProperties.put(DynamicPartitionProperty.HOT_PARTITION_NUM, val);
        }
        if (properties.containsKey(DynamicPartitionProperty.RESERVED_HISTORY_PERIODS)) {
            String reservedHistoryPeriods = properties.get(DynamicPartitionProperty.RESERVED_HISTORY_PERIODS);
            checkReservedHistoryPeriodValidate(reservedHistoryPeriods,
                    analyzedProperties.get(DynamicPartitionProperty.TIME_UNIT));
            properties.remove(DynamicPartitionProperty.RESERVED_HISTORY_PERIODS);
            analyzedProperties.put(DynamicPartitionProperty.RESERVED_HISTORY_PERIODS, reservedHistoryPeriods);
        }
        if (properties.containsKey(DynamicPartitionProperty.REMOTE_STORAGE_POLICY)) {
            String remoteStoragePolicy = properties.get(DynamicPartitionProperty.REMOTE_STORAGE_POLICY);
            checkRemoteStoragePolicy(remoteStoragePolicy);
            properties.remove(DynamicPartitionProperty.REMOTE_STORAGE_POLICY);
            if (!Strings.isNullOrEmpty(remoteStoragePolicy)) {
                analyzedProperties.put(DynamicPartitionProperty.REMOTE_STORAGE_POLICY, remoteStoragePolicy);
            }
        }
        return analyzedProperties;
    }

    public static void checkAlterAllowed(OlapTable olapTable) throws DdlException {
        TableProperty tableProperty = olapTable.getTableProperty();
        if (tableProperty != null && tableProperty.getDynamicPartitionProperty() != null
                && tableProperty.getDynamicPartitionProperty().isExist()
                && tableProperty.getDynamicPartitionProperty().getEnable()) {
            throw new DdlException("Cannot add/drop partition on a Dynamic Partition Table, "
                    + "Use command `ALTER TABLE tbl_name SET (\"dynamic_partition.enable\" = \"false\")` firstly.");
        }
    }

    public static boolean isDynamicPartitionTable(Table table) {
        if (!(table instanceof OlapTable)
                || !(((OlapTable) table).getPartitionInfo().getType().equals(PartitionType.RANGE))) {
            return false;
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) ((OlapTable) table).getPartitionInfo();
        TableProperty tableProperty = ((OlapTable) table).getTableProperty();
        if (tableProperty == null || !tableProperty.getDynamicPartitionProperty().isExist()) {
            return false;
        }

        return rangePartitionInfo.getPartitionColumns().size() == 1
                && tableProperty.getDynamicPartitionProperty().getEnable();
    }

    /**
     * properties should be checked before call this method
     */
    public static void checkAndSetDynamicPartitionProperty(OlapTable olapTable, Map<String, String> properties,
            Database db) throws UserException {
        if (DynamicPartitionUtil.checkInputDynamicPartitionProperties(properties, olapTable.getPartitionInfo())) {
            Map<String, String> dynamicPartitionProperties =
                    DynamicPartitionUtil.analyzeDynamicPartition(properties, olapTable, db);
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
        if (column.getDataType().equals(PrimitiveType.DATE) || column.getDataType().equals(PrimitiveType.DATEV2)) {
            return DATE_FORMAT;
        } else if (column.getDataType().equals(PrimitiveType.DATETIME)
                || column.getDataType().equals(PrimitiveType.DATETIMEV2)) {
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
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())) {
            return formattedDateStr.substring(0, 10);
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
    // add support: HOUR by caoyang10
    // TODO: support YEAR
    public static String getPartitionRangeString(DynamicPartitionProperty property, ZonedDateTime current,
                                                 int offset, String format) {
        String timeUnit = property.getTimeUnit();
        if (timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())) {
            return getPartitionRangeOfDay(current, offset, format);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.WEEK.toString())) {
            return getPartitionRangeOfWeek(current, offset, property.getStartOfWeek(), format);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())) {
            return getPartitionRangeOfHour(current, offset, format);
        } else { // MONTH
            return getPartitionRangeOfMonth(current, offset, property.getStartOfMonth(), format);
        }
    }

    public static String getHistoryPartitionRangeString(DynamicPartitionProperty dynamicPartitionProperty,
            String time, String format) {
        ZoneId zoneId = dynamicPartitionProperty.getTimeZone().toZoneId();
        Date date = null;
        Timestamp timestamp = null;
        String timeUnit = dynamicPartitionProperty.getTimeUnit();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.s").withZone(zoneId);
        SimpleDateFormat simpleDateFormat = getSimpleDateFormat(timeUnit);
        try {
            date = simpleDateFormat.parse(time);
        } catch (ParseException e) {
            LOG.warn("Parse dynamic partition periods error. Error={}", e.getMessage());
            return getFormattedTimeWithoutMinuteSecond(
                    ZonedDateTime.parse(timestamp.toString(), dateTimeFormatter), format);
        }
        timestamp = new Timestamp(date.getTime());
        return getFormattedTimeWithoutMinuteSecond(
                ZonedDateTime.parse(timestamp.toString(), dateTimeFormatter), format);
    }

    /**
     * return formatted string of partition range in HOUR granularity.
     * offset: The offset from the current hour. 0 means current hour, 1 means next hour, -1 last hour.
     * format: the format of the return date string.
     * <p>
     * Eg:
     * Today is 2020-05-24 00:12:34, offset = -1
     * It will return 2020-05-23 23:00:00
     * Today is 2020-05-24 00, offset = 1
     * It will return 2020-05-24 01:00:00
     */
    public static String getPartitionRangeOfHour(ZonedDateTime current, int offset, String format) {
        return getFormattedTimeWithoutMinuteSecond(current.plusHours(offset), format);
    }

    /**
     * return formatted string of partition range in DAY granularity.
     * offset: The offset from the current day. 0 means current day, 1 means tomorrow, -1 means yesterday.
     * format: the format of the return date string.
     * <p>
     * Eg:
     * Today is 2020-05-24, offset = -1
     * It will return 2020-05-23
     */
    private static String getPartitionRangeOfDay(ZonedDateTime current, int offset, String format) {
        return getFormattedTimeWithoutHourMinuteSecond(current.plusDays(offset), format);
    }

    /**
     * return formatted string of partition range in WEEK granularity.
     * offset: The offset from the current week. 0 means current week, 1 means next week, -1 means last week.
     * startOf: Define the start day of each week. 1 means MONDAY, 7 means SUNDAY.
     * format: the format of the return date string.
     * <p>
     * Eg:
     * Today is 2020-05-24, offset = -1, startOf.dayOfWeek = 3
     * It will return 2020-05-20  (Wednesday of last week)
     */
    private static String getPartitionRangeOfWeek(ZonedDateTime current, int offset,
            StartOfDate startOf, String format) {
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
     * <p>
     * Eg:
     * Today is 2020-05-24, offset = 1, startOf.month = 3
     * It will return 2020-06-03
     */
    private static String getPartitionRangeOfMonth(ZonedDateTime current,
            int offset, StartOfDate startOf, String format) {
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

    private static String getFormattedTimeWithoutMinuteSecond(ZonedDateTime zonedDateTime, String format) {
        ZonedDateTime timeWithoutMinuteSecond = zonedDateTime.withMinute(0).withSecond(0);
        return DateTimeFormatter.ofPattern(format).format(timeWithoutMinuteSecond);
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

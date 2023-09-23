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

import org.apache.doris.analysis.TimestampArithmeticExpr.TimeUnit;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.TimeUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.WeekFields;
import java.util.List;
import java.util.Map;

// to describe the key range multi partition's information in create table stmt
public class MultiPartitionDesc implements AllPartitionDesc {
    public static final String HOURS_FORMAT = "yyyyMMddHH";
    public static final String HOUR_FORMAT = "yyyy-MM-dd HH";
    public static final String DATES_FORMAT = "yyyyMMdd";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String MONTHS_FORMAT = "yyyyMM";
    public static final String MONTH_FORMAT = "yyyy-MM";
    public static final String YEAR_FORMAT = "yyyy";
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";


    private LocalDateTime startTime;
    private LocalDateTime endTime;

    private String startString;
    private String endString;

    private DateTimeFormatter startDateTimeFormat;
    private DateTimeFormatter endDateTimeFormat;


    private Long interval;
    private final PartitionKeyDesc partitionKeyDesc;
    private TimeUnit timeUnitType;
    private final Map<String, String> properties;
    private final List<SinglePartitionDesc> singlePartitionDescList = Lists.newArrayList();

    private final ImmutableSet<TimeUnit> timeUnitTypeMultiPartition = ImmutableSet.of(
            TimeUnit.HOUR,
            TimeUnit.DAY,
            TimeUnit.WEEK,
            TimeUnit.MONTH,
            TimeUnit.YEAR
    );

    private final Integer maxAllowedLimit = Config.max_multi_partition_num;
    //multi_partition_name_prefix default: p_
    private final String partitionPrefix = Config.multi_partition_name_prefix;

    public MultiPartitionDesc(PartitionKeyDesc partitionKeyDesc,
            Map<String, String> properties) throws AnalysisException {
        this.partitionKeyDesc = partitionKeyDesc;
        this.properties = properties;
        this.intervalTrans();
        this.trans();
    }

    /**
     * for Nereids
     */
    public MultiPartitionDesc(PartitionKeyDesc partitionKeyDesc, ReplicaAllocation replicaAllocation,
            Map<String, String> properties) throws AnalysisException {
        this.partitionKeyDesc = partitionKeyDesc;
        this.properties = properties;
        this.intervalTrans();
        this.trans();
        for (SinglePartitionDesc desc : getSinglePartitionDescList()) {
            desc.setReplicaAlloc(replicaAllocation);
            desc.setAnalyzed(true);
        }
    }

    public List<SinglePartitionDesc> getSinglePartitionDescList() throws AnalysisException {
        if (singlePartitionDescList.size() == 0) {
            if (this.timeUnitType == null) {
                buildNumberMultiPartitionToSinglePartitionDesc();
            } else {
                buildTimeMultiPartitionToSinglePartitionDesc();
            }
        }
        return singlePartitionDescList;
    }


    private List<SinglePartitionDesc> buildNumberMultiPartitionToSinglePartitionDesc() throws AnalysisException {
        long countNum = 0;
        long beginNum;
        long endNum;
        String partitionPrefix = this.partitionPrefix;


        try {
            beginNum = Long.parseLong(startString);
            endNum = Long.parseLong(endString);
        } catch (NumberFormatException ex) {
            throw new AnalysisException("Batch build partition INTERVAL is number type "
                    + "but From or TO does not type match.");
        }
        if (beginNum >= endNum) {
            throw new AnalysisException("Batch build partition From value should less then TO value.");
        }

        while (beginNum < endNum) {
            String partitionName = partitionPrefix + beginNum;
            PartitionValue lowerPartitionValue = new PartitionValue(beginNum);
            beginNum += interval;
            Long currentEnd = Math.min(beginNum, endNum);
            PartitionValue upperPartitionValue = new PartitionValue(currentEnd);
            partitionName = partitionName + "_" + currentEnd;
            PartitionKeyDesc partitionKeyDesc = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(lowerPartitionValue),
                    Lists.newArrayList(upperPartitionValue)
            );
            singlePartitionDescList.add(
                    new SinglePartitionDesc(
                            false,
                            partitionName,
                            partitionKeyDesc,
                            properties)
            );
            countNum++;
            if (countNum > maxAllowedLimit) {
                throw new AnalysisException("The number of Multi partitions too much, should not exceed:"
                        + maxAllowedLimit);
            }
        }
        return singlePartitionDescList;
    }

    private List<SinglePartitionDesc> buildTimeMultiPartitionToSinglePartitionDesc() throws AnalysisException {
        String partitionName;
        long countNum = 0;
        int startDayOfWeek = 1;
        int startDayOfMonth = 1;
        String partitionPrefix = this.partitionPrefix;
        LocalDateTime startTime = this.startTime;
        if (properties != null) {
            if (properties.containsKey(DynamicPartitionProperty.START_DAY_OF_WEEK)) {
                String dayOfWeekStr = properties.get(DynamicPartitionProperty.START_DAY_OF_WEEK);
                try {
                    DynamicPartitionUtil.checkStartDayOfWeek(dayOfWeekStr);
                } catch (DdlException e) {
                    throw new AnalysisException(e.getMessage());
                }
                startDayOfWeek = Integer.parseInt(dayOfWeekStr);
            }
            if (properties.containsKey(DynamicPartitionProperty.START_DAY_OF_MONTH)) {
                String dayOfMonthStr = properties.get(DynamicPartitionProperty.START_DAY_OF_MONTH);
                try {
                    DynamicPartitionUtil.checkStartDayOfMonth(dayOfMonthStr);
                } catch (DdlException e) {
                    throw new AnalysisException(e.getMessage());
                }
                startDayOfMonth = Integer.parseInt(dayOfMonthStr);
            }

            if (properties.containsKey(DynamicPartitionProperty.CREATE_HISTORY_PARTITION)) {
                properties.put(DynamicPartitionProperty.CREATE_HISTORY_PARTITION, "false");
            }
            if (properties.containsKey(DynamicPartitionProperty.PREFIX)) {
                partitionPrefix = properties.get(DynamicPartitionProperty.PREFIX);
                try {
                    DynamicPartitionUtil.checkPrefix(partitionPrefix);
                } catch (DdlException e) {
                    throw new AnalysisException(e.getMessage());
                }
            }
        }
        WeekFields weekFields = WeekFields.of(DayOfWeek.of(startDayOfWeek), 1);
        while (startTime.isBefore(this.endTime)) {
            PartitionValue lowerPartitionValue = new PartitionValue(
                    startTime.format(dateTypeFormat(partitionKeyDesc.getLowerValues().get(0).getStringValue()))
            );
            switch (this.timeUnitType) {
                case HOUR:
                    partitionName = partitionPrefix + startTime.format(DateTimeFormatter.ofPattern(HOURS_FORMAT));
                    startTime = startTime.plusHours(interval);
                    break;
                case DAY:
                    partitionName = partitionPrefix + startTime.format(DateTimeFormatter.ofPattern(DATES_FORMAT));
                    startTime = startTime.plusDays(interval);
                    break;
                case WEEK:
                    LocalDate localDate = LocalDate.of(startTime.getYear(), startTime.getMonthValue(),
                            startTime.getDayOfMonth());
                    int weekOfYear = localDate.get(weekFields.weekOfYear());
                    partitionName = String.format("%s%s_%02d", partitionPrefix,
                            startTime.format(DateTimeFormatter.ofPattern(YEAR_FORMAT)), weekOfYear);
                    startTime = startTime.with(ChronoField.DAY_OF_WEEK, startDayOfMonth);
                    startTime = startTime.plusWeeks(interval);
                    break;
                case MONTH:
                    partitionName = partitionPrefix + startTime.format(DateTimeFormatter.ofPattern(MONTHS_FORMAT));
                    startTime = startTime.withDayOfMonth(startDayOfMonth);
                    startTime = startTime.plusMonths(interval);
                    break;
                case YEAR:
                    partitionName = partitionPrefix + startTime.format(DateTimeFormatter.ofPattern(YEAR_FORMAT));
                    startTime = startTime.withDayOfYear(1);
                    startTime = startTime.plusYears(interval);
                    break;
                default:
                    throw new AnalysisException("Multi build partition does not support time interval type: "
                            + this.timeUnitType);
            }
            if (this.timeUnitType != TimeUnit.DAY && startTime.isAfter(this.endTime)) {
                startTime = this.endTime;
            }
            PartitionValue upperPartitionValue = new PartitionValue(
                    startTime.format(dateTypeFormat(partitionKeyDesc.getUpperValues().get(0).getStringValue()))
            );
            PartitionKeyDesc partitionKeyDesc = PartitionKeyDesc.createFixed(
                    Lists.newArrayList(lowerPartitionValue),
                    Lists.newArrayList(upperPartitionValue)
            );
            singlePartitionDescList.add(
                    new SinglePartitionDesc(
                            false,
                            partitionName,
                            partitionKeyDesc,
                            properties)
            );

            countNum++;
            if (countNum > maxAllowedLimit) {
                throw new AnalysisException("The number of Multi partitions too much, should not exceed:"
                        + maxAllowedLimit);
            }
        }
        return singlePartitionDescList;
    }

    private void trans() throws AnalysisException {

        if (partitionKeyDesc.getLowerValues().size() != 1 || partitionKeyDesc.getUpperValues().size() != 1) {
            throw new AnalysisException("partition column number in multi partition clause must be one "
                    + "but start column size is " + partitionKeyDesc.getLowerValues().size()
                    + ", end column size is " + partitionKeyDesc.getUpperValues().size() + ".");
        }

        this.startString = partitionKeyDesc.getLowerValues().get(0).getStringValue();
        this.endString = partitionKeyDesc.getUpperValues().get(0).getStringValue();

        if (this.timeUnitType == null) {
            if (this.startString.compareTo(this.endString) >= 0) {
                throw new AnalysisException("Multi build partition start number should less than end number.");
            }
        } else {
            try {
                this.startDateTimeFormat = dateFormat(this.timeUnitType, this.startString);
                this.endDateTimeFormat = dateFormat(this.timeUnitType, this.endString);
                this.startTime = TimeUtils.formatDateTimeAndFullZero(this.startString, startDateTimeFormat);
                this.endTime = TimeUtils.formatDateTimeAndFullZero(this.endString, endDateTimeFormat);
            } catch (Exception e) {
                throw new AnalysisException("Multi build partition start or end time style is illegal.");
            }

            if (!this.startTime.isBefore(this.endTime)) {
                throw new AnalysisException("Multi build partition start time should less than end time.");
            }
        }

    }


    private void intervalTrans() throws AnalysisException {
        this.interval = partitionKeyDesc.getTimeInterval();
        String timeType = partitionKeyDesc.getTimeType();

        if (timeType == null) {
            throw new AnalysisException("Unknown time interval type for Multi build partition.");
        }

        if (this.interval <= 0) {
            throw new AnalysisException("Multi partition time interval must be larger than zero.");
        }

        if (!timeType.equals("")) {
            try {
                this.timeUnitType = TimeUnit.valueOf(timeType.toUpperCase());
            } catch (Exception e) {
                throw new AnalysisException("Multi build partition got an unknown time interval type: "
                        + timeType);
            }
        }

        if (this.timeUnitType != null && !timeUnitTypeMultiPartition.contains(this.timeUnitType)) {
            throw new AnalysisException("Multi build partition does not support time interval type: "
                    + this.timeUnitType);
        }
    }

    private static DateTimeFormatter dateFormat(TimeUnit timeUnitType,
            String dateTimeStr) throws AnalysisException {
        DateTimeFormatter res;
        switch (timeUnitType) {
            case HOUR:
                if (dateTimeStr.length() == 10) {
                    res = DateTimeFormatter.ofPattern(HOURS_FORMAT);
                } else if (dateTimeStr.length() == 13) {
                    res = DateTimeFormatter.ofPattern(HOUR_FORMAT);
                } else if (dateTimeStr.length() == 19) {
                    res = DateTimeFormatter.ofPattern(DATETIME_FORMAT);
                } else {
                    throw new AnalysisException("can not probe datetime(hour) format:" + dateTimeStr);
                }
                break;
            case DAY: case WEEK:
                if (dateTimeStr.length() == 8) {
                    res = DateTimeFormatter.ofPattern(DATES_FORMAT);
                } else if (dateTimeStr.length() == 10) {
                    res = DateTimeFormatter.ofPattern(DATE_FORMAT);
                } else if (dateTimeStr.length() == 19) {
                    res = DateTimeFormatter.ofPattern(DATETIME_FORMAT);
                } else {
                    throw new AnalysisException("can not probe datetime(day or week) format:" + dateTimeStr);
                }
                break;
            case MONTH:
                if (dateTimeStr.length() == 6) {
                    res = DateTimeFormatter.ofPattern(MONTHS_FORMAT);
                } else if (dateTimeStr.length() == 7) {
                    res = DateTimeFormatter.ofPattern(MONTH_FORMAT);
                } else if (dateTimeStr.length() == 10) {
                    res = DateTimeFormatter.ofPattern(DATE_FORMAT);
                } else if (dateTimeStr.length() == 19) {
                    res = DateTimeFormatter.ofPattern(DATETIME_FORMAT);
                } else {
                    throw new AnalysisException("can not probe datetime(month) format:" + dateTimeStr);
                }
                break;
            case YEAR:
                if (dateTimeStr.length() == 4) {
                    res = DateTimeFormatter.ofPattern(YEAR_FORMAT);
                } else if (dateTimeStr.length() == 8) {
                    res = DateTimeFormatter.ofPattern(DATES_FORMAT);
                } else if (dateTimeStr.length() == 10) {
                    res = DateTimeFormatter.ofPattern(DATE_FORMAT);
                } else if (dateTimeStr.length() == 19) {
                    res = DateTimeFormatter.ofPattern(DATETIME_FORMAT);
                } else {
                    throw new AnalysisException("can not probe datetime(year) format:" + dateTimeStr);
                }
                break;
            default:
                throw new AnalysisException("Multi build partition does not support time interval type: "
                        + timeUnitType);
        }
        return res;
    }

    private DateTimeFormatter dateTypeFormat(String dateTimeStr) {
        String s = DATE_FORMAT;
        if (this.timeUnitType.equals(TimeUnit.HOUR) || dateTimeStr.length() == 19) {
            s = DATETIME_FORMAT;
        }
        return DateTimeFormatter.ofPattern(s);
    }

}

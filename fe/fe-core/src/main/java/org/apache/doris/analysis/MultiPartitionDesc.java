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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// to describe the key list partition's information in create table stmt
public class MultiPartitionDesc implements AllPartitionDesc {
    public static final String HOURS_FORMAT = "yyyyMMddHH";
    public static final String HOUR_FORMAT = "yyyy-MM-dd HH";
    public static final String DATES_FORMAT = "yyyyMMdd";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String MONTHS_FORMAT = "yyyyMM";
    public static final String MONTH_FORMAT = "yyyy-MM";
    public static final String YEAR_FORMAT = "yyyy";
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";



    private final String partitionPrefix = "p_";
    private LocalDateTime startTime;
    private LocalDateTime endTime;

    private DateTimeFormatter startDateTimeFormat;
    private DateTimeFormatter endDateTimeFormat;


    private Long timeInterval;
    private final PartitionKeyDesc partitionKeyDesc;
    private TimestampArithmeticExpr.TimeUnit timeUnitType;
    private final Map<String, String> properties;
    private final List<SinglePartitionDesc> singlePartitionDescList = Lists.newArrayList();

    private final ImmutableSet<TimestampArithmeticExpr.TimeUnit> timeUnitTypeMultiPartition = ImmutableSet.of(
            TimestampArithmeticExpr.TimeUnit.HOUR,
            TimestampArithmeticExpr.TimeUnit.DAY,
            TimestampArithmeticExpr.TimeUnit.WEEK,
            TimestampArithmeticExpr.TimeUnit.MONTH,
            TimestampArithmeticExpr.TimeUnit.YEAR
    );

    private final Integer maxAllowedLimit = Config.max_multi_partition_num;

    public MultiPartitionDesc(PartitionKeyDesc partitionKeyDesc,
                          Map<String, String> properties) throws AnalysisException {
        this.partitionKeyDesc = partitionKeyDesc;
        this.properties = properties;
        this.timeIntervalTrans();
        this.timeTrans();
    }

    public List<SinglePartitionDesc> getSinglePartitionDescList() throws AnalysisException {
        if (singlePartitionDescList.size() == 0) {
            buildMultiPartitionToSinglePartitionDescs();
        }
        return singlePartitionDescList;
    }

    private List<SinglePartitionDesc> buildMultiPartitionToSinglePartitionDescs() throws AnalysisException {
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
            PartitionValue lowerPartitionValue = new PartitionValue(startTime.format(dateTypeFormat()));
            switch (this.timeUnitType) {
                case HOUR:
                    partitionName = partitionPrefix + startTime.format(DateTimeFormatter.ofPattern(HOURS_FORMAT));
                    startTime = startTime.plusHours(timeInterval);
                    break;
                case DAY:
                    partitionName = partitionPrefix + startTime.format(DateTimeFormatter.ofPattern(DATES_FORMAT));
                    startTime = startTime.plusDays(timeInterval);
                    break;
                case WEEK:
                    LocalDate localDate = LocalDate.of(startTime.getYear(), startTime.getMonthValue(),
                            startTime.getDayOfMonth());
                    int weekOfYear = localDate.get(weekFields.weekOfYear());
                    partitionName = String.format("%s%s_%02d", partitionPrefix,
                            startTime.format(DateTimeFormatter.ofPattern(YEAR_FORMAT)), weekOfYear);
                    startTime = startTime.with(ChronoField.DAY_OF_WEEK, startDayOfMonth);
                    startTime = startTime.plusWeeks(timeInterval);
                    break;
                case MONTH:
                    partitionName = partitionPrefix + startTime.format(DateTimeFormatter.ofPattern(MONTHS_FORMAT));
                    startTime = startTime.withDayOfMonth(startDayOfMonth);
                    startTime = startTime.plusMonths(timeInterval);
                    break;
                case YEAR:
                    partitionName = partitionPrefix + startTime.format(DateTimeFormatter.ofPattern(YEAR_FORMAT));
                    startTime = startTime.withDayOfYear(1);
                    startTime = startTime.plusYears(timeInterval);
                    break;
                default:
                    throw new AnalysisException("Multi build partition does not support time interval type: "
                            + this.timeUnitType);
            }
            if (this.timeUnitType != TimestampArithmeticExpr.TimeUnit.DAY && startTime.isAfter(this.endTime)) {
                startTime = this.endTime;
            }
            PartitionValue upperPartitionValue = new PartitionValue(startTime.format(dateTypeFormat()));
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

    private void timeTrans() throws AnalysisException {

        if (partitionKeyDesc.getLowerValues().size() != 1 || partitionKeyDesc.getUpperValues().size() != 1) {
            throw new AnalysisException("partition column number in multi partition clause must be one "
                    + "but START column size is " + partitionKeyDesc.getLowerValues().size()
                    + ", END column size is " + partitionKeyDesc.getUpperValues().size() + ".");
        }

        String startString = partitionKeyDesc.getLowerValues().get(0).getStringValue();
        String endString = partitionKeyDesc.getUpperValues().get(0).getStringValue();

        try {
            this.startDateTimeFormat = dateFormat(this.timeUnitType, startString);
            this.endDateTimeFormat = dateFormat(this.timeUnitType, endString);
            this.startTime = TimeUtils.formatDateTimeAndFullZero(startString, startDateTimeFormat);
            this.endTime = TimeUtils.formatDateTimeAndFullZero(endString, endDateTimeFormat);
        } catch (Exception e) {
            throw new AnalysisException("Multi build partition START or END time style is illegal.");
        }

        if (!this.startTime.isBefore(this.endTime)) {
            throw new AnalysisException("Multi build partition start time should less than end time.");
        }
    }


    private void timeIntervalTrans() throws AnalysisException {
        this.timeInterval = partitionKeyDesc.getTimeInterval();
        String timeType = partitionKeyDesc.getTimeType();
        if (timeType == null) {
            throw new AnalysisException("Unknown time interval type for Multi build partition.");
        }
        if (this.timeInterval <= 0) {
            throw new AnalysisException("Multi partition time interval mush be larger than zero.");
        }
        try {
            this.timeUnitType = TimestampArithmeticExpr.TimeUnit.valueOf(timeType);
        } catch (Exception e) {
            throw new AnalysisException("Multi build partition got an unknow time interval type: "
                    + timeType);
        }
        if (!timeUnitTypeMultiPartition.contains(this.timeUnitType)) {
            throw new AnalysisException("Multi build partition does not support time interval type: "
                    + this.timeUnitType);
        }
    }

    private static DateTimeFormatter dateFormat(TimestampArithmeticExpr.TimeUnit timeUnitType,
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

    private DateTimeFormatter dateTypeFormat() {
        return DateTimeFormatter.ofPattern(this.timeUnitType.equals(TimeUnit.HOUR) ? DATETIME_FORMAT : DATE_FORMAT);
    }


    private List<AllPartitionDesc> aaa(int ii) throws AnalysisException {
        List<AllPartitionDesc> res = new ArrayList<>();
        for (int i = 0; i < ii; i++) {
            if (ii % 2 == 1) {
                res.add(new MultiPartitionDesc(null, null));
            } else {
                res.add(new SinglePartitionDesc(true, "-", null, null));
            }
        }
        return res;
    }

}

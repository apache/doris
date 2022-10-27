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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.nereids.util.DateUtils;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.WeekFields;
import java.util.List;
import java.util.Map;

// to describe the key list partition's information in create table stmt
public class MultiPartition {

    public static final String HOUR_FORMAT = "yyyyMMddHH";

    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String MONTH_FORMAT = "yyyyMM";
    public static final String YEAR_FORMAT = "yyyy";
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";



    private final String partitionPrefix = "p_";
    private LocalDateTime startTime;
    private LocalDateTime endTime;

    private DateTimeFormatter startDateTimeFormat;
    private DateTimeFormatter endDateTimeFormat;


    private Integer timeInterval;
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



    public MultiPartition(PartitionKeyDesc partitionKeyDesc,
                          Map<String, String> properties) throws AnalysisException {
        this.partitionKeyDesc = partitionKeyDesc;
        this.properties = properties;
        this.timeTrans();
        this.timeIntervalTrans();
    }

    public List<SinglePartitionDesc> getSinglePartitionDescList() {

       // MultiPartition to List<SinglePartitionDesc>
        //        PARTITION START ("2021-05-01") END ("2021-05-04") EVERY (INTERVAL 1 DAY)
        //        ->
        //        PARTITION p20210501 VALUES [('2021-05-01'), ('2021-05-02')),
        //        PARTITION p20210502 VALUES [('2021-05-02'), ('2021-05-03')),
        //        PARTITION p20210503 VALUES [('2021-05-03'), ('2021-05-04'))
        if(singlePartitionDescList.size()==0) {
            try {
                buildMultiPartitionToSinglePartitionDescs();
            } catch (AnalysisException e) {
                throw new RuntimeException(e);
            }
        }
        return singlePartitionDescList;
    }

    private List<SinglePartitionDesc> buildMultiPartitionToSinglePartitionDescs() throws AnalysisException {
        String partitionName;
        long countNum = 0;
        int dayOfWeek = 1;
        int dayOfMonth = 1;
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
                dayOfWeek = Integer.parseInt(dayOfWeekStr);
            }
            if (properties.containsKey(DynamicPartitionProperty.START_DAY_OF_MONTH)) {
                String dayOfMonthStr = properties.get(DynamicPartitionProperty.START_DAY_OF_MONTH);
                try {
                    DynamicPartitionUtil.checkStartDayOfMonth(dayOfMonthStr);
                } catch (DdlException e) {
                    throw new AnalysisException(e.getMessage());
                }
                dayOfMonth = Integer.parseInt(dayOfMonthStr);
            }

            if (properties.containsKey(DynamicPartitionProperty.CREATE_HISTORY_PARTITION)) {
                properties.put(DynamicPartitionProperty.CREATE_HISTORY_PARTITION,"false");
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
        WeekFields weekFields = WeekFields.of(DayOfWeek.of(dayOfWeek), 1);
        while (startTime.isBefore(this.endTime)) {
            PartitionValue lowerPartitionValue = new PartitionValue(startTime.format(startDateTimeFormat));
            switch (this.timeUnitType) {
                case HOUR:
                    partitionName = partitionPrefix + startTime.format(DateTimeFormatter.ofPattern(HOUR_FORMAT));
                    startTime = startTime.plusHours(timeInterval);
                    break;
                case DAY:
                    partitionName = partitionPrefix + startTime.format(DateTimeFormatter.ofPattern(DATE_FORMAT));
                    startTime = startTime.plusDays(timeInterval);
                    break;
                case WEEK:
                    LocalDate localDate = LocalDate.of(startTime.getYear(), startTime.getMonthValue(),
                        startTime.getDayOfMonth());
                    int weekOfYear = localDate.get(weekFields.weekOfYear());
                    partitionName = String.format("%s%s_%02d", partitionPrefix, startTime.format(DateTimeFormatter.ofPattern(YEAR_FORMAT)), weekOfYear);
                    startTime = startTime.with(ChronoField.DAY_OF_WEEK, dayOfMonth);
                    startTime = startTime.plusWeeks(timeInterval);
                    break;
                case MONTH:
                    partitionName = partitionPrefix + startTime.format(DateTimeFormatter.ofPattern(MONTH_FORMAT));
                    startTime = startTime.withDayOfMonth(dayOfMonth);
                    startTime = startTime.plusMonths(timeInterval);
                    break;
                case YEAR:
                    partitionName = partitionPrefix + startTime.format(DateTimeFormatter.ofPattern(YEAR_FORMAT));
                    startTime = startTime.withDayOfYear(1);
                    startTime = startTime.plusYears(timeInterval);
                    break;
                default:
                    throw new AnalysisException("Batch build partition does not support time interval type: " +
                        this.timeUnitType);
            }
            if (this.timeUnitType != TimestampArithmeticExpr.TimeUnit.DAY && startTime.isAfter(this.endTime)) {
                startTime = this.endTime;
            }

            PartitionValue upperPartitionValue = new PartitionValue(startTime.format(startDateTimeFormat));
            PartitionKeyDesc partitionKeyDesc = PartitionKeyDesc.createFixed(Lists.newArrayList(lowerPartitionValue), Lists.newArrayList(upperPartitionValue));
            singlePartitionDescList.add(
                new SinglePartitionDesc(
                    false,
                    partitionName,
                    partitionKeyDesc,
                    properties)
            );

            countNum++;
            if (countNum > maxAllowedLimit) {
                throw new AnalysisException("The number of batch partitions should not exceed:" + maxAllowedLimit);
            }
        }
        return singlePartitionDescList;
    }

    private void timeTrans() throws AnalysisException {

        if(partitionKeyDesc.getLowerValues().size()!=1 || partitionKeyDesc.getUpperValues().size()!=1){
            throw new AnalysisException("Batch build partition column size must be one " +
                "but START column size is "+ partitionKeyDesc.getLowerValues().size() +
                ", END column size is "+ partitionKeyDesc.getUpperValues().size() + ".");
        }

        String startString = partitionKeyDesc.getLowerValues().get(0).getStringValue();
        String endString = partitionKeyDesc.getUpperValues().get(0).getStringValue();

        try {
            this.startDateTimeFormat = dateFormat(startString);
            this.endDateTimeFormat = dateFormat(endString);
            this.startTime = DateUtils.formatDateTimeAndFullZero(startString, startDateTimeFormat);
            this.endTime = DateUtils.formatDateTimeAndFullZero(endString, endDateTimeFormat);
        } catch (Exception e) {
            throw new AnalysisException("Batch build partition EVERY is date type " +
                "but START or END does not type match.");
        }

        if (!this.startTime.isBefore(this.endTime)) {
            throw new AnalysisException("Batch build partition start date should less than end date.");
        }
    }


    private void timeIntervalTrans() throws AnalysisException {
        this.timeInterval = partitionKeyDesc.getTimeInterval();
        String timeType = partitionKeyDesc.getTimeType();
        if (timeType == null) {
            throw new AnalysisException("Unknown timeunit for batch build partition.");
        }
        if (this.timeInterval <= 0) {
            throw new AnalysisException("Batch partition every clause mush be larger than zero.");
        }
        this.timeUnitType = TimestampArithmeticExpr.TimeUnit.valueOf(timeType);
        if ( !timeUnitTypeMultiPartition.contains(this.timeUnitType)) {
            throw new AnalysisException("Batch build partition does not support time interval type: " + this.timeUnitType);
        }
    }

    private static DateTimeFormatter dateFormat(String dateTimeStr) throws AnalysisException {
        if (dateTimeStr.length() == 8) {
            return DateTimeFormatter.ofPattern(DATE_FORMAT);
        } else if (dateTimeStr.length() == 10) {
            return DateTimeFormatter.ofPattern(DATE_FORMAT);
        } else if (dateTimeStr.length() == 19) {
            return DateTimeFormatter.ofPattern(DATETIME_FORMAT);
        } else {
            throw new AnalysisException("can not probe datetime format:" + dateTimeStr);
        }
    }

}

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
import org.apache.doris.catalog.Partition;
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

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

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

    public static boolean checkDynamicPartitionPropertiesExist(Map<String, String> properties) {
        if (properties == null) {
            return false;
        }
        return properties.containsKey(DynamicPartitionProperty.TIME_UNIT) ||
                properties.containsKey(DynamicPartitionProperty.START) ||
                properties.containsKey(DynamicPartitionProperty.END) ||
                properties.containsKey(DynamicPartitionProperty.PREFIX) ||
                properties.containsKey(DynamicPartitionProperty.BUCKETS) ||
                properties.containsKey(DynamicPartitionProperty.ENABLE);
    }

    public static boolean checkInputDynamicPartitionProperties(Map<String, String> properties, PartitionInfo partitionInfo) throws DdlException{
        if (properties == null || properties.isEmpty()) {
            return false;
        }
        if (partitionInfo.getType() != PartitionType.RANGE || partitionInfo.isMultiColumnPartition()) {
            throw new DdlException("Dynamic partition only support single-column range partition");
        }
        String timeUnit = properties.get(DynamicPartitionProperty.TIME_UNIT);
        String prefix = properties.get(DynamicPartitionProperty.PREFIX);
        String start = properties.get(DynamicPartitionProperty.START);
        String end = properties.get(DynamicPartitionProperty.END);
        String buckets = properties.get(DynamicPartitionProperty.BUCKETS);
        String enable = properties.get(DynamicPartitionProperty.ENABLE);
        if (!((Strings.isNullOrEmpty(enable) &&
                Strings.isNullOrEmpty(timeUnit) &&
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
            checkStart(properties.get(DynamicPartitionProperty.START));
            properties.remove(DynamicPartitionProperty.START);
            analyzedProperties.put(DynamicPartitionProperty.START, startValue);
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

    public static String getFormattedPartitionName(String name, String timeUnit) {
        name = name.replace("-", "").replace(":", "").replace(" ", "");
        if (timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())) {
            return name.substring(0, 8);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString())) {
            return name.substring(0, 6);
        } else {
            name = name.substring(0, 8);
            Calendar calendar = Calendar.getInstance();
            try {
                calendar.setTime(new SimpleDateFormat("yyyyMMdd").parse(name));
            } catch (ParseException e) {
                LOG.warn("Format dynamic partition name error. Error={}", e.getMessage());
                return name;
            }
            return String.format("%s_%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.WEEK_OF_YEAR));
        }
    }

    public static String getPartitionRange(String timeUnit, int offset, Calendar calendar, String format) {
        if (timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())) {
            calendar.add(Calendar.DAY_OF_MONTH, offset);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.WEEK.toString())) {
            calendar.add(Calendar.WEEK_OF_MONTH, offset);
        } else {
            calendar.add(Calendar.MONTH, offset);
        }
        // dynamic partition's time accuracy is DAY
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(calendar.getTime());
    }

    public static int estimateReplicateNum(OlapTable table) {
        int replicateNum = table.getDefaultReplicationNum();
        long maxPartitionId = 0;
        for (Partition partition: table.getPartitions()) {
            if (partition.getId() > maxPartitionId) {
                maxPartitionId = partition.getId();
                replicateNum = table.getPartitionInfo().getReplicationNum(partition.getId());
            }
        }
        return replicateNum;
    }
}
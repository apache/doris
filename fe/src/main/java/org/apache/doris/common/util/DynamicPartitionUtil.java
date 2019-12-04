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

import com.google.common.base.Strings;
import org.apache.doris.analysis.TimestampArithmeticExpr.TimeUnit;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class DynamicPartitionUtil {
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

    public static void checkPrefix(String prefix) throws DdlException {
        try {
            FeNameFormat.checkPartitionName(prefix);
        } catch (AnalysisException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_PREFIX, prefix);
        }
    }

    public static void checkEnd(String end) throws DdlException {
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

    public static void checkBuckets(String buckets) throws DdlException {
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

    public static void checkEnable(Database db, OlapTable tbl, String enable) throws DdlException {
        if (!Strings.isNullOrEmpty(enable) ||
                Boolean.TRUE.toString().equalsIgnoreCase(enable) ||
                Boolean.FALSE.toString().equalsIgnoreCase(enable)) {
            if (Boolean.parseBoolean(enable)) {
                Catalog.getCurrentCatalog().getDynamicPartitionScheduler().registerDynamicPartitionTable(db.getId(), tbl.getId());
            } else {
                Catalog.getCurrentCatalog().getDynamicPartitionScheduler().removeDynamicPartitionTable(db.getId(), tbl.getId());
            }
        } else {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_ENABLE, enable);
        }
    }

    public static boolean checkDynamicPartitionPropertiesExist(Map<String, String> properties) {
        if (properties == null) {
            return false;
        }
        return properties.containsKey(DynamicPartitionProperty.TIME_UNIT) ||
                properties.containsKey(DynamicPartitionProperty.END) ||
                properties.containsKey(DynamicPartitionProperty.PREFIX) ||
                properties.containsKey(DynamicPartitionProperty.BUCKETS) ||
                properties.containsKey(DynamicPartitionProperty.ENABLE);
    }

    public static void checkAllDynamicPartitionProperties(Map<String, String> properties) throws DdlException{
        if (properties == null) {
            return;
        }
        String timeUnit = properties.get(DynamicPartitionProperty.TIME_UNIT);
        String prefix = properties.get(DynamicPartitionProperty.PREFIX);
        String end = properties.get(DynamicPartitionProperty.END);
        String buckets = properties.get(DynamicPartitionProperty.BUCKETS);
        String enable = properties.get(DynamicPartitionProperty.ENABLE);
        if (!((Strings.isNullOrEmpty(timeUnit) &&
                Strings.isNullOrEmpty(prefix) &&
                Strings.isNullOrEmpty(end) &&
                Strings.isNullOrEmpty(buckets)))) {
            if (Strings.isNullOrEmpty(timeUnit)) {
                throw new DdlException("Must assign dynamic_partition.time_unit properties");
            }
            if (Strings.isNullOrEmpty(prefix)) {
                throw new DdlException("Must assign dynamic_partition.prefix properties");
            }
            if (Strings.isNullOrEmpty(end)) {
                throw new DdlException("Must assign dynamic_partition.end properties");
            }
            if (Strings.isNullOrEmpty(buckets)) {
                throw new DdlException("Must assign dynamic_partition.buckets properties");
            }
            // dynamic partition enable default to true
            if (Strings.isNullOrEmpty(enable)) {
                enable = Boolean.TRUE.toString();
                properties.put(DynamicPartitionProperty.ENABLE, enable);
            }
        }
    }

    public static Map<String, String> analyzeDynamicPartition(Database db, OlapTable olapTable,
                                                              Map<String, String> properties) throws DdlException {
        if (properties == null || properties.isEmpty()) {
            throw new DdlException("Table " + olapTable.getName() + " is not a dynamic partition olap table");
        }
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
            checkEnable(db, olapTable, enableValue);
            properties.remove(DynamicPartitionProperty.ENABLE);
            analyzedProperties.put(DynamicPartitionProperty.ENABLE, enableValue);
        }
        return analyzedProperties;
    }

    public static void checkAlterAllowed(OlapTable olapTable) throws DdlException {
        TableProperty tableProperty = olapTable.getTableProperty();
        if (tableProperty != null &&
                tableProperty.getDynamicPartitionProperty().isExist() &&
                Boolean.parseBoolean(tableProperty.getDynamicPartitionProperty().getEnable())) {
            throw new DdlException("Cannot modify partition on a Dynamic Partition Table, set `dynamic_partition.enable` to false firstly.");
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

        String enable = tableProperty.getDynamicPartitionProperty().getEnable();
        return rangePartitionInfo.getPartitionColumns().size() == 1 &&
                !Strings.isNullOrEmpty(enable) && enable.equalsIgnoreCase(Boolean.TRUE.toString());
    }

    public static String getPartitionFormat(Column column) {
        if (column.getDataType().equals(PrimitiveType.DATE)) {
            return DATE_FORMAT;
        } else if (column.getDataType().equals(PrimitiveType.DATETIME)) {
            return DATETIME_FORMAT;
        } else {
            // TODO: How to resolve int type a better way
            return TIMESTAMP_FORMAT;
        }
    }

    public static String getFormattedPartitionName(String name) {
        return name.replace("-", "").replace(":", "").replace(" ", "");
    }

    public static String getPartitionRange(String timeUnit, int offset, Calendar calendar, String format) {
        if (timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())) {
            calendar.add(Calendar.DAY_OF_MONTH, offset);
        } else if (timeUnit.equalsIgnoreCase(TimeUnit.WEEK.toString())) {
            calendar.add(Calendar.WEEK_OF_MONTH, offset);
        } else {
            calendar.add(Calendar.MONTH, offset);
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(calendar.getTime());
    }

    public static int estimateReplicateNum(OlapTable table) {
        int replicateNum = 3;
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
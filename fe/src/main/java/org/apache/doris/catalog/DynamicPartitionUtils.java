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


package org.apache.doris.catalog;

import com.google.common.base.Strings;
import org.apache.doris.analysis.TimestampArithmeticExpr.TimeUnit;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.clone.DynamicPartitionScheduler;

import java.util.HashMap;
import java.util.Map;

public class DynamicPartitionUtils {

    public enum DynamicPartitionProperties {
        TIME_UNIT("dynamic_partition.time_unit"),
        PREFIX("dynamic_partition.prefix"),
        END("dynamic_partition.end"),
        BUCKETS("dynamic_partition.buckets"),
        ENABLE("dynamic_partition.enable");

        private String desc;

        DynamicPartitionProperties(String desc) {
            this.desc = desc;
        }

        public String getDesc() {
            return desc;
        }
    }

    public static void checkTimeUnit(OlapTable tbl, String timeUnit) throws DdlException {
        if (Strings.isNullOrEmpty(timeUnit)
                || !(timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.WEEK.toString())
                || timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString()))) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_TIME_UNIT, timeUnit);
        }
    }

    public static void checkPrefix(OlapTable tbl, String prefix) throws DdlException {
        try {
            FeNameFormat.checkPartitionName(prefix);
        } catch (AnalysisException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_PREFIX, prefix);
        }
    }

    public static void checkEnd(OlapTable tbl, String end) throws DdlException {
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

    public static void checkBuckets(OlapTable tbl, String buckets) throws DdlException {
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
                DynamicPartitionScheduler.registerDynamicPartitionTable(db.getId(), tbl.getId());
            } else {
                DynamicPartitionScheduler.removeDynamicPartitionTable(db.getId(), tbl.getId());
            }
        } else {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_ENABLE, enable);
        }
    }

    public static boolean checkDynamicPartitionPropertiesExist(Map<String, String> properties) {
        if (properties == null) {
            return false;
        }
        return properties.containsKey(DynamicPartitionProperties.ENABLE.getDesc()) ||
                properties.containsKey(DynamicPartitionProperties.TIME_UNIT.getDesc()) ||
                properties.containsKey(DynamicPartitionProperties.PREFIX.getDesc()) ||
                properties.containsKey(DynamicPartitionProperties.END.getDesc()) ||
                properties.containsKey(DynamicPartitionProperties.BUCKETS.getDesc());
    }

    public static void checkAllDynamicPartitionProperties(Map<String, String> properties) throws DdlException{
        if (properties == null) {
            return;
        }
        String timeUnit = properties.get(DynamicPartitionProperties.TIME_UNIT.getDesc());
        String prefix = properties.get(DynamicPartitionProperties.PREFIX.getDesc());
        String end = properties.get(DynamicPartitionProperties.END.getDesc());
        String buckets = properties.get(DynamicPartitionProperties.BUCKETS.getDesc());
        String enable = properties.get(DynamicPartitionProperties.ENABLE.getDesc());
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
                properties.put(DynamicPartitionProperties.ENABLE.getDesc(), enable);
            }
        }
    }

    public static Map<String, String> analyzeDynamicPartition(Database db, OlapTable olapTable,
                                                              Map<String, String> properties) throws DdlException {
        if (properties == null || properties.isEmpty()) {
            throw new DdlException("Table " + olapTable.getName() + " is not a dynamic partition olap table");
        }
        Map<String, String> analyzedProperties = new HashMap<>();
        if (properties.containsKey(DynamicPartitionProperties.TIME_UNIT.getDesc())) {
            String timeUnitKey = DynamicPartitionProperties.TIME_UNIT.getDesc();
            String timeUnitValue = properties.get(timeUnitKey);
            DynamicPartitionUtils.checkTimeUnit(olapTable, timeUnitValue);
            properties.remove(timeUnitKey);
            analyzedProperties.put(timeUnitKey, timeUnitValue);
        }
        if (properties.containsKey(DynamicPartitionProperties.PREFIX.getDesc())) {
            String prefixKey = DynamicPartitionProperties.PREFIX.getDesc();
            String prefixValue = properties.get(prefixKey);
            DynamicPartitionUtils.checkPrefix(olapTable, prefixValue);
            properties.remove(prefixKey);
            analyzedProperties.put(prefixKey, prefixValue);
        }
        if (properties.containsKey(DynamicPartitionProperties.END.getDesc())) {
            String endKey = DynamicPartitionProperties.END.getDesc();
            String endValue = properties.get(endKey);
            DynamicPartitionUtils.checkEnd(olapTable, endValue);
            properties.remove(endKey);
            analyzedProperties.put(endKey, endValue);
        }
        if (properties.containsKey(DynamicPartitionProperties.BUCKETS.getDesc())) {
            String bucketsKey = DynamicPartitionProperties.BUCKETS.getDesc();
            String bucketsValue = properties.get(bucketsKey);
            DynamicPartitionUtils.checkBuckets(olapTable, bucketsValue);
            properties.remove(bucketsKey);
            analyzedProperties.put(bucketsKey, bucketsValue);
        }
        if (properties.containsKey(DynamicPartitionProperties.ENABLE.getDesc())) {
            String enableKey = DynamicPartitionProperties.ENABLE.getDesc();
            String enableValue = properties.get(enableKey);
            DynamicPartitionUtils.checkEnable(db, olapTable, enableValue);
            properties.remove(enableKey);
            analyzedProperties.put(enableKey, enableValue);
        }
        return analyzedProperties;
    }

    public static void checkAlterAllowed(OlapTable olapTable) throws DdlException {
        TableProperty tableProperty = olapTable.getTableProperty();
        if (tableProperty != null && (!tableProperty.getDynamicProperties().isEmpty()) &&
                Boolean.parseBoolean(tableProperty.getDynamicPartitionEnable())) {
            throw new DdlException("Cannot modify partition on a Dynamic Partition Table, set `dynamic_partition.enable` to false.");
        }
    }
}

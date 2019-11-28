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

import org.apache.doris.analysis.TimestampArithmeticExpr.TimeUnit;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.task.DynamicPartitionScheduler;

import java.util.Map;

public class DynamicPartitionUtils {
    public static final String TRUE = "true";
    public static final String FALSE = "false";

    public static void checkTimeUnit(OlapTable tbl, String timeUnit) throws DdlException {
        if (!(timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())
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
        try {
            if (Integer.parseInt(end) <= 0) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_END_ZERO, end);
            }
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_END_FORMAT, end);
        }
    }

    public static void checkBuckets(OlapTable tbl, String buckets) throws DdlException {
        try {
            if (Integer.parseInt(buckets) <= 0) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_BUCKETS_ZERO, buckets);
            }
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_BUCKETS_FORMAT, buckets);
        }
    }

    public static void checkEnable(Database db, OlapTable tbl, String enable) throws DdlException {
        if (TRUE.equalsIgnoreCase(enable) || FALSE.equalsIgnoreCase(enable)) {
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
        return properties.containsKey(PropertyAnalyzer.PROPERTIES_DYANMIC_PARTITION_ENABLE) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_DYNAMIC_PARTITION_TIME_UNIT) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_DYNAMIC_PARTITION_PREFIX) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_DYNAMIC_PARTITION_END) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_DYNAMIC_PARTITION_BUCKETS);
    }
}

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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.Set;

public class MTMVPropertyUtil {
    public static final Set<String> MV_PROPERTY_KEYS = Sets.newHashSet(
            PropertyAnalyzer.PROPERTIES_GRACE_PERIOD,
            PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES,
            PropertyAnalyzer.PROPERTIES_REFRESH_PARTITION_NUM,
            PropertyAnalyzer.PROPERTIES_WORKLOAD_GROUP,
            PropertyAnalyzer.PROPERTIES_PARTITION_SYNC_LIMIT,
            PropertyAnalyzer.PROPERTIES_PARTITION_TIME_UNIT,
            PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT,
            PropertyAnalyzer.PROPERTIES_ENABLE_NONDETERMINISTIC_FUNCTION,
            PropertyAnalyzer.PROPERTIES_USE_FOR_REWRITE
    );

    public static void analyzeProperty(String key, String value) {
        switch (key) {
            case PropertyAnalyzer.PROPERTIES_GRACE_PERIOD:
                analyzeGracePeriod(value);
                break;
            case PropertyAnalyzer.PROPERTIES_REFRESH_PARTITION_NUM:
                analyzeRefreshPartitionNum(value);
                break;
            case PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES:
                analyzeExcludedTriggerTables(value);
                break;
            case PropertyAnalyzer.PROPERTIES_WORKLOAD_GROUP:
                analyzeWorkloadGroup(value);
                break;
            case PropertyAnalyzer.PROPERTIES_PARTITION_TIME_UNIT:
                analyzePartitionTimeUnit(value);
                break;
            case PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT:
                analyzePartitionDateFormat(value);
                break;
            case PropertyAnalyzer.PROPERTIES_PARTITION_SYNC_LIMIT:
                analyzePartitionSyncLimit(value);
                break;
            case PropertyAnalyzer.PROPERTIES_ENABLE_NONDETERMINISTIC_FUNCTION:
                analyzeBooleanProperty(value, PropertyAnalyzer.PROPERTIES_ENABLE_NONDETERMINISTIC_FUNCTION);
                break;
            case PropertyAnalyzer.PROPERTIES_USE_FOR_REWRITE:
                analyzeBooleanProperty(value, PropertyAnalyzer.PROPERTIES_USE_FOR_REWRITE);
                break;
            default:
                throw new AnalysisException("illegal key:" + key);

        }
    }

    private static void analyzePartitionSyncLimit(String value) {
        if (StringUtils.isEmpty(value)) {
            return;
        }
        try {
            Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("valid partition_sync_limit: " + value);
        }
    }

    private static void analyzePartitionDateFormat(String value) {
        // do nothing
    }

    private static void analyzePartitionTimeUnit(String value) {
        if (StringUtils.isEmpty(value)) {
            return;
        }
        Optional<MTMVPartitionSyncTimeUnit> mtmvPartitionSyncTimeUnit = MTMVPartitionSyncTimeUnit
                .fromString(value);
        if (!mtmvPartitionSyncTimeUnit.isPresent()) {
            throw new AnalysisException("valid partition_sync_time_unit: " + value);
        }
    }

    private static void analyzeWorkloadGroup(String value) {
        if (StringUtils.isEmpty(value)) {
            return;
        }
        if (!StringUtils.isEmpty(value) && !Env.getCurrentEnv().getAccessManager()
                .checkWorkloadGroupPriv(ConnectContext.get(), value, PrivPredicate.USAGE)) {
            String message = String
                    .format("Access denied; you need (at least one of) "
                                    + "the %s privilege(s) to use workload group '%s'.",
                            "USAGE/ADMIN", value);
            throw new AnalysisException(message);
        }
    }

    private static void analyzeExcludedTriggerTables(String value) {
        // do nothing
    }

    private static void analyzeGracePeriod(String value) {
        if (StringUtils.isEmpty(value)) {
            return;
        }
        try {
            Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("valid grace_period: " + value);
        }
    }

    private static void analyzeRefreshPartitionNum(String value) {
        if (StringUtils.isEmpty(value)) {
            return;
        }
        try {
            Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("valid refresh_partition_num: " + value);
        }
    }

    private static void analyzeBooleanProperty(String propertyValue, String propertyName) {
        if (StringUtils.isEmpty(propertyValue)) {
            return;
        }
        if (!"true".equalsIgnoreCase(propertyValue) && !"false".equalsIgnoreCase(propertyValue)) {
            throw new AnalysisException(String.format("valid property %s fail", propertyName));
        }
    }
}

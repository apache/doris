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
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MTMVPropertyUtil {
    public static final Set<String> MV_PROPERTY_KEYS = Sets.newHashSet(
            PropertyAnalyzer.PROPERTIES_GRACE_PERIOD,
            PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES,
            PropertyAnalyzer.ASYNC_MV_QUERY_REWRITE_CONSISTENCY_RELAXED_TABLES,
            PropertyAnalyzer.PROPERTIES_REFRESH_PARTITION_NUM,
            PropertyAnalyzer.PROPERTIES_WORKLOAD_GROUP,
            PropertyAnalyzer.PROPERTIES_PARTITION_SYNC_LIMIT,
            PropertyAnalyzer.PROPERTIES_PARTITION_TIME_UNIT,
            PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT,
            PropertyAnalyzer.PROPERTIES_ENABLE_NONDETERMINISTIC_FUNCTION,
            PropertyAnalyzer.PROPERTIES_USE_FOR_REWRITE,
            PropertyAnalyzer.PROPERTIES_IVM_USE_FULL_KEYS,
            PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT
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
            case PropertyAnalyzer.ASYNC_MV_QUERY_REWRITE_CONSISTENCY_RELAXED_TABLES:
                analyzeDataChangeStillRewrittenTables(value);
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
            case PropertyAnalyzer.PROPERTIES_IVM_USE_FULL_KEYS:
                analyzeBooleanProperty(value, PropertyAnalyzer.PROPERTIES_IVM_USE_FULL_KEYS);
                break;
            case PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT:
                analyzePartitionWindowLimit(value);
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

    private static void analyzePartitionWindowLimit(String value) {
        parsePartitionWindowLimit(value);
    }

    /**
     * Parse {@code "tbl:N,tbl2:M"} into table -> window partition count.
     * Only validates the value syntax; membership against MV base tables is
     * checked at create/alter time where the relation is known.
     *
     * <p>The window applies only to IVM incremental refresh: COMPLETE refresh always
     * covers the full table, so the window never reduces a full baseline.
     */
    public static Map<TableNameInfo, Integer> parsePartitionWindowLimit(String value) {
        Map<TableNameInfo, Integer> windowLimits = Maps.newHashMap();
        if (StringUtils.isEmpty(value)) {
            return windowLimits;
        }
        for (String entry : value.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int colon = trimmed.lastIndexOf(':');
            if (colon <= 0 || colon == trimmed.length() - 1) {
                throw new AnalysisException("valid " + PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT
                        + ": " + value + ", expected 'tableName:N' entries separated by ','");
            }
            String tableName = trimmed.substring(0, colon).trim();
            String limitStr = trimmed.substring(colon + 1).trim();
            int limit;
            try {
                limit = Integer.parseInt(limitStr);
            } catch (NumberFormatException e) {
                throw new AnalysisException("valid " + PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT
                        + ": " + value + ", invalid partition count '" + limitStr + "'");
            }
            if (limit <= 0) {
                throw new AnalysisException("valid " + PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT
                        + ": " + value + ", partition count must be positive");
            }
            TableNameInfo tableNameInfo;
            try {
                tableNameInfo = new TableNameInfo(tableName);
            } catch (IllegalArgumentException e) {
                // TableNameInfo rejects names like ".." with a raw IllegalArgumentException.
                throw new AnalysisException("valid " + PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT
                        + ": " + value + ", invalid table name '" + tableName + "'");
            }
            if (windowLimits.containsKey(tableNameInfo)) {
                throw new AnalysisException("valid " + PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT
                        + ": " + value + ", duplicated table '" + tableName + "'");
            }
            windowLimits.put(tableNameInfo, limit);
        }
        return windowLimits;
    }

    /**
     * Returns the configured window limit (last N partitions to refresh) per base table,
     * or an empty map when the property is not set.
     */
    public static Map<TableNameInfo, Integer> getIvmPartitionWindowLimit(Map<String, String> mvProperties) {
        if (mvProperties == null || !mvProperties.containsKey(
                PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT)) {
            return Maps.newHashMap();
        }
        return parsePartitionWindowLimit(mvProperties.get(
                PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT));
    }

    /**
     * Look up the window limit configured for a base table, mirroring the
     * excluded_trigger_tables name-matching semantics (empty db/ctl wildcard).
     * Returns -1 when the table is not configured, meaning the full table.
     */
    public static int getPartitionWindowLimit(Map<TableNameInfo, Integer> windowLimits, TableNameInfo baseTableName) {
        int matchedLimit = -1;
        for (Map.Entry<TableNameInfo, Integer> entry : windowLimits.entrySet()) {
            if (MTMVPartitionUtil.isTableNamelike(entry.getKey(), baseTableName)) {
                if (matchedLimit != -1) {
                    throw new AnalysisException("valid "
                            + PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT
                            + ": table '" + baseTableName.getTbl() + "' is configured more than once");
                }
                matchedLimit = entry.getValue();
            }
        }
        return matchedLimit;
    }

    /**
     * Partition ids of the configured window — the last N partitions ordered by
     * partition value (range upper bound / list value), matching the semantics of
     * {@code partition_sync_limit} and dynamic partition retention. Returns
     * {@code null} when the table is not configured or when the window covers all
     * current partitions — in both cases the caller keeps the full table semantics.
     * A non-partitioned table has a single default partition and returns {@code null}.
     */
    public static List<Long> getIvmPartitionWindowIds(OlapTable table, TableNameInfo tableName,
            Map<TableNameInfo, Integer> windowLimits) {
        int limit = getPartitionWindowLimit(windowLimits, tableName);
        if (limit <= 0) {
            return null;
        }
        PartitionInfo partitionInfo = table.getPartitionInfo();
        List<Map.Entry<Long, PartitionItem>> idToItems =
                new ArrayList<>(partitionInfo.getIdToItem(false).entrySet());
        Comparator<Map.Entry<Long, PartitionItem>> valueOrder;
        if (partitionInfo instanceof RangePartitionInfo) {
            // Range partition: order by the range upper bound (same as auto-partition retention).
            valueOrder = Comparator.comparing(
                    entry -> ((RangePartitionItem) entry.getValue()).getItems().upperEndpoint());
        } else if (partitionInfo instanceof ListPartitionInfo) {
            // List partition: order by the minimum list value; there is no time semantics.
            valueOrder = Comparator.comparing(entry -> Collections.min(
                    ((ListPartitionItem) entry.getValue()).getItems()));
        } else {
            // Non-partitioned (single default partition) table: the window covers the full table.
            return null;
        }
        idToItems.sort(valueOrder);
        if (idToItems.size() <= limit) {
            return null;
        }
        List<Long> windowPartitionIds = Lists.newArrayListWithCapacity(limit);
        for (int i = idToItems.size() - limit; i < idToItems.size(); i++) {
            windowPartitionIds.add(idToItems.get(i).getKey());
        }
        return windowPartitionIds;
    }

    public static Set<TableNameInfo> parseTableNameInfos(String value) {
        Set<TableNameInfo> tableNameInfos = Sets.newHashSet();
        if (StringUtils.isEmpty(value)) {
            return tableNameInfos;
        }
        for (String tableName : value.split(",")) {
            String trimmed = tableName.trim();
            if (!trimmed.isEmpty()) {
                tableNameInfos.add(new TableNameInfo(trimmed));
            }
        }
        return tableNameInfos;
    }

    private static void analyzeDataChangeStillRewrittenTables(String value) {
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

    public static boolean isIvmUseFullKeys(Map<String, String> mvProperties) {
        return mvProperties != null && "true".equalsIgnoreCase(
                mvProperties.get(PropertyAnalyzer.PROPERTIES_IVM_USE_FULL_KEYS));
    }
}

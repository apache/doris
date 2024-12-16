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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.DynamicPartitionUtil.StartOfDate;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.TimeUtils;

import com.google.common.base.Strings;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.BiConsumer;

public class DynamicPartitionProperty {
    public static final String DYNAMIC_PARTITION_PROPERTY_PREFIX = "dynamic_partition.";
    public static final String TIME_UNIT = "dynamic_partition.time_unit";
    public static final String START = "dynamic_partition.start";
    public static final String END = "dynamic_partition.end";
    public static final String PREFIX = "dynamic_partition.prefix";
    public static final String BUCKETS = "dynamic_partition.buckets";
    public static final String ENABLE = "dynamic_partition.enable";
    public static final String START_DAY_OF_WEEK = "dynamic_partition.start_day_of_week";
    public static final String START_DAY_OF_MONTH = "dynamic_partition.start_day_of_month";
    public static final String TIME_ZONE = "dynamic_partition.time_zone";
    public static final String REPLICATION_NUM = "dynamic_partition.replication_num";
    public static final String REPLICATION_ALLOCATION = "dynamic_partition.replication_allocation";
    public static final String CREATE_HISTORY_PARTITION = "dynamic_partition.create_history_partition";
    public static final String HISTORY_PARTITION_NUM = "dynamic_partition.history_partition_num";
    public static final String HOT_PARTITION_NUM = "dynamic_partition.hot_partition_num";
    public static final String RESERVED_HISTORY_PERIODS = "dynamic_partition.reserved_history_periods";
    public static final String STORAGE_POLICY = "dynamic_partition.storage_policy";
    public static final String STORAGE_MEDIUM = "dynamic_partition.storage_medium";

    public static final Set<String> DYNAMIC_PARTITION_PROPERTIES = new HashSet<>(
            Arrays.asList(TIME_UNIT, START, END, PREFIX, BUCKETS, ENABLE, START_DAY_OF_WEEK, START_DAY_OF_MONTH,
                    TIME_ZONE, REPLICATION_NUM, REPLICATION_ALLOCATION, CREATE_HISTORY_PARTITION, HISTORY_PARTITION_NUM,
                    HOT_PARTITION_NUM, RESERVED_HISTORY_PERIODS, STORAGE_POLICY, STORAGE_MEDIUM));

    public static final int MIN_START_OFFSET = Integer.MIN_VALUE;
    public static final int MAX_END_OFFSET = Integer.MAX_VALUE;
    public static final int NOT_SET_REPLICATION_NUM = -1;
    public static final int NOT_SET_HISTORY_PARTITION_NUM = -1;
    public static final String NOT_SET_RESERVED_HISTORY_PERIODS = "NULL";

    private boolean exist;

    private boolean enable;
    private String timeUnit;
    private int start;
    private int end;
    private String prefix;
    private int buckets;
    private StartOfDate startOfWeek;
    private StartOfDate startOfMonth;
    private TimeZone tz = TimeUtils.getSystemTimeZone();
    // if NOT_SET, it will use table's default replica allocation
    private ReplicaAllocation replicaAlloc;
    private boolean createHistoryPartition = false;
    private int historyPartitionNum;
    // This property are used to describe the number of partitions that need to be reserved on the high-speed storage.
    // If not set, default is 0
    private int hotPartitionNum;
    private String reservedHistoryPeriods;
    private String storagePolicy;
    private String storageMedium; // ssd or hdd

    public DynamicPartitionProperty(Map<String, String> properties) {
        if (properties != null && !properties.isEmpty()) {
            this.exist = true;
            this.enable = Boolean.parseBoolean(properties.get(ENABLE));
            this.timeUnit = properties.get(TIME_UNIT);
            this.tz = TimeUtils.getOrSystemTimeZone(properties.get(TIME_ZONE));
            // In order to compatible dynamic add partition version
            this.start = Integer.parseInt(properties.getOrDefault(START, String.valueOf(MIN_START_OFFSET)));
            this.end = Integer.parseInt(properties.get(END));
            this.prefix = properties.get(PREFIX);
            this.buckets = Integer.parseInt(properties.get(BUCKETS));
            this.replicaAlloc = analyzeReplicaAllocation(properties);
            this.createHistoryPartition = Boolean.parseBoolean(properties.get(CREATE_HISTORY_PARTITION));
            this.historyPartitionNum = Integer.parseInt(properties.getOrDefault(
                    HISTORY_PARTITION_NUM, String.valueOf(NOT_SET_HISTORY_PARTITION_NUM)));
            this.hotPartitionNum = Integer.parseInt(properties.getOrDefault(HOT_PARTITION_NUM, "0"));
            this.reservedHistoryPeriods = properties.getOrDefault(
                    RESERVED_HISTORY_PERIODS, NOT_SET_RESERVED_HISTORY_PERIODS);
            this.storagePolicy = properties.getOrDefault(STORAGE_POLICY, "");
            this.storageMedium = properties.getOrDefault(STORAGE_MEDIUM, "");
            createStartOfs(properties);
        } else {
            this.exist = false;
        }
    }

    protected boolean supportProperty(String property) {
        return true;
    }

    private ReplicaAllocation analyzeReplicaAllocation(Map<String, String> properties) {
        try {
            return PropertyAnalyzer.analyzeReplicaAllocation(properties, "dynamic_partition");
        } catch (AnalysisException e) {
            // should not happen
            return ReplicaAllocation.NOT_SET;
        }
    }

    private void createStartOfs(Map<String, String> properties) {
        if (properties.containsKey(START_DAY_OF_WEEK)) {
            startOfWeek = new StartOfDate(-1, -1, Integer.valueOf(properties.get(START_DAY_OF_WEEK)));
        } else {
            // default:
            startOfWeek = new StartOfDate(-1, -1, 1 /* start from MONDAY */);
        }

        if (properties.containsKey(START_DAY_OF_MONTH)) {
            startOfMonth = new StartOfDate(-1, Integer.valueOf(properties.get(START_DAY_OF_MONTH)), -1);
        } else {
            // default:
            startOfMonth = new StartOfDate(-1, 1 /* 1st of month */, -1);
        }
    }

    public boolean isExist() {
        return exist;
    }

    public String getTimeUnit() {
        return timeUnit;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public String getPrefix() {
        return prefix;
    }

    public int getBuckets() {
        return buckets;
    }

    public boolean getEnable() {
        return enable;
    }

    public StartOfDate getStartOfWeek() {
        return startOfWeek;
    }

    public StartOfDate getStartOfMonth() {
        return startOfMonth;
    }

    public boolean isCreateHistoryPartition() {
        return createHistoryPartition;
    }

    public int getHistoryPartitionNum() {
        return historyPartitionNum;
    }

    public int getHotPartitionNum() {
        return hotPartitionNum;
    }

    public String getStoragePolicy() {
        return storagePolicy;
    }

    public String getStorageMedium() {
        return storageMedium;
    }

    public String getStartOfInfo() {
        if (getTimeUnit().equalsIgnoreCase(TimeUnit.WEEK.toString())) {
            return startOfWeek.toDisplayInfo();
        } else if (getTimeUnit().equalsIgnoreCase(TimeUnit.MONTH.toString())) {
            return startOfMonth.toDisplayInfo();
        } else {
            return FeConstants.null_string;
        }
    }

    public TimeZone getTimeZone() {
        return tz;
    }

    public ReplicaAllocation getReplicaAllocation() {
        return replicaAlloc;
    }

    public String getReservedHistoryPeriods() {
        return reservedHistoryPeriods;
    }

    public String getSortedReservedHistoryPeriods(String reservedHistoryPeriods, String timeUnit) throws DdlException {
        return DynamicPartitionUtil.sortedListedToString(reservedHistoryPeriods, timeUnit);
    }

    /**
     * use table replication_num as dynamic_partition.replication_num default value
     */
    public String getProperties(ReplicaAllocation tableReplicaAlloc) {
        ReplicaAllocation tmpAlloc = this.replicaAlloc.isNotSet() ? tableReplicaAlloc : this.replicaAlloc;
        StringBuilder sb = new StringBuilder();
        BiConsumer<String, Object> addProperty = (property, value) -> {
            if (supportProperty(property)) {
                sb.append(",\n\"" + property + "\" = \"" + value + "\"");
            }
        };
        addProperty.accept(ENABLE, enable);
        addProperty.accept(TIME_UNIT, timeUnit);
        addProperty.accept(TIME_ZONE, tz.getID());
        addProperty.accept(START, start);
        addProperty.accept(END, end);
        addProperty.accept(PREFIX, prefix);
        addProperty.accept(REPLICATION_ALLOCATION, tmpAlloc.toCreateStmt());
        addProperty.accept(BUCKETS, buckets);
        addProperty.accept(CREATE_HISTORY_PARTITION, createHistoryPartition);
        addProperty.accept(HISTORY_PARTITION_NUM, historyPartitionNum);
        addProperty.accept(HOT_PARTITION_NUM, hotPartitionNum);
        addProperty.accept(RESERVED_HISTORY_PERIODS, reservedHistoryPeriods);
        addProperty.accept(STORAGE_POLICY, storagePolicy);
        if (!Strings.isNullOrEmpty(storageMedium)) {
            addProperty.accept(STORAGE_MEDIUM, storageMedium);
        }
        if (getTimeUnit().equalsIgnoreCase(TimeUnit.WEEK.toString())) {
            addProperty.accept(START_DAY_OF_WEEK, startOfWeek.dayOfWeek);
        } else if (getTimeUnit().equalsIgnoreCase(TimeUnit.MONTH.toString())) {
            addProperty.accept(START_DAY_OF_MONTH, startOfMonth.day);
        }

        return sb.toString();
    }

}

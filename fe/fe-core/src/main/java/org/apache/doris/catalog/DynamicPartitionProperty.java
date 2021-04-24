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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DynamicPartitionUtil.StartOfDate;
import org.apache.doris.common.util.TimeUtils;

import java.util.Map;
import java.util.TimeZone;

public class DynamicPartitionProperty {
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
    public static final String CREATE_HISTORY_PARTITION = "dynamic_partition.create_history_partition";

    public static final int MIN_START_OFFSET = Integer.MIN_VALUE;
    public static final int MAX_END_OFFSET = Integer.MAX_VALUE;
    public static final int NOT_SET_REPLICATION_NUM = -1;

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
    private int replicationNum;
    private boolean createHistoryPartition = false;

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
            this.replicationNum = Integer.parseInt(properties.getOrDefault(REPLICATION_NUM, String.valueOf(NOT_SET_REPLICATION_NUM)));
            this.createHistoryPartition = Boolean.parseBoolean(properties.get(CREATE_HISTORY_PARTITION));
            createStartOfs(properties);
        } else {
            this.exist = false;
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

    public int getReplicationNum() {
        return replicationNum;
    }

    /**
     * use table replication_num as dynamic_partition.replication_num default value
     */
    public String getProperties(int tableReplicationNum) {
        int useReplicationNum = replicationNum;
        if (useReplicationNum == NOT_SET_REPLICATION_NUM) {
            useReplicationNum = tableReplicationNum;
        }
        String res = ",\n\"" + ENABLE + "\" = \"" + enable + "\"" +
                ",\n\"" + TIME_UNIT + "\" = \"" + timeUnit + "\"" +
                ",\n\"" + TIME_ZONE + "\" = \"" + tz.getID() + "\"" +
                ",\n\"" + START + "\" = \"" + start + "\"" +
                ",\n\"" + END + "\" = \"" + end + "\"" +
                ",\n\"" + PREFIX + "\" = \"" + prefix + "\"" +
                ",\n\"" + REPLICATION_NUM + "\" = \"" + useReplicationNum + "\"" +
                ",\n\"" + BUCKETS + "\" = \"" + buckets + "\"" +
                ",\n\"" + CREATE_HISTORY_PARTITION + "\" = \"" + createHistoryPartition + "\"";
        if (getTimeUnit().equalsIgnoreCase(TimeUnit.WEEK.toString())) {
            res += ",\n\"" + START_DAY_OF_WEEK + "\" = \"" + startOfWeek.dayOfWeek + "\"";
        } else if (getTimeUnit().equalsIgnoreCase(TimeUnit.MONTH.toString())) {
            res += ",\n\"" + START_DAY_OF_MONTH + "\" = \"" + startOfMonth.day + "\"";
        }
        return res;
    }
}

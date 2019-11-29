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
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.catalog.DynamicPartitionUtils.DynamicPartitionProperties;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TableProperty implements Writable {

    public enum Status {
        LAST_SCHEDULER_TIME("LastSchedulerTime"),
        LAST_UPDATE_TIME("LastUpdateTime"),
        MSG("Msg"),
        STATE("State");

        private String desc;

        Status(String desc) {
            this.desc = desc;
        }

        public String getDesc() {
            return desc;
        }
    }

    public enum State {
        NORMAL,
        CANCELLED,
        ERROR
    }

    private static final String TIMESTAMP_FORMAT = "yyyyMMdd";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private Map<String, String> dynamicProperties = new HashMap<>();

    public TableProperty() {}

    public TableProperty(Map<String, String> dynamicProperties) {
        this.dynamicProperties = dynamicProperties;
    }

    public Map<String, String> getDynamicProperties() {
        return dynamicProperties;
    }

    public String getDynamicPartitionTimeUnit() {
        return dynamicProperties.get(DynamicPartitionProperties.TIME_UNIT.getDesc());
    }

    public String getDynamicPartitionPrefix() {
        return dynamicProperties.get(DynamicPartitionProperties.PREFIX.getDesc());
    }

    public String getDynamicPartitionEnd() {
        return dynamicProperties.get(DynamicPartitionProperties.END.getDesc());
    }

    public String getDynamicPartitionBuckets() {
        return dynamicProperties.get(DynamicPartitionProperties.BUCKETS.getDesc());
    }

    public String getDynamicPartitionEnable() {
        return dynamicProperties.get(DynamicPartitionProperties.ENABLE.getDesc());
    }

    public void setLastSchedulerTime(String time) {
        dynamicProperties.put(Status.LAST_SCHEDULER_TIME.getDesc(), time);
    }

    public String getLastSchedulerTime() {
        return dynamicProperties.get(Status.LAST_SCHEDULER_TIME.getDesc());
    }

    public void setLastUpdateTime(String time) {
        dynamicProperties.put(Status.LAST_UPDATE_TIME.getDesc(), time);
    }

    public String getLastUpdateTime() {
        return dynamicProperties.get(Status.LAST_UPDATE_TIME.getDesc());
    }

    public void setState(String state) {
        dynamicProperties.put(Status.STATE.getDesc(), state);
    }

    public String getState() {
        return dynamicProperties.get(Status.STATE.getDesc());
    }

    public void setMsg(String msg) {
        dynamicProperties.put(Status.MSG.getDesc(), msg);
    }

    public String getMsg() {
        return dynamicProperties.get(Status.MSG.getDesc());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        JSONObject jsonObject = new JSONObject();
        for (Map.Entry<String, String> entry : dynamicProperties.entrySet()) {
            jsonObject.put(entry.getKey(), entry.getValue());
        }
        Text.writeString(out, jsonObject.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String jsonProperties = Text.readString(in);
        JSONObject jsonObject = new JSONObject(jsonProperties);
        Iterator<String> iterator = jsonObject.keys();
        while (iterator.hasNext()) {
            String key = iterator.next();
            dynamicProperties.put(key, jsonObject.getString(key));
        }
    }

    public static TableProperty read(DataInput in) throws IOException {
        TableProperty info = new TableProperty();
        info.readFields(in);
        return info;
    }

    public static boolean checkDynamicPartitionTable(Table table) {
        if (!(table instanceof OlapTable) ||
                !(((OlapTable) table).getPartitionInfo().getType().equals(PartitionType.RANGE))) {
            return false;
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) ((OlapTable) table).getPartitionInfo();
        String enable = ((OlapTable) table).getTableProperty().getDynamicPartitionEnable();
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

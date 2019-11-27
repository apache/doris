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
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PropertyAnalyzer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TableProperty implements Writable {
    private Map<String, String> dynamicProperties = new HashMap<>();

    public static final String LAST_UPDATE_TIME = "LastUpdateTime";
    public static final String MSG = "Msg";
    public static final String STATE = "State";

    private long dbId;
    private long tableId;

    public enum State {
        NORMAL,
        CANCELLED,
        ERROR
    }

    public TableProperty() {}

    public TableProperty(long dbId, long tableId, Map<String, String> dynamicProperties) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.dynamicProperties = dynamicProperties;
    }

    public long getTableId() {
        return tableId;
    }

    public long getDbId() {
        return dbId;
    }

    public Map<String, String> getDynamicProperties() {
        return dynamicProperties;
    }

    public String getDynamicPartitionTimeUnit() {
        return this.dynamicProperties.get(PropertyAnalyzer.PROPERTIES_DYNAMIC_PARTITION_TIME_UNIT);
    }

    public String getDynamicPartitionPrefix() {
        return this.dynamicProperties.get(PropertyAnalyzer.PROPERTIES_DYNAMIC_PARTITION_PREFIX);
    }

    public String getDynamicPartitionEnd() {
        return this.dynamicProperties.get(PropertyAnalyzer.PROPERTIES_DYNAMIC_PARTITION_END);
    }

    public String getDynamicPartitionBuckets() {
        return this.dynamicProperties.get(PropertyAnalyzer.PROPERTIES_DYNAMIC_PARTITION_BUCKETS);
    }


    public String getDynamicPartitionEnable() {
        return this.dynamicProperties.get(PropertyAnalyzer.PROPERTIES_DYANMIC_PARTITION_ENABLE);
    }

    public String getLastUpdateTime() {
        return this.dynamicProperties.get(LAST_UPDATE_TIME);
    }

    public void setState(State state) {
        this.dynamicProperties.put(STATE, state.toString());
    }

    public String getState() {
        return this.dynamicProperties.get(STATE);
    }

    public void setMsg(String msg) {
        this.dynamicProperties.put(MSG, msg);
    }

    public String getMsg() {
        String msg = this.dynamicProperties.get(MSG);
        return Strings.isNullOrEmpty(msg) ? "N/A" : msg;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        int counter = dynamicProperties.size();
        out.writeInt(counter);
        for (Map.Entry<String, String> entry : dynamicProperties.entrySet()) {
            String dynamicPropertyName = entry.getKey();
            String dynamicPropertyValue = entry.getValue();
            Text.writeString(out, dynamicPropertyName);
            Text.writeString(out, dynamicPropertyValue);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        int counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            String dynamicPropertyName = Text.readString(in);
            String dynamicPropertyValue = Text.readString(in);
            dynamicProperties.put(dynamicPropertyName, dynamicPropertyValue);
        }
    }

    public static TableProperty read(DataInput in) throws IOException {
        TableProperty info = new TableProperty();
        info.readFields(in);
        return info;
    }
}

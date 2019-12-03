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

package org.apache.doris.persist;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DynamicPartitionInfo implements Writable {

    private long dbId;
    private long tableId;
    private Map<String, String> properties = new HashMap<>();

    public DynamicPartitionInfo() {

    }

    public DynamicPartitionInfo(long dbId, long tableId, Map<String, String> properties) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.properties = properties;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public static DynamicPartitionInfo read(DataInput in) throws IOException {
        DynamicPartitionInfo info = new DynamicPartitionInfo();
        info.readFields(in);
        return info;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        JSONObject jsonObject = new JSONObject();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            jsonObject.put(entry.getKey(), entry.getValue());
        }
        Text.writeString(out, jsonObject.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        String jsonProperties = Text.readString(in);
        JSONObject jsonObject = new JSONObject(jsonProperties);
        Iterator<String> iterator = jsonObject.keys();
        while (iterator.hasNext()) {
            String key = iterator.next();
            properties.put(key, jsonObject.getString(key));
        }
    }
}

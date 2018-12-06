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
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * PersistInfo for Table properties
 */
public class TablePropertyInfo implements Writable {
    private long dbId;
    private long tableId;
    private Map<String, String> propertyMap;

    public TablePropertyInfo() {

    }

    public TablePropertyInfo(long dbId, long tableId, Map<String, String> propertyMap) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.propertyMap = propertyMap;
    }

    public Map<String, String> getPropertyMap() {
        return propertyMap;
    }

    public void setPropertyMap(Map<String, String> propertyMap) {
        this.propertyMap = propertyMap;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        int size = propertyMap.size();
        out.writeInt(size);
        for (Map.Entry<String, String> kv : propertyMap.entrySet()) {
            Text.writeString(out, kv.getKey());
            Text.writeString(out, kv.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();

        int size = in.readInt();
        propertyMap = Maps.newHashMap();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            propertyMap.put(key, value);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof TablePropertyInfo)) {
            return false;
        }

        TablePropertyInfo info = (TablePropertyInfo) obj;

        return dbId == info.dbId && tableId == info.tableId && propertyMap.equals(info.propertyMap);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("db id: ").append(dbId);
        sb.append(" table id: ").append(tableId);
        sb.append(" propertyMap: ").append(propertyMap);
        return sb.toString();
    }
}

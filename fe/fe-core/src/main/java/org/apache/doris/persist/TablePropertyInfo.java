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

import org.apache.doris.catalog.ColocateTableIndex.GroupId;
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
    private long tableId;
    private Map<String, String> propertyMap;
    private GroupId groupId;

    public TablePropertyInfo() {

    }

    public TablePropertyInfo(long tableId, GroupId groupId, Map<String, String> propertyMap) {
        this.tableId = tableId;
        this.groupId = groupId;
        this.propertyMap = propertyMap;
    }

    public Map<String, String> getPropertyMap() {
        return propertyMap;
    }

    public long getTableId() {
        return tableId;
    }

    public GroupId getGroupId() {
        return groupId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tableId);
        if (groupId == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            groupId.write(out);
        }
        int size = propertyMap.size();
        out.writeInt(size);
        for (Map.Entry<String, String> kv : propertyMap.entrySet()) {
            Text.writeString(out, kv.getKey());
            Text.writeString(out, kv.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        long dbId = -1;
        tableId = in.readLong();
        if (in.readBoolean()) {
            groupId = GroupId.read(in);
        }

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

        return tableId == info.tableId && groupId.equals(info.groupId)
                && propertyMap.equals(info.propertyMap);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" table id: ").append(tableId);
        sb.append(" group id: ").append(groupId);
        sb.append(" propertyMap: ").append(propertyMap);
        return sb.toString();
    }
}

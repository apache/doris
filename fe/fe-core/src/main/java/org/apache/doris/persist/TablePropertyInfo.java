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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * PersistInfo for Table properties
 */
public class TablePropertyInfo implements Writable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "propertyMap")
    private Map<String, String> propertyMap;
    @SerializedName(value = "groupId")
    private GroupId groupId;

    private TablePropertyInfo() {
    }

    public TablePropertyInfo(long dbId, long tableId, GroupId groupId, Map<String, String> propertyMap) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.groupId = groupId;
        this.propertyMap = propertyMap;
    }

    public Map<String, String> getPropertyMap() {
        return propertyMap;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public GroupId getGroupId() {
        return groupId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TablePropertyInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_119) {
            TablePropertyInfo info = new TablePropertyInfo();
            info.readFields(in);
            return info;
        } else {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, TablePropertyInfo.class);
        }
    }

    @Deprecated
    private void readFields(DataInput in) throws IOException {
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

        return dbId == info.dbId && tableId == info.tableId && groupId.equals(info.groupId)
                && propertyMap.equals(info.propertyMap);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" db id: ").append(dbId);
        sb.append(" table id: ").append(tableId);
        sb.append(" group id: ").append(groupId);
        sb.append(" propertyMap: ").append(propertyMap);
        return sb.toString();
    }
}

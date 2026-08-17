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
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * PersistInfo for ColocateTableIndex.
 */
public class TenantLevelColocateTableInfo implements Writable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "groupMap")
    private Map<Long, TenantLevelColocateGroupInfo> groupMap = Maps.newHashMap();

    public TenantLevelColocateTableInfo() {
    }

    public TenantLevelColocateTableInfo(long dbId, long tableId,
            Map<Long, TenantLevelColocateGroupInfo> groupMap) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.groupMap = groupMap;
    }

    public static TenantLevelColocateTableInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TenantLevelColocateTableInfo.class);
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Map<Long, TenantLevelColocateGroupInfo> getGroupMap() {
        return groupMap;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        TenantLevelColocateTableInfo that = (TenantLevelColocateTableInfo) object;
        return dbId == that.dbId && tableId == that.tableId && Objects.equals(groupMap,
                that.groupMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, tableId, groupMap);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("db id: ").append(dbId);
        sb.append(", table id: ").append(tableId);
        sb.append(", groupMap: ").append(groupMap);
        sb.append("}");
        return sb.toString();
    }

}

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

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DropPartitionInfo implements Writable {
    @SerializedName(value = "dbId")
    private Long dbId;
    @SerializedName(value = "tableId")
    private Long tableId;
    @SerializedName(value = "pid")
    private Long partitionId;
    @SerializedName(value = "partitionName")
    private String partitionName;
    @SerializedName(value = "isTempPartition")
    private boolean isTempPartition = false;
    @SerializedName(value = "forceDrop")
    private boolean forceDrop = false;
    @SerializedName(value = "recycleTime")
    private long recycleTime = 0;
    @SerializedName(value = "sql")
    private String sql;
    @SerializedName(value = "version")
    private long version = 0L;
    @SerializedName(value = "versionTime")
    private long versionTime = 0L;

    public DropPartitionInfo(Long dbId, Long tableId, Long partitionId, String partitionName,
            boolean isTempPartition, boolean forceDrop, long recycleTime, long version, long versionTime) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.partitionName = partitionName;
        this.isTempPartition = isTempPartition;
        this.forceDrop = forceDrop;
        this.recycleTime = recycleTime;

        StringBuilder sb = new StringBuilder();
        sb.append("DROP PARTITION ");
        if (isTempPartition) {
            sb.append("TEMPORARY ");
        }
        sb.append("`").append(partitionName).append("`");
        if (forceDrop) {
            sb.append(" FORCE");
        }
        this.sql = sb.toString();
        this.version = version;
        this.versionTime = versionTime;
    }

    public Long getDbId() {
        return dbId;
    }

    public Long getTableId() {
        return tableId;
    }

    public Long getPartitionId() {
        // the field partition ID was added in PR: apache/doris#37196, the old version doesn't
        // contain this field so it will be null.
        return partitionId == null ? -1 : partitionId;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    public boolean isForceDrop() {
        return forceDrop;
    }

    public Long getRecycleTime() {
        return recycleTime;
    }

    public long getVersion() {
        return version;
    }

    public long getVersionTime() {
        return versionTime;
    }

    public static DropPartitionInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DropPartitionInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static DropPartitionInfo fromJson(String data) {
        return GsonUtils.GSON.fromJson(data, DropPartitionInfo.class);
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DropPartitionInfo)) {
            return false;
        }

        DropPartitionInfo info = (DropPartitionInfo) obj;

        return (dbId.equals(info.dbId))
                && (tableId.equals(info.tableId))
                && (partitionId.equals(info.partitionId))
                && (partitionName.equals(info.partitionName))
                && (isTempPartition == info.isTempPartition)
                && (forceDrop == info.forceDrop)
                && (recycleTime == info.recycleTime);
    }
}

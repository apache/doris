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

package org.apache.doris.transaction;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionCommitInfo implements Writable {

    @SerializedName(value = "partitionId")
    private long partitionId;
    @SerializedName(value = "range")
    private String range;
    @SerializedName(value = "version")
    private long version;
    @SerializedName(value = "versionTime")
    private long versionTime;
    @SerializedName(value = "isTempPartition")
    private boolean isTempPartition;

    public PartitionCommitInfo() {

    }

    public PartitionCommitInfo(long partitionId, String partitionRange, long version, long visibleTime,
            boolean isTempPartition) {
        super();
        this.partitionId = partitionId;
        this.range = partitionRange;
        this.version = version;
        this.versionTime = visibleTime;
        this.isTempPartition = isTempPartition;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static PartitionCommitInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, PartitionCommitInfo.class);
    }

    public long getPartitionId() {
        return partitionId;
    }

    public String getPartitionRange() {
        return range;
    }

    public long getVersion() {
        return version;
    }

    public long getVersionTime() {
        return versionTime;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void setVersionTime(long versionTime) {
        this.versionTime = versionTime;
    }

    public boolean isTempPartition() {
        return this.isTempPartition;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("partitionId=");
        sb.append(partitionId);
        sb.append(", version=").append(version);
        sb.append(", versionTime=").append(versionTime);
        sb.append(", isTemp=").append(isTempPartition);
        return sb.toString();
    }
}

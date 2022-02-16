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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeMetaVersion;
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
    @SerializedName(value = "version")
    private long version;
    @SerializedName(value = "versionTime")
    private long versionTime;
    @SerializedName(value = "versionHash")
    private long versionHash;

    public PartitionCommitInfo() {
        
    }

    public PartitionCommitInfo(long partitionId, long version, long versionHash, long visibleTime) {
        super();
        this.partitionId = partitionId;
        this.version = version;
        this.versionTime = visibleTime;
        this.versionHash = versionHash;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static PartitionCommitInfo read(DataInput in) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_88) {
            long partitionId = in.readLong();
            long version = in.readLong();
            long versionHash = in.readLong();
            return new PartitionCommitInfo(partitionId, version, versionHash, System.currentTimeMillis());
        } else {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, PartitionCommitInfo.class);
        }
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getVersion() {
        return version;
    }
    
    public long getVersionTime() {
        return versionTime;
    }

    public long getVersionHash() {
        return versionHash;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void setVersionTime(long versionTime) {
        this.versionTime = versionTime;
    }

    public void setVersionHash(long versionHash) {
        this.versionHash = versionHash;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("partitionid=");
        sb.append(partitionId);
        sb.append(", version=").append(version);
        sb.append(", versionHash=").append(versionHash);
        sb.append(", versionTime=").append(versionTime);
        return sb.toString();
    }
}

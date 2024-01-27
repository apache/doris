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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class TableCommitInfo implements Writable {

    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "idToPartitionCommitInfo")
    private Map<Long, PartitionCommitInfo> idToPartitionCommitInfo;
    @SerializedName(value = "version")
    private long version;
    @SerializedName(value = "versionTime")
    private long versionTime;

    public TableCommitInfo() {

    }

    public TableCommitInfo(long tableId, long version, long visibleTime) {
        this.tableId = tableId;
        idToPartitionCommitInfo = Maps.newHashMap();
        this.version = version;
        this.versionTime = visibleTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tableId);
        if (idToPartitionCommitInfo == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(idToPartitionCommitInfo.size());
            for (PartitionCommitInfo partitionCommitInfo : idToPartitionCommitInfo.values()) {
                partitionCommitInfo.write(out);
            }
        }
        out.writeLong(version);
        out.writeLong(versionTime);
    }

    public void readFields(DataInput in) throws IOException {
        tableId = in.readLong();
        boolean hasPartitionInfo = in.readBoolean();
        idToPartitionCommitInfo = Maps.newHashMap();
        if (hasPartitionInfo) {
            int elementNum = in.readInt();
            for (int i = 0; i < elementNum; ++i) {
                PartitionCommitInfo partitionCommitInfo = PartitionCommitInfo.read(in);
                idToPartitionCommitInfo.put(partitionCommitInfo.getPartitionId(), partitionCommitInfo);
            }
        }
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_129) {
            version = in.readLong();
            versionTime = in.readLong();
        }
    }

    public long getTableId() {
        return tableId;
    }

    public Map<Long, PartitionCommitInfo> getIdToPartitionCommitInfo() {
        return idToPartitionCommitInfo;
    }

    public void addPartitionCommitInfo(PartitionCommitInfo info) {
        this.idToPartitionCommitInfo.put(info.getPartitionId(), info);
    }

    public void removePartition(long partitionId) {
        this.idToPartitionCommitInfo.remove(partitionId);
    }

    public PartitionCommitInfo getPartitionCommitInfo(long partitionId) {
        return this.idToPartitionCommitInfo.get(partitionId);
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getVersionTime() {
        return versionTime;
    }

    public void setVersionTime(long versionTime) {
        this.versionTime = versionTime;
    }
}

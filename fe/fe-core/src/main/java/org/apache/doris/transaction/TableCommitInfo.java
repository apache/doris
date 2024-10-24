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
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TPartitionVersionInfo;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableCommitInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(TableCommitInfo.class);

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

    public TableCommitInfo(long tableId) {
        this.tableId = tableId;
        idToPartitionCommitInfo = Maps.newHashMap();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static TableCommitInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_129) {
            TableCommitInfo info = new TableCommitInfo();
            info.readFields(in);
            return info;
        } else {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, TableCommitInfo.class);
        }
    }

    @Deprecated
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

    public List<TPartitionVersionInfo> generateTPartitionVersionInfos() {
        return idToPartitionCommitInfo
                .values().stream()
                .map(commitInfo -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("try to publish version info partitionid [{}], version [{}]",
                                commitInfo.getPartitionId(), commitInfo.getVersion());
                    }
                    return new TPartitionVersionInfo(commitInfo.getPartitionId(),
                            commitInfo.getVersion(), 0);
                }).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return new StringBuilder("TableCommitInfo{tableId=").append(tableId)
                .append(", idToPartitionCommitInfo=").append(idToPartitionCommitInfo)
                .append(", version=").append(version).append(", versionTime=").append(versionTime)
                .append('}').toString();
    }
}

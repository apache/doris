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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

// This class is used to record the replica information that needs to be persisted
// and synchronized to other FEs when BE reports tablet,
// such as bad replica or missing version replica information, etc.
public class BackendReplicasInfo implements Writable {

    @SerializedName(value = "backendId")
    private long backendId;
    @SerializedName(value = "replicaReportInfos")
    private List<ReplicaReportInfo> replicaReportInfos = Lists.newArrayList();

    public BackendReplicasInfo(long backendId) {
        this.backendId = backendId;
    }

    public void addBadReplica(long tabletId) {
        replicaReportInfos.add(new ReplicaReportInfo(tabletId, -1, ReportInfoType.BAD));
    }

    public void addMissingVersionReplica(long tabletId, long lastFailedVersion) {
        replicaReportInfos.add(new ReplicaReportInfo(tabletId, lastFailedVersion, ReportInfoType.MISSING_VERSION));
    }

    public long getBackendId() {
        return backendId;
    }

    public List<ReplicaReportInfo> getReplicaReportInfos() {
        return replicaReportInfos;
    }

    public boolean isEmpty() {
        return replicaReportInfos.isEmpty();
    }

    public static BackendReplicasInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, BackendReplicasInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public enum ReportInfoType {
        BAD,
        MISSING_VERSION
    }

    public static class ReplicaReportInfo implements Writable {
        @SerializedName(value = "tabletId")
        public long tabletId;
        @SerializedName(value = "type")
        public ReportInfoType type;
        @SerializedName(value = "lastFailedVersion")
        public long lastFailedVersion;

        public ReplicaReportInfo(long tabletId, long lastFailedVersion, ReportInfoType type) {
            this.tabletId = tabletId;
            this.lastFailedVersion = lastFailedVersion;
            this.type = type;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            String json = GsonUtils.GSON.toJson(this);
            Text.writeString(out, json);
        }

        public static ReplicaReportInfo read(DataInput in) throws IOException {
            String json = Text.readString(in);
            ReplicaReportInfo info = GsonUtils.GSON.fromJson(json, ReplicaReportInfo.class);
            if (info.type == ReportInfoType.MISSING_VERSION && info.lastFailedVersion <= 0) {
                // FIXME(cmy): Just for compatibility, should be remove in v1.2
                info.lastFailedVersion = 1;
            }
            return info;
        }
    }

}

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

import org.apache.doris.catalog.Replica.ReplicaStatus;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SetReplicaStatusOperationLog implements Writable {

    @SerializedName(value = "backendId")
    private long backendId;
    @SerializedName(value = "tabletId")
    private long tabletId;
    @SerializedName(value = "replicaStatus")
    private ReplicaStatus replicaStatus;
    @SerializedName(value = "userDropTime")
    private long userDropTime;

    public SetReplicaStatusOperationLog(long backendId, long tabletId, ReplicaStatus replicaStatus, long userDropTime) {
        this.backendId = backendId;
        this.tabletId = tabletId;
        this.replicaStatus = replicaStatus;
        this.userDropTime = userDropTime;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBackendId() {
        return backendId;
    }

    public ReplicaStatus getReplicaStatus() {
        return replicaStatus;
    }

    public long getUserDropTime() {
        return userDropTime;
    }

    public static SetReplicaStatusOperationLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, SetReplicaStatusOperationLog.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}

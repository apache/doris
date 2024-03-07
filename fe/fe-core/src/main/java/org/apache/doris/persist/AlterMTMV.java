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
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mtmv.MTMVAlterOpType;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRefreshPartitionSnapshot;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class AlterMTMV implements Writable {
    @SerializedName("ot")
    private MTMVAlterOpType opType;
    @SerializedName("mn")
    private TableNameInfo mvName;
    @SerializedName("ri")
    private MTMVRefreshInfo refreshInfo;
    @SerializedName("s")
    private MTMVStatus status;
    @SerializedName("nrj")
    private boolean needRebuildJob = false;
    @SerializedName("mp")
    private Map<String, String> mvProperties;
    @SerializedName("t")
    private MTMVTask task;
    @SerializedName("r")
    private MTMVRelation relation;
    @SerializedName("ps")
    private Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots;

    public AlterMTMV(TableNameInfo mvName, MTMVRefreshInfo refreshInfo, MTMVAlterOpType opType) {
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
        this.refreshInfo = Objects.requireNonNull(refreshInfo, "require refreshInfo object");
        this.opType = Objects.requireNonNull(opType, "require opType object");
        this.needRebuildJob = true;
    }

    public AlterMTMV(TableNameInfo mvName, MTMVAlterOpType opType) {
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
        this.opType = Objects.requireNonNull(opType, "require opType object");
    }

    public TableNameInfo getMvName() {
        return mvName;
    }

    public MTMVStatus getStatus() {
        return status;
    }

    public boolean isNeedRebuildJob() {
        return needRebuildJob;
    }

    public MTMVRefreshInfo getRefreshInfo() {
        return refreshInfo;
    }

    public void setMvName(TableNameInfo mvName) {
        this.mvName = mvName;
    }

    public void setRefreshInfo(MTMVRefreshInfo refreshInfo) {
        this.refreshInfo = refreshInfo;
    }

    public void setStatus(MTMVStatus status) {
        this.status = status;
    }

    public void setMvProperties(Map<String, String> mvProperties) {
        this.mvProperties = mvProperties;
    }

    public Map<String, String> getMvProperties() {
        return mvProperties;
    }

    public void setNeedRebuildJob(boolean needRebuildJob) {
        this.needRebuildJob = needRebuildJob;
    }

    public MTMVAlterOpType getOpType() {
        return opType;
    }

    public MTMVTask getTask() {
        return task;
    }

    public void setTask(MTMVTask task) {
        this.task = task;
    }

    public MTMVRelation getRelation() {
        return relation;
    }

    public void setRelation(MTMVRelation relation) {
        this.relation = relation;
    }

    public Map<String, MTMVRefreshPartitionSnapshot> getPartitionSnapshots() {
        return partitionSnapshots;
    }

    public void setPartitionSnapshots(
            Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots) {
        this.partitionSnapshots = partitionSnapshots;
    }

    @Override
    public String toString() {
        return "AlterMTMV{"
                + "mvName=" + mvName
                + ", refreshInfo=" + refreshInfo
                + ", status=" + status
                + ", needRebuildJob=" + needRebuildJob
                + ", mvProperties=" + mvProperties
                + ", task=" + task
                + ", relation=" + relation
                + '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterMTMV read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), AlterMTMV.class);
    }

}

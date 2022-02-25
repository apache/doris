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

package org.apache.doris.load;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class DeleteInfo implements Writable, GsonPostProcessable {

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "tableName")
    private String tableName;
    @SerializedName(value = "deleteConditions")
    private List<String> deleteConditions;
    @SerializedName(value = "createTimeMs")
    private long createTimeMs;
    @SerializedName(value = "partitionIds")
    private List<Long> partitionIds;
    @SerializedName(value = "partitionNames")
    private List<String> partitionNames;
    @SerializedName(value = "noPartitionSpecified")
    private boolean noPartitionSpecified = false;

    // The following partition id and partition name are deprecated.
    // Leave them here just for compatibility
    @Deprecated
    @SerializedName(value = "partitionId")
    private long partitionId;
    @Deprecated
    @SerializedName(value = "partitionName")
    private String partitionName;

    public DeleteInfo() {
        this.deleteConditions = Lists.newArrayList();
    }

    public DeleteInfo(long dbId, long tableId, String tableName, List<String> deleteConditions) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.deleteConditions = deleteConditions;
        this.createTimeMs = System.currentTimeMillis();
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getDeleteConditions() {
        return deleteConditions;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public boolean isNoPartitionSpecified() {
        return noPartitionSpecified;
    }

    public void setPartitions(boolean noPartitionSpecified, List<Long> partitionIds, List<String> partitionNames) {
        this.noPartitionSpecified = noPartitionSpecified;
        Preconditions.checkState(partitionIds.size() == partitionNames.size());
        this.partitionIds = partitionIds;
        this.partitionNames = partitionNames;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public static DeleteInfo read(DataInput in) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_96) {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, DeleteInfo.class);
        } else {
            DeleteInfo deleteInfo = new DeleteInfo();
            deleteInfo.readFields(in);
            return deleteInfo;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    private void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        long partitionId = in.readLong();
        long partitionVersion = in.readLong();
        long partitionVersionHash = in.readLong();
        this.partitionIds = Lists.newArrayList(partitionId);

        List<ReplicaPersistInfo> replicaInfos = Lists.newArrayList();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            ReplicaPersistInfo info = ReplicaPersistInfo.read(in);
            replicaInfos.add(info);
        }

        tableName = Text.readString(in);
        String partitionName = Text.readString(in);

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            String deleteCond = Text.readString(in);
            deleteConditions.add(deleteCond);
        }

        createTimeMs = in.readLong();
        this.partitionNames = Lists.newArrayList(partitionName);
        boolean hasAsyncDeleteJob = in.readBoolean();
        Preconditions.checkState(!hasAsyncDeleteJob, "async delete job is deprecated");
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // This logic is just for forward compatibility
        if (this.partitionId > 0) {
            Preconditions.checkState(!Strings.isNullOrEmpty(this.partitionName));
            this.partitionIds = Lists.newArrayList(this.partitionId);
            this.partitionNames = Lists.newArrayList(this.partitionName);
        }
    }
}

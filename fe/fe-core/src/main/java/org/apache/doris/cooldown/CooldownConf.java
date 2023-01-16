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

package org.apache.doris.cooldown;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class represents the olap replica related metadata.
 */
public class CooldownConf implements Writable {
    private static final Logger LOG = LogManager.getLogger(CooldownConf.class);

    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "tableId")
    protected long tableId;
    @SerializedName(value = "partitionId")
    protected long partitionId;
    @SerializedName(value = "indexId")
    protected long indexId;
    @SerializedName(value = "tabletId")
    protected long tabletId;
    @SerializedName(value = "cooldownReplicaId")
    protected long cooldownReplicaId;
    @SerializedName(value = "cooldownTerm")
    protected long cooldownTerm;

    public CooldownConf(long dbId, long tableId, long partitionId, long indexId, long tabletId, long cooldownReplicaId,
                        long cooldownTerm) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;
        this.cooldownReplicaId = cooldownReplicaId;
        this.cooldownTerm = cooldownTerm;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(long partitionId) {
        this.partitionId = partitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public void setIndexId(long indexId) {
        this.indexId = indexId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public void setTabletId(long tabletId) {
        this.tabletId = tabletId;
    }

    public long getCooldownReplicaId() {
        return cooldownReplicaId;
    }

    public void setCooldownReplicaId(long cooldownReplicaId) {
        this.cooldownReplicaId = cooldownReplicaId;
    }

    public long getCooldownTerm() {
        return cooldownTerm;
    }

    public void setCooldownTerm(long cooldownTerm) {
        this.cooldownTerm = cooldownTerm;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static CooldownJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CooldownJob.class);
    }
}

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

import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.RangeUtils;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionPersistInfo implements Writable {
    @SerializedName(value = "dbId")
    private Long dbId;
    @SerializedName(value = "tableId")
    private Long tableId;
    @SerializedName(value = "partition")
    private Partition partition;

    @SerializedName(value = "range")
    private Range<PartitionKey> range;
    @SerializedName(value = "listPartitionItem")
    private PartitionItem listPartitionItem;
    @SerializedName(value = "dataProperty")
    private DataProperty dataProperty;
    @SerializedName(value = "replicaAlloc")
    private ReplicaAllocation replicaAlloc;
    @SerializedName(value = "isInMemory")
    private boolean isInMemory = false;
    @SerializedName(value = "isTempPartition")
    private boolean isTempPartition = false;
    @SerializedName(value = "isMutable")
    private boolean isMutable = true;

    public PartitionPersistInfo() {
    }

    public PartitionPersistInfo(long dbId, long tableId, Partition partition, Range<PartitionKey> range,
            PartitionItem listPartitionItem, DataProperty dataProperty, ReplicaAllocation replicaAlloc,
            boolean isInMemory, boolean isTempPartition, boolean isMutable) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partition = partition;

        this.range = range;
        this.listPartitionItem = listPartitionItem;
        this.dataProperty = dataProperty;

        this.replicaAlloc = replicaAlloc;
        this.isInMemory = isInMemory;
        this.isTempPartition = isTempPartition;
        this.isMutable = isMutable;
    }

    public Long getDbId() {
        return dbId;
    }

    public Long getTableId() {
        return tableId;
    }

    public Partition getPartition() {
        return partition;
    }

    public Range<PartitionKey> getRange() {
        return range;
    }

    public PartitionItem getListPartitionItem() {
        return listPartitionItem;
    }

    public DataProperty getDataProperty() {
        return dataProperty;
    }

    public ReplicaAllocation getReplicaAlloc() {
        return replicaAlloc;
    }

    public boolean isInMemory() {
        return isInMemory;
    }

    public boolean isMutable() {
        return isMutable;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static PartitionPersistInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_136) {
            return GsonUtils.GSON.fromJson(Text.readString(in), PartitionPersistInfo.class);
        } else {
            PartitionPersistInfo info = new PartitionPersistInfo();
            info.readFields(in);
            return info;
        }
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        partition = Partition.read(in);

        range = RangeUtils.readRange(in);
        listPartitionItem = ListPartitionItem.read(in);
        dataProperty = DataProperty.read(in);
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_105) {
            this.replicaAlloc = new ReplicaAllocation(in.readShort());
        } else {
            this.replicaAlloc = ReplicaAllocation.read(in);
        }

        isInMemory = in.readBoolean();
        isTempPartition = in.readBoolean();
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_115) {
            isMutable = in.readBoolean();
        }
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PartitionPersistInfo)) {
            return false;
        }

        PartitionPersistInfo info = (PartitionPersistInfo) obj;

        return dbId.equals(info.dbId)
                   && tableId.equals(info.tableId)
                   && partition.equals(info.partition);
    }
}

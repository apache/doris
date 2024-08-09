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

package org.apache.doris.binlog;

import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.persist.PartitionPersistInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;

public class AddPartitionRecord {
    @SerializedName(value = "commitSeq")
    private long commitSeq;
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
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
    @SerializedName(value = "sql")
    private String sql;

    public AddPartitionRecord(long commitSeq, PartitionPersistInfo partitionPersistInfo) {
        this.commitSeq = commitSeq;
        this.dbId = partitionPersistInfo.getDbId();
        this.tableId = partitionPersistInfo.getTableId();
        this.partition = partitionPersistInfo.getPartition();
        this.range = partitionPersistInfo.getRange();
        this.listPartitionItem = partitionPersistInfo.getListPartitionItem();
        this.dataProperty = partitionPersistInfo.getDataProperty();
        this.replicaAlloc = partitionPersistInfo.getReplicaAlloc();
        this.isInMemory = partitionPersistInfo.isInMemory();
        this.isTempPartition = partitionPersistInfo.isTempPartition();
        this.isMutable = partitionPersistInfo.isMutable();

        StringBuilder sb = new StringBuilder();
        sb.append("ADD ");
        if (isTempPartition) {
            sb.append("TEMPORARY ");
        }
        sb.append("PARTITION `");
        sb.append(partition.getName());
        sb.append("` ");

        // See fe/fe-core/src/main/java/org/apache/doris/datasource/InternalCatalog.java:addPartition for details.
        if (!this.range.equals(RangePartitionItem.DUMMY_RANGE)) {
            // range
            sb.append("VALUES [");
            sb.append(range.lowerEndpoint().toSql());
            sb.append(", ");
            sb.append(range.upperEndpoint().toSql());
            sb.append(") (\"version_info\" = \"");
            sb.append(partition.getVisibleVersion());
            sb.append("\");");
        } else if (!this.listPartitionItem.equals(ListPartitionItem.DUMMY_ITEM)) {
            // list
            sb.append("VALUES IN ");
            sb.append(((ListPartitionItem) listPartitionItem).toSql());
            sb.append(" (\"version_info\" = \"");
            sb.append(partition.getVisibleVersion());
            sb.append("\");");
        } else {
            // unpartitioned.
        }
        this.sql = sb.toString();
    }

    public long getCommitSeq() {
        return commitSeq;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }
}

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

import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.transaction.PartitionCommitInfo;
import org.apache.doris.transaction.TableCommitInfo;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UpsertRecord {
    public static class TableRecord {
        public static class PartitionRecord {
            @SerializedName(value = "partitionId")
            public long partitionId;

            @SerializedName(value = "range")
            private String range;

            @SerializedName(value = "version")
            public long version;

            @SerializedName(value = "isTempPartition")
            public boolean isTemp;

            @SerializedName(value = "stid")
            public long subTxnId;
        }

        @SerializedName(value = "partitionRecords")
        private List<PartitionRecord> partitionRecords;

        @SerializedName(value = "indexIds")
        private Set<Long> indexIds;

        public TableRecord(Set<Long> indexIds) {
            partitionRecords = Lists.newArrayList();
            this.indexIds = indexIds;
        }

        private void addPartitionRecord(PartitionCommitInfo partitionCommitInfo) {
            addPartitionRecord(-1, partitionCommitInfo);
        }

        private void addPartitionRecord(long subTxnId, PartitionCommitInfo partitionCommitInfo) {
            PartitionRecord partitionRecord = new PartitionRecord();
            partitionRecord.subTxnId = subTxnId;
            partitionRecord.partitionId = partitionCommitInfo.getPartitionId();
            partitionRecord.range = partitionCommitInfo.getPartitionRange();
            partitionRecord.version = partitionCommitInfo.getVersion();
            partitionRecord.isTemp = partitionCommitInfo.isTempPartition();
            partitionRecords.add(partitionRecord);
        }

        public List<PartitionRecord> getPartitionRecords() {
            return partitionRecords;
        }
    }

    @SerializedName(value = "commitSeq")
    private long commitSeq;
    // record the transaction state
    // (label, db, table, [shard_id, partition_id, index_id, version, version_hash])
    @SerializedName(value = "txnId")
    private long txnId;
    @SerializedName(value = "timeStamp")
    private long timeStamp;
    @SerializedName(value = "label")
    private String label;
    @SerializedName(value = "dbId")
    private long dbId;
    // pair is (tableId, tableRecord)
    @SerializedName(value = "tableRecords")
    private Map<Long, TableRecord> tableRecords;
    @SerializedName(value = "stids")
    private List<Long> subTxnIds;

    // construct from TransactionState
    public UpsertRecord(long commitSeq, TransactionState state) {
        this.commitSeq = commitSeq;
        txnId = state.getTransactionId();
        timeStamp = state.getFinishTime();
        label = state.getLabel();
        dbId = state.getDbId();
        tableRecords = Maps.newHashMap();

        Map<Long, Set<Long>> loadedTableIndexIds = state.getLoadedTblIndexes();
        if (state.getSubTxnIds() != null) {
            state.getSubTxnIdToTableCommitInfo().forEach((subTxnId, tableCommitInfo) -> {
                Set<Long> indexIds = loadedTableIndexIds.get(tableCommitInfo.getTableId());
                TableRecord tableRecord = tableRecords.compute(tableCommitInfo.getTableId(),
                        (k, v) -> v == null ? new TableRecord(indexIds) : v);
                for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                    tableRecord.addPartitionRecord(subTxnId, partitionCommitInfo);
                }
            });
            subTxnIds = state.getSubTxnIds();
        } else {
            for (TableCommitInfo info : state.getIdToTableCommitInfos().values()) {
                Set<Long> indexIds = loadedTableIndexIds.get(info.getTableId());
                TableRecord tableRecord = new TableRecord(indexIds);
                tableRecords.put(info.getTableId(), tableRecord);

                for (PartitionCommitInfo partitionCommitInfo : info.getIdToPartitionCommitInfo().values()) {
                    tableRecord.addPartitionRecord(partitionCommitInfo);
                }
            }
        }
    }

    public long getTimestamp() {
        return timeStamp;
    }

    public long getDbId() {
        return dbId;
    }

    public long getCommitSeq() {
        return commitSeq;
    }

    public List<Long> getAllReleatedTableIds() {
        return new ArrayList<>(tableRecords.keySet());
    }

    public Map<Long, TableRecord> getTableRecords() {
        return tableRecords;
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    public static UpsertRecord fromJson(String json) {
        return GsonUtils.GSON.fromJson(json, UpsertRecord.class);
    }

    @Override
    public String toString() {
        return toJson();
    }
}

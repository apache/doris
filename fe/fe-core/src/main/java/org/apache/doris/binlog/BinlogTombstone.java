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

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Collections;
import java.util.Map;

public class BinlogTombstone {
    @SerializedName(value = "dbBinlogTombstone")
    private boolean dbBinlogTombstone;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "commitSeq")
    private long commitSeq;

    @SerializedName(value = "tableCommitSeqMap")
    private Map<Long, Long> tableCommitSeqMap;

    @SerializedName(value = "tableVersionMap")
    // this map keep last upsert record <tableId, UpsertRecord>
    // only for master fe to send be gc task, not need persist
    private Map<Long, UpsertRecord.TableRecord> tableVersionMap = Maps.newHashMap();

    public BinlogTombstone(long dbId, boolean isDbTombstone) {
        this.dbBinlogTombstone = isDbTombstone;
        this.dbId = dbId;
        this.commitSeq = -1;
        this.tableCommitSeqMap = Maps.newHashMap();
    }

    public BinlogTombstone(long tableId, long commitSeq) {
        this.dbBinlogTombstone = false;
        this.dbId = -1;
        this.commitSeq = commitSeq;
        this.tableCommitSeqMap = Collections.singletonMap(tableId, commitSeq);
    }

    public void addTableRecord(long tableId, UpsertRecord upsertRecord) {
        Map<Long, UpsertRecord.TableRecord> tableRecords = upsertRecord.getTableRecords();
        UpsertRecord.TableRecord tableRecord = tableRecords.get(tableId);
        tableVersionMap.put(tableId, tableRecord);
    }

    public void addTableRecord(Map<Long, UpsertRecord.TableRecord> records) {
        tableVersionMap.putAll(records);
    }

    // Can only be used to merge tombstone of the same db
    public void mergeTableTombstone(BinlogTombstone tombstone) {
        if (commitSeq < tombstone.getCommitSeq()) {
            commitSeq = tombstone.getCommitSeq();
        }
        tableCommitSeqMap.putAll(tombstone.getTableCommitSeqMap());
    }

    public boolean isDbBinlogTomstone() {
        return dbBinlogTombstone;
    }

    public long getDbId() {
        return dbId;
    }

    public Map<Long, Long> getTableCommitSeqMap() {
        if (tableCommitSeqMap == null) {
            tableCommitSeqMap = Maps.newHashMap();
        }
        return tableCommitSeqMap;
    }

    public long getCommitSeq() {
        return commitSeq;
    }

    public void setCommitSeq(long seq) {
        commitSeq = seq;
    }

    public Map<Long, UpsertRecord.TableRecord> getTableVersionMap() {
        return tableVersionMap;
    }

    // only call when log editlog
    public void clearTableVersionMap() {
        tableVersionMap.clear();
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }
}

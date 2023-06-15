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

import java.util.List;
import java.util.Map;

public class BinlogTombstone {
    @SerializedName(value = "dbBinlogTombstone")
    private boolean dbBinlogTombstone;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "commitSeq")
    private long commitSeq;

    @SerializedName(value = "tableIds")
    private List<Long> tableIds;

    @SerializedName(value = "tableVersionMap")
    // this map keep last upsert record <tableId, UpsertRecord>
    // only for master fe to send be gc task, not need persist
    private Map<Long, UpsertRecord.TableRecord> tableVersionMap = Maps.newHashMap();

    public BinlogTombstone(long dbId, List<Long> tableIds, long commitSeq) {
        this.dbBinlogTombstone = true;
        this.dbId = dbId;
        this.tableIds = tableIds;
        this.commitSeq = commitSeq;
    }

    public BinlogTombstone(long dbId, long commitSeq) {
        this.dbBinlogTombstone = false;
        this.dbId = dbId;
        this.tableIds = null;
        this.commitSeq = commitSeq;
    }

    public void addTableRecord(long tableId, UpsertRecord upsertRecord) {
        Map<Long, UpsertRecord.TableRecord> tableRecords = upsertRecord.getTableRecords();
        UpsertRecord.TableRecord tableRecord = tableRecords.get(tableId);
        tableVersionMap.put(tableId, tableRecord);
    }

    public void addTableRecord(long tableId, UpsertRecord.TableRecord record) {
        tableVersionMap.put(tableId, record);
    }

    public boolean isDbBinlogTomstone() {
        return dbBinlogTombstone;
    }

    public long getDbId() {
        return dbId;
    }

    public List<Long> getTableIds() {
        return tableIds;
    }

    public long getCommitSeq() {
        return commitSeq;
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

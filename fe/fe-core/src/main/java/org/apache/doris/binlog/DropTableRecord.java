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

import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

public class DropTableRecord {
    @SerializedName(value = "commitSeq")
    private long commitSeq;
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "tableName")
    private String tableName;
    @SerializedName(value = "rawSql")
    private String rawSql;

    public DropTableRecord(long commitSeq, DropInfo info) {
        this.commitSeq = commitSeq;
        this.dbId = info.getDbId();
        this.tableId = info.getTableId();
        this.tableName = info.getTableName();
        this.rawSql = String.format("DROP TABLE IF EXISTS `%s`", this.tableName);
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

    public static DropTableRecord fromJson(String json) {
        return GsonUtils.GSON.fromJson(json, DropTableRecord.class);
    }

    @Override
    public String toString() {
        return toJson();
    }
}

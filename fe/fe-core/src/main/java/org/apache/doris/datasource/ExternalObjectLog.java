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

package org.apache.doris.datasource;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.Getter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

@Getter
@Data
public class ExternalObjectLog implements Writable {
    @SerializedName(value = "catalogId")
    private long catalogId;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "dbName")
    private String dbName;

    @SerializedName(value = "tableId")
    private long tableId;

    @SerializedName(value = "tableName")
    private String tableName;

    @SerializedName(value = "ntn")
    private String newTableName; // for rename table op

    @SerializedName(value = "invalidCache")
    private boolean invalidCache;

    @SerializedName(value = "partitionNames")
    private List<String> partitionNames;

    @SerializedName(value = "lastUpdateTime")
    private long lastUpdateTime;

    private ExternalObjectLog() {

    }

    public static ExternalObjectLog createForRefreshDb(long catalogId, String dbName) {
        ExternalObjectLog externalObjectLog = new ExternalObjectLog();
        externalObjectLog.setCatalogId(catalogId);
        externalObjectLog.setDbName(dbName);
        return externalObjectLog;
    }

    public static ExternalObjectLog createForRefreshTable(long catalogId, String dbName, String tblName) {
        ExternalObjectLog externalObjectLog = new ExternalObjectLog();
        externalObjectLog.setCatalogId(catalogId);
        externalObjectLog.setDbName(dbName);
        externalObjectLog.setTableName(tblName);
        return externalObjectLog;
    }

    public static ExternalObjectLog createForRenameTable(long catalogId, String dbName, String tblName,
            String newTblName) {
        ExternalObjectLog externalObjectLog = new ExternalObjectLog();
        externalObjectLog.setCatalogId(catalogId);
        externalObjectLog.setDbName(dbName);
        externalObjectLog.setTableName(tblName);
        externalObjectLog.setNewTableName(newTblName);
        return externalObjectLog;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static ExternalObjectLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ExternalObjectLog.class);
    }

    public String debugForRefreshDb() {
        StringBuilder sb = new StringBuilder();
        sb.append("[catalogId: " + catalogId + ", ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("dbName: " + dbName + "]");
        } else {
            sb.append("dbId: " + dbId + "]");
        }
        return sb.toString();
    }

    public String debugForRefreshTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("[catalogId: " + catalogId + ", ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("dbName: " + dbName + ", ");
        } else {
            sb.append("dbId: " + dbId + ", ");
        }
        if (!Strings.isNullOrEmpty(tableName)) {
            sb.append("tableName: " + tableName + "]");
        } else {
            sb.append("tableId: " + tableId + "]");
        }
        return sb.toString();
    }
}

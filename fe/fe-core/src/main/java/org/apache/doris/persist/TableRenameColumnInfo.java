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
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * PersistInfo for Table rename column info
 */
public class TableRenameColumnInfo implements Writable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "colName")
    private String colName;
    @SerializedName(value = "newColName")
    private String newColName;
    @SerializedName(value = "indexIdToSchemaVersion")
    private Map<Long, Integer> indexIdToSchemaVersion;

    public TableRenameColumnInfo(long dbId, long tableId,
            String colName, String newColName, Map<Long, Integer> indexIdToSchemaVersion) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.colName = colName;
        this.newColName = newColName;
        this.indexIdToSchemaVersion = indexIdToSchemaVersion;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getColName() {
        return colName;
    }

    public String getNewColName() {
        return newColName;
    }

    public Map<Long, Integer> getIndexIdToSchemaVersion() {
        return indexIdToSchemaVersion;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TableRenameColumnInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), TableRenameColumnInfo.class);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof TableRenameColumnInfo)) {
            return false;
        }

        TableRenameColumnInfo info = (TableRenameColumnInfo) obj;

        return (dbId == info.dbId && tableId == info.tableId
                && colName.equals(info.colName) && newColName.equals(info.newColName)
                && indexIdToSchemaVersion.equals(info.indexIdToSchemaVersion));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" dbId: ").append(dbId);
        sb.append(" tableId: ").append(tableId);
        sb.append(" colName: ").append(colName);
        sb.append(" newColName: ").append(newColName);
        sb.append(" indexIdToSchemaVersion: ").append(indexIdToSchemaVersion.toString());
        return sb.toString();
    }
}

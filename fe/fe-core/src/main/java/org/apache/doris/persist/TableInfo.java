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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableInfo implements Writable {

    @SerializedName("db")
    private long dbId;
    @SerializedName("tb")
    private long tableId;
    @SerializedName("ind")
    private long indexId;
    @SerializedName("p")
    private long partitionId;

    @SerializedName("nT")
    private String newTableName;
    @SerializedName("oT")
    private String oldTableName;
    @SerializedName("nR")
    private String newRollupName;
    @SerializedName("oR")
    private String oldRollupName;
    @SerializedName("nP")
    private String newPartitionName;
    @SerializedName("oP")
    private String oldPartitionName;

    public TableInfo() {
        // for persist
    }

    private TableInfo(long dbId, long tableId, long indexId, long partitionId,
                      String newTableName, String newRollupName, String newPartitionName) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.indexId = indexId;
        this.partitionId = partitionId;

        this.newTableName = newTableName;
        this.newRollupName = newRollupName;
        this.newPartitionName = newPartitionName;
    }

    private TableInfo(long dbId, long tableId, long indexId, long partitionId,
                      String newTableName, String oldTableName, String newRollupName, String oldRollupName,
                      String newPartitionName, String oldPartitionName) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.indexId = indexId;
        this.partitionId = partitionId;

        this.newTableName = newTableName;
        this.oldTableName = oldTableName;
        this.newRollupName = newRollupName;
        this.oldRollupName = oldRollupName;
        this.newPartitionName = newPartitionName;
        this.oldPartitionName = oldPartitionName;
    }

    public static TableInfo createForTableRename(long dbId, long tableId, String newTableName) {
        return new TableInfo(dbId, tableId, -1L, -1L, newTableName, "", "");
    }

    public static TableInfo createForTableRename(long dbId, long tableId, String oldTableName, String newTableName) {
        return new TableInfo(dbId, tableId, -1L, -1L, newTableName, oldTableName, "", "", "", "");
    }

    public static TableInfo createForRollupRename(long dbId, long tableId, long indexId, String oldRollupName,
                                                  String newRollupName) {
        return new TableInfo(dbId, tableId, indexId, -1L, "", "", newRollupName, oldRollupName, "", "");
    }

    public static TableInfo createForPartitionRename(long dbId, long tableId, long partitionId,
                                                     String oldPartitionName, String newPartitionName) {
        return new TableInfo(dbId, tableId, -1L, partitionId, "", "", "", "", newPartitionName, oldPartitionName);
    }

    public static TableInfo createForModifyDistribution(long dbId, long tableId) {
        return new TableInfo(dbId, tableId, -1L, -1, "", "", "");
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public String getNewTableName() {
        return newTableName;
    }

    public String getNewRollupName() {
        return newRollupName;
    }

    public String getNewPartitionName() {
        return newPartitionName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TableInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_134) {
            TableInfo tableInfo = new TableInfo();
            tableInfo.readFields(in);
            return tableInfo;
        } else {
            return GsonUtils.GSON.fromJson(Text.readString(in), TableInfo.class);
        }
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        indexId = in.readLong();
        partitionId = in.readLong();

        newTableName = Text.readString(in);
        newRollupName = Text.readString(in);
        newPartitionName = Text.readString(in);
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }
}

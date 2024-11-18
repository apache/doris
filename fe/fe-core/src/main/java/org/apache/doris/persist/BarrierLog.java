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
import org.apache.doris.thrift.TBinlogType;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BarrierLog implements Writable {
    @SerializedName(value = "dbId")
    long dbId = 0L;
    @SerializedName(value = "dbName")
    String dbName;
    @SerializedName(value = "tableId")
    long tableId = 0L;
    @SerializedName(value = "tableName")
    String tableName;

    @SerializedName(value = "binlogType")
    int binlogType;
    @SerializedName(value = "binlog")
    String binlog;

    public BarrierLog() {
    }

    public BarrierLog(long dbId, String dbName) {
        this.dbId = dbId;
        this.dbName = dbName;
    }

    public BarrierLog(long dbId, String dbName, long tableId, String tableName) {
        this.dbId = dbId;
        this.dbName = dbName;
        this.tableId = tableId;
        this.tableName = tableName;
    }

    // A trick: Wrap the binlog as part of the BarrierLog so that it can work in
    // the old Doris version without breaking the compatibility.
    public BarrierLog(long dbId, long tableId, TBinlogType binlogType, String binlog) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.binlogType = binlogType.getValue();
        this.binlog = binlog;
    }

    public boolean hasBinlog() {
        return binlog != null;
    }

    public String getBinlog() {
        return binlog;
    }

    // null is returned if binlog is not set or binlogType is not recognized.
    public TBinlogType getBinlogType() {
        return binlog == null ? null : TBinlogType.findByValue(binlogType);
    }

    public long getDbId() {
        return dbId;
    }

    public String getDbName() {
        return dbName;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, "");
    }

    public static BarrierLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), BarrierLog.class);
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }
}

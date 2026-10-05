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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DropInfo implements Writable {
    @SerializedName(value = "ctl")
    private String ctl;
    @SerializedName(value = "db")
    private String db;
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "tableName")
    private String tableName; // not used in equals and hashCode
    @SerializedName(value = "indexId")
    private long indexId;
    @SerializedName(value = "indexName")
    private String indexName; // not used in equals and hashCode
    @SerializedName(value = "isView")
    private boolean isView = false;
    @SerializedName(value = "forceDrop")
    private boolean forceDrop = false;
    @SerializedName(value = "recycleTime")
    private long recycleTime = 0;

    public DropInfo() {
    }

    // for external table
    public DropInfo(String ctl, String db, String tbl) {
        this.ctl = ctl;
        this.db = db;
        this.tableName = tbl;
    }

    // for internal table
    public DropInfo(long dbId, long tableId, String tableName, boolean isView, boolean forceDrop,
            long recycleTime) {
        this(dbId, tableId, tableName, -1L, "", isView, forceDrop, recycleTime);
    }

    // for internal table
    public DropInfo(long dbId, long tableId, String tableName, long indexId, String indexName, boolean isView,
            boolean forceDrop, long recycleTime) {
        this.ctl = InternalCatalog.INTERNAL_CATALOG_NAME;
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.indexId = indexId;
        this.indexName = indexName;
        this.isView = isView;
        this.forceDrop = forceDrop;
        this.recycleTime = recycleTime;
    }

    public String getCtl() {
        return ctl;
    }

    public String getDb() {
        return db;
    }

    public long getDbId() {
        return this.dbId;
    }

    public long getTableId() {
        return this.tableId;
    }

    public String getTableName() {
        return this.tableName;
    }

    public long getIndexId() {
        return this.indexId;
    }

    public String getIndexName() {
        return this.indexName;
    }

    public boolean isView() {
        return this.isView;
    }

    public boolean isForceDrop() {
        return this.forceDrop;
    }

    public Long getRecycleTime() {
        return this.recycleTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static DropInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), DropInfo.class);
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof DropInfo)) {
            return false;
        }

        DropInfo info = (DropInfo) obj;

        return (dbId == info.dbId) && (tableId == info.tableId) && (indexId == info.indexId)
                && (isView == info.isView) && (forceDrop == info.forceDrop) && (recycleTime == info.recycleTime);
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    public static DropInfo fromJson(String json) {
        return GsonUtils.GSON.fromJson(json, DropInfo.class);
    }

    @Override
    public String toString() {
        // In previous versions, ctl and db are not set, so they may be null.
        return String.format("%s.%s.%s",
                Strings.isNullOrEmpty(ctl) ? InternalCatalog.INTERNAL_CATALOG_NAME : ctl,
                Strings.isNullOrEmpty(db) ? dbId : db,
                tableName);
    }
}

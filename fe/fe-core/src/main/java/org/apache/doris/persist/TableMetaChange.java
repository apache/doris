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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Persist payload for {@link OperationType#OP_TABLE_META_CHANGE}.
 * Generic "an operation modified this table's metadata" signal that follower
 * FEs use to invalidate any local FE caches keyed by the table (sql cache,
 * sorted partition cache, and future per-table caches). This is about
 * metadata mutations (schema/properties/partitions/etc.), not data writes.
 * Carries both ids and names of catalog / database / table so each subscriber
 * can match by id (preferred) or by full name (fallback, e.g. when the table
 * has been concurrently dropped/recreated and the id no longer matches but
 * the name still does). Also carries the master-side timestamp so subscribers
 * and audit tooling can correlate the event with the originating DDL.
 */
public class TableMetaChange implements Writable {
    @SerializedName("ci")
    private long catalogId;
    @SerializedName("cn")
    private String catalogName;
    @SerializedName("di")
    private long dbId;
    @SerializedName("dn")
    private String dbName;
    @SerializedName("ti")
    private long tableId;
    @SerializedName("tn")
    private String tableName;
    // master-side millis-since-epoch when this event was emitted
    @SerializedName("ts")
    private long eventTimeMs;

    public TableMetaChange() {
        // for persist
    }

    /** Build a TableMetaChange from a TableIf (master-side helper). */
    public static TableMetaChange fromTable(TableIf table) {
        long catalogId = -1L;
        String catalogName = "";
        long dbId = -1L;
        String dbName = "";
        DatabaseIf<?> db = table.getDatabase();
        if (db != null) {
            dbId = db.getId();
            dbName = db.getFullName();
            CatalogIf<?> catalog = db.getCatalog();
            if (catalog != null) {
                catalogId = catalog.getId();
                catalogName = catalog.getName();
            }
        }
        return new TableMetaChange(catalogId, catalogName, dbId, dbName,
                table.getId(), table.getName());
    }

    public TableMetaChange(long catalogId, String catalogName,
                              long dbId, String dbName,
                              long tableId, String tableName) {
        this(catalogId, catalogName, dbId, dbName, tableId, tableName, System.currentTimeMillis());
    }

    public TableMetaChange(long catalogId, String catalogName,
                              long dbId, String dbName,
                              long tableId, String tableName,
                              long eventTimeMs) {
        this.catalogId = catalogId;
        this.catalogName = catalogName;
        this.dbId = dbId;
        this.dbName = dbName;
        this.tableId = tableId;
        this.tableName = tableName;
        this.eventTimeMs = eventTimeMs;
    }

    public long getCatalogId() {
        return catalogId;
    }

    public String getCatalogName() {
        return catalogName;
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

    public long getEventTimeMs() {
        return eventTimeMs;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TableMetaChange read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), TableMetaChange.class);
    }

    @Override
    public String toString() {
        return "TableMetaChange{catalogId=" + catalogId
                + ", catalogName='" + catalogName + '\''
                + ", dbId=" + dbId
                + ", dbName='" + dbName + '\''
                + ", tableId=" + tableId
                + ", tableName='" + tableName + '\''
                + ", eventTimeMs=" + eventTimeMs
                + '}';
    }
}

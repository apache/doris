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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class CreateTableRecord {
    private static final Logger LOG = LogManager.getLogger(CreateTableRecord.class);

    @SerializedName(value = "commitSeq")
    private long commitSeq;
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "tableName")
    private String tableName;
    @SerializedName(value = "tableType")
    protected TableType type;
    @SerializedName(value = "sql")
    private String sql;

    public CreateTableRecord(long commitSeq, CreateTableInfo info) {
        this.commitSeq = commitSeq;

        Table table = info.getTable();
        this.tableName = table.getName();

        this.tableId = table.getId();
        String dbName = info.getDbName();
        this.dbName = dbName;

        this.type = table.getType();

        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
        if (db == null) {
            LOG.warn("db not found. dbId: {}", dbId);
            this.dbId = -1L;
        } else {
            this.dbId = db.getId();
        }

        List<String> createTableStmt = Lists.newArrayList();
        List<String> addPartitionStmt = Lists.newArrayList();
        List<String> createRollupStmt = Lists.newArrayList();

        table.readLock();
        try {
            Env.getSyncedDdlStmt(table, createTableStmt, addPartitionStmt, createRollupStmt,
                    false, false /* show password */, -1L);
        } finally {
            table.readUnlock();
        }
        if (createTableStmt.size() > 0) {
            this.sql = createTableStmt.get(0);
        } else {
            this.sql = "";
        }
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

    public String getSql() {
        return sql;
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }
}

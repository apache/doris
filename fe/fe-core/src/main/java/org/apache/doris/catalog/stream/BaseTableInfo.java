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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

public class BaseTableInfo {
    private static final Logger LOG = LogManager.getLogger(BaseTableInfo.class);
    // for internal table we use id as identifier otherwise use name instead
    @SerializedName("ci")
    private final long ctlId;
    @SerializedName("di")
    private long dbId;
    @SerializedName("ti")
    private long tableId;
    @SerializedName("tn")
    private String tableName;
    @SerializedName("dn")
    private String dbName;
    @SerializedName("cn")
    private String ctlName;

    public BaseTableInfo(TableIf table) {
        java.util.Objects.requireNonNull(table, "table is null");
        DatabaseIf database = table.getDatabase();
        java.util.Objects.requireNonNull(database, "database is null");
        CatalogIf catalog = database.getCatalog();
        java.util.Objects.requireNonNull(database, "catalog is null");
        this.tableId = table.getId();
        this.dbId = database.getId();
        this.ctlId = catalog.getId();
        this.tableName = table.getName();
        this.dbName = database.getFullName();
        this.ctlName = catalog.getName();
    }

    public String getTableName() {
        return tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getCtlName() {
        return ctlName;
    }

    public long getTableId() {
        return tableId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getCtlId() {
        return ctlId;
    }

    public boolean isInternalTable() {
        return InternalCatalog.INTERNAL_CATALOG_ID == ctlId;
    }

    public TableIf getTableNullable() {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(ctlId);
        if (catalog != null) {
            if (isInternalTable()) {
                Optional<DatabaseIf> db = catalog.getDb(dbId);
                if (db.isPresent()) {
                    Optional<TableIf> table = db.get().getTable(tableId);
                    if (table.isPresent()) {
                        return table.get();
                    }
                }
            } else {
                Optional<DatabaseIf> db = catalog.getDb(dbName);
                if (db.isPresent()) {
                    Optional<TableIf> table = db.get().getTable(tableName);
                    if (table.isPresent()) {
                        return table.get();
                    }
                }
            }
        }
        LOG.warn("invalid base table: {}", this);
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BaseTableInfo that = (BaseTableInfo) o;
        if (isInternalTable()) {
            return Objects.equal(tableId, that.tableId) && Objects.equal(
                    dbId, that.dbId) && Objects.equal(ctlId, that.ctlId);
        } else {
            return Objects.equal(tableName, that.tableName) && Objects.equal(
                    dbName, that.dbName) && Objects.equal(ctlId, that.ctlId);
        }
    }

    @Override
    public String toString() {
        return "BaseTableInfo{"
                + "tableName='" + tableName + '\''
                + ", dbName='" + dbName + '\''
                + ", ctlName='" + ctlName + '\''
                + '}';
    }
}

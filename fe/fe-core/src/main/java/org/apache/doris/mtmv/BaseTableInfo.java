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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class BaseTableInfo {
    private static final Logger LOG = LogManager.getLogger(BaseTableInfo.class);

    // The MTMV needs to record the name to avoid changing the ID after rebuilding the same named base table,
    // which may make the materialized view unusable.
    // The previous version stored the ID, so it is temporarily kept for compatibility with the old version
    @SerializedName("ti")
    @Deprecated
    private long tableId;
    @SerializedName("di")
    @Deprecated
    private long dbId;
    @SerializedName("ci")
    @Deprecated
    private long ctlId;

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

    // for replay MTMV, can not use  `table.getDatabase();`,because database not added to catalog
    public BaseTableInfo(OlapTable table, long dbId) {
        java.util.Objects.requireNonNull(table, "table is null");
        this.tableId = table.getId();
        this.dbId = dbId;
        this.ctlId = InternalCatalog.INTERNAL_CATALOG_ID;
        this.tableName = table.getName();
        this.dbName = table.getQualifiedDbName();
        this.ctlName = InternalCatalog.INTERNAL_CATALOG_NAME;
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

    @Deprecated
    public long getTableId() {
        return tableId;
    }

    @Deprecated
    public long getDbId() {
        return dbId;
    }

    @Deprecated
    public long getCtlId() {
        return ctlId;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    // if compatible failed due catalog dropped, ctlName will be null
    public boolean isInternalTable() {
        if (!StringUtils.isEmpty(ctlName)) {
            return InternalCatalog.INTERNAL_CATALOG_NAME.equals(ctlName);
        } else {
            return InternalCatalog.INTERNAL_CATALOG_ID == ctlId;
        }
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
        // for compatibility
        if (StringUtils.isEmpty(ctlName) || StringUtils.isEmpty(that.ctlName)) {
            return Objects.equal(tableId, that.tableId) && Objects.equal(
                    dbId, that.dbId) && Objects.equal(ctlId, that.ctlId);
        } else {
            return Objects.equal(tableName, that.tableName) && Objects.equal(
                    dbName, that.dbName) && Objects.equal(ctlName, that.ctlName);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableName, dbName, ctlName);
    }

    @Override
    public String toString() {
        return "BaseTableInfo{"
                + "tableName='" + tableName + '\''
                + ", dbName='" + dbName + '\''
                + ", ctlName='" + ctlName + '\''
                + '}';
    }

    public void compatible(CatalogMgr catalogMgr) {
        if (!StringUtils.isEmpty(ctlName)) {
            return;
        }
        try {
            CatalogIf catalog = catalogMgr.getCatalogOrAnalysisException(ctlId);
            DatabaseIf db = catalog.getDbOrAnalysisException(dbId);
            TableIf table = db.getTableOrAnalysisException(tableId);
            this.ctlName = catalog.getName();
            this.dbName = db.getFullName();
            this.tableName = table.getName();
        } catch (AnalysisException e) {
            LOG.warn("MTMV compatible failed, ctlId: {}, dbId: {}, tableId: {}", ctlId, dbId, tableId, e);
        }
    }

    public List<String> toList() {
        return Lists.newArrayList(getCtlName(), getDbName(), getTableName());
    }
}

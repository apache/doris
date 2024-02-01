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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BaseTableInfo {
    private static final Logger LOG = LogManager.getLogger(BaseTableInfo.class);

    @SerializedName("ti")
    private Long tableId;
    @SerializedName("di")
    private Long dbId;
    @SerializedName("ci")
    private Long ctlId;

    public BaseTableInfo(Long tableId, Long dbId) {
        this.tableId = java.util.Objects.requireNonNull(tableId, "tableId is null");
        this.dbId = java.util.Objects.requireNonNull(dbId, "dbId is null");
        this.ctlId = InternalCatalog.INTERNAL_CATALOG_ID;
    }

    public BaseTableInfo(TableIf table) {
        DatabaseIf database = table.getDatabase();
        java.util.Objects.requireNonNull(database, "database is null");
        CatalogIf catalog = database.getCatalog();
        java.util.Objects.requireNonNull(database, "catalog is null");
        this.tableId = table.getId();
        this.dbId = database.getId();
        this.ctlId = catalog.getId();
    }

    public Long getTableId() {
        return tableId;
    }

    public Long getDbId() {
        return dbId;
    }

    public Long getCtlId() {
        return ctlId;
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
        return Objects.equal(tableId, that.tableId)
                && Objects.equal(dbId, that.dbId)
                && Objects.equal(ctlId, that.ctlId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableId, dbId, ctlId);
    }

    @Override
    public String toString() {
        return "BaseTableInfo{"
                + "tableId=" + tableId
                + ", dbId=" + dbId
                + ", ctlId=" + ctlId
                + '}';
    }

    public String getTableName() {
        try {
            return MTMVUtil.getTable(this).getName();
        } catch (AnalysisException e) {
            LOG.warn("can not get table: " + this);
            return "";
        }
    }
}

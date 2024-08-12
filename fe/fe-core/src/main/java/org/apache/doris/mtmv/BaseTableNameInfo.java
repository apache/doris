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
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BaseTableNameInfo {
    private static final Logger LOG = LogManager.getLogger(BaseTableNameInfo.class);

    @SerializedName("tn")
    private String tableName;
    @SerializedName("dn")
    private String dbName;
    @SerializedName("cn")
    private String ctlName;

    public BaseTableNameInfo(String tableName, String dbName) {
        this.tableName = java.util.Objects.requireNonNull(tableName, "tableName is null");
        this.dbName = java.util.Objects.requireNonNull(dbName, "dbName is null");
        this.ctlName = InternalCatalog.INTERNAL_CATALOG_NAME;
    }

    public BaseTableNameInfo(String tableName, String dbName, String ctlName) {
        this.tableName = java.util.Objects.requireNonNull(tableName, "tableName is null");
        this.dbName = java.util.Objects.requireNonNull(dbName, "dbName is null");
        this.ctlName = java.util.Objects.requireNonNull(ctlName, "ctlName is null");
    }

    public BaseTableNameInfo(TableIf table) {
        DatabaseIf database = table.getDatabase();
        java.util.Objects.requireNonNull(database, "database is null");
        CatalogIf catalog = database.getCatalog();
        java.util.Objects.requireNonNull(database, "catalog is null");
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BaseTableNameInfo that = (BaseTableNameInfo) o;
        return Objects.equal(tableName, that.tableName) && Objects.equal(
                dbName, that.dbName) && Objects.equal(ctlName, that.ctlName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableName, dbName, ctlName);
    }

    @Override
    public String toString() {
        return "BaseTableNameInfo{"
                + "tableName='" + tableName + '\''
                + ", dbName='" + dbName + '\''
                + ", ctlName='" + ctlName + '\''
                + '}';
    }
}

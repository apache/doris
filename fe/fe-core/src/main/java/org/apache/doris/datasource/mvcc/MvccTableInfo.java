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

package org.apache.doris.datasource.mvcc;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;

import com.google.common.base.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MvccTableInfo {
    private static final Logger LOG = LogManager.getLogger(MvccTableInfo.class);

    private String tableName;
    private String dbName;
    private String ctlName;
    // One statement may bind the same table through aliases with different relation selectors;
    // their snapshots must remain distinct even though the catalog/db/table identity is identical.
    private final String version;

    public MvccTableInfo(TableIf table) {
        this(table, "");
    }

    public MvccTableInfo(TableIf table, String version) {
        java.util.Objects.requireNonNull(table, "table is null");
        DatabaseIf database = table.getDatabase();
        java.util.Objects.requireNonNull(database, "database is null");
        CatalogIf catalog = database.getCatalog();
        java.util.Objects.requireNonNull(database, "catalog is null");
        this.tableName = table.getName();
        this.dbName = database.getFullName();
        this.ctlName = catalog.getName();
        this.version = version == null ? "" : version;
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

    public String getVersion() {
        return version;
    }

    public boolean isSameTable(MvccTableInfo other) {
        return other != null
                && Objects.equal(tableName, other.tableName)
                && Objects.equal(dbName, other.dbName)
                && Objects.equal(ctlName, other.ctlName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MvccTableInfo that = (MvccTableInfo) o;
        return Objects.equal(tableName, that.tableName) && Objects.equal(
                dbName, that.dbName) && Objects.equal(ctlName, that.ctlName)
                && Objects.equal(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableName, dbName, ctlName, version);
    }

    @Override
    public String toString() {
        return "MvccTableInfo{"
                + "tableName='" + tableName + '\''
                + ", dbName='" + dbName + '\''
                + ", ctlName='" + ctlName + '\''
                + ", version='" + version + '\''
                + '}';
    }
}

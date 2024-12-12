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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.CatalogIf;

import java.util.Objects;
import java.util.StringJoiner;

public class PaimonSnapshotCacheKey {
    private final CatalogIf catalog;
    private final String dbName;
    private final String tableName;

    public PaimonSnapshotCacheKey(CatalogIf catalog, String dbName, String tableName) {
        this.catalog = catalog;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public CatalogIf getCatalog() {
        return catalog;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PaimonSnapshotCacheKey that = (PaimonSnapshotCacheKey) o;
        return catalog.getId() == that.catalog.getId()
                && Objects.equals(dbName, that.dbName)
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog.getId(), dbName, tableName);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", PaimonSnapshotCacheKey.class.getSimpleName() + "[", "]")
                .add("catalog=" + catalog)
                .add("dbName='" + dbName + "'")
                .add("tableName='" + tableName + "'")
                .toString();
    }
}

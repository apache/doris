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

package org.apache.doris.info;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.GlobalVariable;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Utility methods for creating {@link org.apache.doris.catalog.info.TableNameInfo} from catalog objects.
 * Kept separate from {@code TableNameInfo} to avoid coupling that class to
 * {@code Database}/{@code TableIf}/{@code CatalogIf}.
 *
 * <p>All factory methods apply case conversion via
 * {@link GlobalVariable#isStoredTableNamesLowerCase()} consistently,
 * matching the behavior of the {@code TableNameInfo} constructors.</p>
 */
public class TableNameInfoUtils {

    private TableNameInfoUtils() {
    }

    /**
     * Apply lower-case conversion to a table name if the global config requires it.
     */
    private static String normalizeTableName(String tableName) {
        if (GlobalVariable.isStoredTableNamesLowerCase() && !StringUtils.isEmpty(tableName)) {
            return tableName.toLowerCase();
        }
        return tableName;
    }

    /**
     * Create a TableNameInfo from a Database and table name.
     * Uses the database's catalog name, falling back to INTERNAL_CATALOG_NAME.
     */
    public static TableNameInfo fromDb(Database db, String tableName) {
        Objects.requireNonNull(db, "db must not be null");
        String catalogName = db.getCatalog() != null ? db.getCatalog().getName()
                : NameSpaceContext.INTERNAL_CATALOG_NAME;
        return new TableNameInfo(catalogName, db.getFullName(), normalizeTableName(tableName));
    }

    /**
     * Create a TableNameInfo from catalog, database, and table name string.
     */
    public static TableNameInfo fromCatalogDb(CatalogIf<?> catalog, DatabaseIf<?> db, String tableName) {
        Objects.requireNonNull(catalog, "catalog must not be null");
        Objects.requireNonNull(db, "db must not be null");
        return new TableNameInfo(catalog.getName(), db.getFullName(), normalizeTableName(tableName));
    }

    /**
     * Create a TableNameInfo from catalog, database, and table objects.
     */
    public static TableNameInfo fromCatalogDb(CatalogIf<?> catalog, DatabaseIf<?> db, TableIf table) {
        Objects.requireNonNull(catalog, "catalog must not be null");
        Objects.requireNonNull(db, "db must not be null");
        Objects.requireNonNull(table, "table must not be null");
        return new TableNameInfo(catalog.getName(), db.getFullName(), normalizeTableName(table.getName()));
    }

    /**
     * Create a TableNameInfo from a TableIf, looking up its database and catalog.
     * Returns null if the table is null, its name is empty, or its database/catalog
     * is not available — matching the old {@code TableNameInfo.createOrNull} contract.
     */
    public static TableNameInfo fromTableOrNull(TableIf table) {
        if (table == null || StringUtils.isEmpty(table.getName())) {
            return null;
        }
        DatabaseIf<?> db = table.getDatabase();
        if (db == null) {
            return null;
        }
        CatalogIf<?> catalog = db.getCatalog();
        if (catalog == null) {
            return null;
        }
        return new TableNameInfo(catalog.getName(), db.getFullName(), normalizeTableName(table.getName()));
    }
}

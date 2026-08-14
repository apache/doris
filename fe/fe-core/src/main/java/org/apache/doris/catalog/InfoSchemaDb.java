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

package org.apache.doris.catalog;

import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.GlobalVariable;

// Information schema used for MySQL compatible.
public class InfoSchemaDb extends MysqlCompatibleDatabase {
    public static final String DATABASE_NAME = "information_schema";
    public static final long DATABASE_ID = 0L;

    /**
     * The schema name a MySQL client sees for a database. With
     * {@code show_full_dbname_in_info_schema_db} on, a database outside the internal
     * catalog is qualified with the name of its catalog, so that databases of the same
     * name in different catalogs stay apart.
     *
     * <p>Every information_schema column that carries a schema name has to go through
     * here. A column that reports the raw name instead is not just inconsistent with the
     * rest of the output: a client's own {@code TABLE_SCHEMA = 'catalog.db'} filter then
     * matches none of its rows.
     */
    public static String getMysqlTableSchema(String catalogName, String dbName) {
        if (!GlobalVariable.showFullDbNameInInfoSchemaDb) {
            return dbName;
        }
        if (InternalCatalog.INTERNAL_CATALOG_NAME.equals(catalogName)) {
            return dbName;
        }
        return catalogName + "." + dbName;
    }

    /** The inverse of {@link #getMysqlTableSchema}, for a name that arrives from a client. */
    public static String getDbNameFromMysqlTableSchema(String catalogName, String schemaName) {
        if (InternalCatalog.INTERNAL_CATALOG_NAME.equals(catalogName)) {
            return schemaName;
        }
        String[] parts = schemaName.split("\\.");
        if (parts.length == 2) {
            return parts[1];
        }
        return schemaName;
    }

    public InfoSchemaDb() {
        super(DATABASE_ID, DATABASE_NAME);
    }

    @Override
    protected void initTables() {
        for (Table table : SchemaTable.TABLE_MAP.values()) {
            super.registerTable(table);
        }
    }

    @Override
    public boolean registerTable(TableIf table) {
        return false;
    }
}

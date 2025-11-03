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

package org.apache.doris.datasource.infoschema;

import org.apache.doris.catalog.MysqlDBTable;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InitDatabaseLog.Type;

import com.google.common.collect.Lists;

import java.util.List;

public class ExternalMysqlDatabase extends ExternalDatabase {
    /**
     * Create external database.
     *
     * @param extCatalog The catalog this database belongs to.
     * @param dbId The id of this database.
     */
    public ExternalMysqlDatabase(ExternalCatalog extCatalog, long dbId) {
        super(extCatalog, dbId, MysqlDb.DATABASE_NAME, MysqlDb.DATABASE_NAME, Type.INFO_SCHEMA_DB);
    }

    public static List<String> listTableNames() {
        return Lists.newArrayList(MysqlDBTable.TABLE_MAP.keySet());
    }

    @Override
    public ExternalTable buildTableInternal(String remoteTableName, String localTableName, long tblId,
            ExternalCatalog catalog,
            ExternalDatabase db) {
        return new ExternalMysqlTable(tblId, localTableName, catalog, db);
    }

    @Override
    public ExternalTable getTableNullable(String name) {
        return super.getTableNullable(name.toLowerCase());
    }
}

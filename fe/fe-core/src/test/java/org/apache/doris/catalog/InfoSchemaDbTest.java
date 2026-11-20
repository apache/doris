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

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.GlobalVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class InfoSchemaDbTest {
    @Test
    public void testNormal() throws IOException, DdlException {
        Database db = new InfoSchemaDb();

        Assertions.assertFalse(db.registerTable(null));
        Assertions.assertFalse(db.createTableWithLock(null, false, false).first);
        db.unregisterTable("authors");
        Assertions.assertThrows(IOException.class, () -> db.write(null));
        Assertions.assertNull(db.getTableNullable("authors"));
    }

    @Test
    public void testMysqlTableSchemaRoundTrip() {
        boolean original = GlobalVariable.showFullDbNameInInfoSchemaDb;
        try {
            for (boolean showFullName : new boolean[] {false, true}) {
                GlobalVariable.showFullDbNameInInfoSchemaDb = showFullName;
                for (String dbName : new String[] {"db", "gold.reporting", "a.b.c", "ctl.db"}) {
                    for (String catalogName : new String[] {InternalCatalog.INTERNAL_CATALOG_NAME, "ctl"}) {
                        String schemaName = InfoSchemaDb.getMysqlTableSchema(catalogName, dbName);
                        Assertions.assertEquals(dbName,
                                InfoSchemaDb.getDbNameFromMysqlTableSchema(catalogName, schemaName),
                                "showFullName=" + showFullName + ", catalog=" + catalogName);
                    }
                }
            }
        } finally {
            GlobalVariable.showFullDbNameInInfoSchemaDb = original;
        }
    }

    @Test
    public void testDbNameFromMysqlTableSchemaKeepsDottedName() {
        boolean original = GlobalVariable.showFullDbNameInInfoSchemaDb;
        try {
            GlobalVariable.showFullDbNameInInfoSchemaDb = false;
            Assertions.assertEquals("gold.reporting",
                    InfoSchemaDb.getDbNameFromMysqlTableSchema("iceberg_ctl", "gold.reporting"));

            GlobalVariable.showFullDbNameInInfoSchemaDb = true;
            Assertions.assertEquals("gold.reporting",
                    InfoSchemaDb.getDbNameFromMysqlTableSchema("iceberg_ctl", "iceberg_ctl.gold.reporting"));
            Assertions.assertEquals("db", InfoSchemaDb.getDbNameFromMysqlTableSchema("ctl", "db"));
        } finally {
            GlobalVariable.showFullDbNameInInfoSchemaDb = original;
        }
    }
}

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
import org.apache.doris.datasource.CatalogIf;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableNameInfoUtilsTest {

    @Test
    public void testFromDbWithCatalog(@Mocked Database db, @Mocked CatalogIf catalog) {
        new Expectations() {
            {
                db.getCatalog();
                result = catalog;
                catalog.getName();
                result = "test_catalog";
                db.getFullName();
                result = "test_db";
            }
        };

        TableNameInfo info = TableNameInfoUtils.fromDb(db, "test_table");
        Assertions.assertEquals("test_catalog", info.getCtl());
        Assertions.assertEquals("test_db", info.getDb());
        Assertions.assertEquals("test_table", info.getTbl());
    }

    @Test
    public void testFromDbWithNullCatalog(@Mocked Database db) {
        new Expectations() {
            {
                db.getCatalog();
                result = null;
                db.getFullName();
                result = "test_db";
            }
        };

        TableNameInfo info = TableNameInfoUtils.fromDb(db, "test_table");
        Assertions.assertEquals(NameSpaceContext.INTERNAL_CATALOG_NAME, info.getCtl());
        Assertions.assertEquals("test_db", info.getDb());
        Assertions.assertEquals("test_table", info.getTbl());
    }

    @Test
    public void testFromCatalogDbWithString(@Mocked CatalogIf catalog, @Mocked DatabaseIf db) {
        new Expectations() {
            {
                catalog.getName();
                result = "my_catalog";
                db.getFullName();
                result = "my_db";
            }
        };

        TableNameInfo info = TableNameInfoUtils.fromCatalogDb(catalog, db, "my_table");
        Assertions.assertEquals("my_catalog", info.getCtl());
        Assertions.assertEquals("my_db", info.getDb());
        Assertions.assertEquals("my_table", info.getTbl());
    }

    @Test
    public void testFromCatalogDbWithTableIf(@Mocked CatalogIf catalog, @Mocked DatabaseIf db,
            @Mocked TableIf table) {
        new Expectations() {
            {
                catalog.getName();
                result = "cat1";
                db.getFullName();
                result = "db1";
                table.getName();
                result = "tbl1";
            }
        };

        TableNameInfo info = TableNameInfoUtils.fromCatalogDb(catalog, db, table);
        Assertions.assertEquals("cat1", info.getCtl());
        Assertions.assertEquals("db1", info.getDb());
        Assertions.assertEquals("tbl1", info.getTbl());
    }

    @Test
    public void testFromTableOrNullWithDbAndCatalog(@Mocked TableIf table, @Mocked DatabaseIf db,
            @Mocked CatalogIf catalog) {
        new Expectations() {
            {
                table.getDatabase();
                result = db;
                db.getCatalog();
                result = catalog;
                catalog.getName();
                result = "ext_catalog";
                db.getFullName();
                result = "ext_db";
                table.getName();
                result = "ext_table";
            }
        };

        TableNameInfo info = TableNameInfoUtils.fromTableOrNull(table);
        Assertions.assertNotNull(info);
        Assertions.assertEquals("ext_catalog", info.getCtl());
        Assertions.assertEquals("ext_db", info.getDb());
        Assertions.assertEquals("ext_table", info.getTbl());
    }

    @Test
    public void testFromTableOrNullWithNullDb(@Mocked TableIf table) {
        new Expectations() {
            {
                table.getName();
                result = "some_table";
                table.getDatabase();
                result = null;
            }
        };

        TableNameInfo info = TableNameInfoUtils.fromTableOrNull(table);
        Assertions.assertNull(info);
    }

    @Test
    public void testFromTableOrNullWithNullCatalog(@Mocked TableIf table, @Mocked DatabaseIf db) {
        new Expectations() {
            {
                table.getName();
                result = "internal_table";
                table.getDatabase();
                result = db;
                db.getCatalog();
                result = null;
            }
        };

        TableNameInfo info = TableNameInfoUtils.fromTableOrNull(table);
        Assertions.assertNull(info);
    }

    @Test
    public void testFromTableOrNullWithNullTable() {
        TableNameInfo info = TableNameInfoUtils.fromTableOrNull(null);
        Assertions.assertNull(info);
    }

    @Test
    public void testFromTableOrNullWithEmptyName(@Mocked TableIf table) {
        new Expectations() {
            {
                table.getName();
                result = "";
            }
        };

        TableNameInfo info = TableNameInfoUtils.fromTableOrNull(table);
        Assertions.assertNull(info);
    }
}

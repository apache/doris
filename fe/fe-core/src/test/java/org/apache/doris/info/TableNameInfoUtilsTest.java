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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TableNameInfoUtilsTest {

    @Test
    public void testFromDbWithCatalog() {
        Database db = Mockito.mock(Database.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);

        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("test_catalog");
        Mockito.when(db.getFullName()).thenReturn("test_db");

        org.apache.doris.catalog.info.TableNameInfo info = TableNameInfoUtils.fromDb(db, "test_table");
        Assertions.assertEquals("test_catalog", info.getCtl());
        Assertions.assertEquals("test_db", info.getDb());
        Assertions.assertEquals("test_table", info.getTbl());
    }

    @Test
    public void testFromDbWithNullCatalog() {
        Database db = Mockito.mock(Database.class);

        Mockito.when(db.getCatalog()).thenReturn(null);
        Mockito.when(db.getFullName()).thenReturn("test_db");

        org.apache.doris.catalog.info.TableNameInfo info = TableNameInfoUtils.fromDb(db, "test_table");
        Assertions.assertEquals(NameSpaceContext.INTERNAL_CATALOG_NAME, info.getCtl());
        Assertions.assertEquals("test_db", info.getDb());
        Assertions.assertEquals("test_table", info.getTbl());
    }

    @Test
    public void testFromCatalogDbWithString() {
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf db = Mockito.mock(DatabaseIf.class);

        Mockito.when(catalog.getName()).thenReturn("my_catalog");
        Mockito.when(db.getFullName()).thenReturn("my_db");

        org.apache.doris.catalog.info.TableNameInfo info = TableNameInfoUtils.fromCatalogDb(catalog, db, "my_table");
        Assertions.assertEquals("my_catalog", info.getCtl());
        Assertions.assertEquals("my_db", info.getDb());
        Assertions.assertEquals("my_table", info.getTbl());
    }

    @Test
    public void testFromCatalogDbWithTableIf() {
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf db = Mockito.mock(DatabaseIf.class);
        TableIf table = Mockito.mock(TableIf.class);

        Mockito.when(catalog.getName()).thenReturn("cat1");
        Mockito.when(db.getFullName()).thenReturn("db1");
        Mockito.when(table.getName()).thenReturn("tbl1");

        org.apache.doris.catalog.info.TableNameInfo info = TableNameInfoUtils.fromCatalogDb(catalog, db, table);
        Assertions.assertEquals("cat1", info.getCtl());
        Assertions.assertEquals("db1", info.getDb());
        Assertions.assertEquals("tbl1", info.getTbl());
    }

    @Test
    public void testFromTableOrNullWithDbAndCatalog() {
        TableIf table = Mockito.mock(TableIf.class);
        DatabaseIf db = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);

        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ext_catalog");
        Mockito.when(db.getFullName()).thenReturn("ext_db");
        Mockito.when(table.getName()).thenReturn("ext_table");

        org.apache.doris.catalog.info.TableNameInfo info = TableNameInfoUtils.fromTableOrNull(table);
        Assertions.assertNotNull(info);
        Assertions.assertEquals("ext_catalog", info.getCtl());
        Assertions.assertEquals("ext_db", info.getDb());
        Assertions.assertEquals("ext_table", info.getTbl());
    }

    @Test
    public void testFromTableOrNullWithNullDb() {
        TableIf table = Mockito.mock(TableIf.class);

        Mockito.when(table.getName()).thenReturn("some_table");
        Mockito.when(table.getDatabase()).thenReturn(null);

        org.apache.doris.catalog.info.TableNameInfo info = TableNameInfoUtils.fromTableOrNull(table);
        Assertions.assertNull(info);
    }

    @Test
    public void testFromTableOrNullWithNullCatalog() {
        TableIf table = Mockito.mock(TableIf.class);
        DatabaseIf db = Mockito.mock(DatabaseIf.class);

        Mockito.when(table.getName()).thenReturn("internal_table");
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(db.getCatalog()).thenReturn(null);

        org.apache.doris.catalog.info.TableNameInfo info = TableNameInfoUtils.fromTableOrNull(table);
        Assertions.assertNull(info);
    }

    @Test
    public void testFromTableOrNullWithNullTable() {
        org.apache.doris.catalog.info.TableNameInfo info = TableNameInfoUtils.fromTableOrNull(null);
        Assertions.assertNull(info);
    }

    @Test
    public void testFromTableOrNullWithEmptyName() {
        TableIf table = Mockito.mock(TableIf.class);

        Mockito.when(table.getName()).thenReturn("");

        org.apache.doris.catalog.info.TableNameInfo info = TableNameInfoUtils.fromTableOrNull(table);
        Assertions.assertNull(info);
    }
}

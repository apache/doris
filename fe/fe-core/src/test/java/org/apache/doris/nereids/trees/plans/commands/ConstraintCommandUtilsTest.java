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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class ConstraintCommandUtilsTest {
    @Test
    void lockCurrentDatabaseReturnsOnlyCurrentDatabase() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogManager = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        TableNameInfo tableNameInfo = new TableNameInfo("internal", "db", "tbl");
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogManager);
        Mockito.when(catalogManager.getCatalogOrDdlException("internal")).thenReturn(catalog);
        Mockito.when(catalogManager.getCatalog("internal")).thenReturn(catalog);
        Mockito.when(catalog.getDbOrDdlException("db")).thenReturn(database);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(database);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            DatabaseIf locked = ConstraintCommandUtils.lockCurrentDatabase(tableNameInfo);

            Assertions.assertSame(database, locked);
            Mockito.verify(database).readLock();
            locked.readUnlock();
        }
    }

    @Test
    void lockCurrentDatabaseRejectsRecreatedDatabase() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogManager = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf resolvedDatabase = Mockito.mock(DatabaseIf.class);
        DatabaseIf recreatedDatabase = Mockito.mock(DatabaseIf.class);
        TableNameInfo tableNameInfo = new TableNameInfo("internal", "db", "tbl");
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogManager);
        Mockito.when(catalogManager.getCatalogOrDdlException("internal")).thenReturn(catalog);
        Mockito.when(catalogManager.getCatalog("internal")).thenReturn(catalog);
        Mockito.when(catalog.getDbOrDdlException("db")).thenReturn(resolvedDatabase);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(recreatedDatabase);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            Assertions.assertThrows(DdlException.class,
                    () -> ConstraintCommandUtils.lockCurrentDatabase(tableNameInfo));

            Mockito.verify(resolvedDatabase).readLock();
            Mockito.verify(resolvedDatabase).readUnlock();
        }
    }
}

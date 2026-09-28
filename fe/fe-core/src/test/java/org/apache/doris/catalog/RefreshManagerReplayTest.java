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

import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.ExternalTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

public class RefreshManagerReplayTest {

    @Test
    public void testReplayRefreshCatalogContainsInvalidationFailure() {
        long catalogId = 1L;
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.isInternalCatalog()).thenReturn(false);
        Mockito.doThrow(new RuntimeException("injected catalog invalidation failure"))
                .when(catalog).onRefreshCache(true);
        Env env = mockEnv(catalogId, catalog);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            CatalogLog log = CatalogLog.createForRefreshCatalog(catalogId, true);
            Assertions.assertDoesNotThrow(() -> new RefreshManager().replayRefreshCatalog(log));
        }
    }

    @Test
    public void testReplayRefreshDbContainsInvalidationFailure() {
        long catalogId = 2L;
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        @SuppressWarnings("unchecked")
        ExternalDatabase<ExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.doThrow(new RuntimeException("injected db invalidation failure"))
                .when(db).resetMetaToUninitialized();
        Mockito.doReturn(Optional.of(db)).when(catalog).getDbForReplay("db");
        Env env = mockEnv(catalogId, catalog);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ExternalObjectLog log = ExternalObjectLog.createForRefreshDb(catalogId, "db");
            Assertions.assertDoesNotThrow(() -> new RefreshManager().replayRefreshDb(log));
        }
    }

    @Test
    public void testReplayRefreshTableContainsInvalidationFailure() {
        long catalogId = 3L;
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        @SuppressWarnings("unchecked")
        ExternalDatabase<ExternalTable> db = Mockito.mock(ExternalDatabase.class);
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.doReturn(Optional.of(table)).when(db).getTableForReplay("table");
        Mockito.doReturn(Optional.of(db)).when(catalog).getDbForReplay("db");
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.doThrow(new RuntimeException("injected table invalidation failure"))
                .when(cacheMgr).invalidateTableCache(Mockito.any());
        Env env = mockEnv(catalogId, catalog);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(catalogId, "db", "table", 0L);
            Assertions.assertDoesNotThrow(() -> new RefreshManager().replayRefreshTable(log));
        }
    }

    private Env mockEnv(long catalogId, ExternalCatalog catalog) {
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(catalogId);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        return env;
    }
}

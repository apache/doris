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

package org.apache.doris.datasource.plugin;

import org.apache.doris.catalog.Env;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.log.InitCatalogLog;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A database that include_database_list or exclude_database_list keeps out of a catalog's database list must not
 * get into it through a metastore event (a CREATE DATABASE, or an ALTER DATABASE that renames to it) either.
 */
public class PluginDrivenExternalCatalogEventFilterTest {

    @Test
    public void testEventDoesNotAddExcludedDatabase() {
        assertEventKeepsOut(ExternalCatalog.EXCLUDE_DATABASE_LIST, "archived", "archived", "sales2");
    }

    @Test
    public void testEventDoesNotAddDatabaseOutsideIncludeList() {
        assertEventKeepsOut(ExternalCatalog.INCLUDE_DATABASE_LIST, "sales,sales2", "other", "sales2");
    }

    // With the database names cached, an event for keptOut leaves them as they are, and one for allowed adds it.
    private static void assertEventKeepsOut(String filterKey, String filterValue, String keptOut, String allowed) {
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(cacheMgr.commonRefreshExecutor()).thenReturn(MoreExecutors.newDirectExecutorService());
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            FilteredCatalog catalog = new FilteredCatalog(filterKey, filterValue);
            catalog.setInitializedForTest(true);
            Assertions.assertFalse(catalog.getDbNames().contains(keptOut));

            catalog.registerDatabaseFromEvent(keptOut, keptOut);
            Assertions.assertFalse(catalog.getDbNames().contains(keptOut));
            Assertions.assertFalse(catalog.builtDatabases.contains(keptOut));

            catalog.registerDatabaseFromEvent(allowed, allowed);
            Assertions.assertTrue(catalog.getDbNames().contains(allowed));
        }
    }

    private static class FilteredCatalog extends PluginDrivenExternalCatalog {
        private final List<String> builtDatabases = new ArrayList<>();

        FilteredCatalog(String filterKey, String filterValue) {
            super(1L, "event_filter_catalog", null, properties(filterKey, filterValue), "",
                    Mockito.mock(Connector.class));
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        protected List<String> listDatabaseNames() {
            return ImmutableList.of("sales", "archived", "other");
        }

        @Override
        public String fromRemoteDatabaseName(String remoteDatabaseName) {
            return remoteDatabaseName;
        }

        @Override
        protected ExternalDatabase<? extends ExternalTable> buildDbForInit(
                String remoteDbName, String localDbName, long dbId,
                InitCatalogLog.Type logType, boolean checkExists) {
            builtDatabases.add(localDbName);
            ExternalDatabase<? extends ExternalTable> db = Mockito.mock(ExternalDatabase.class);
            Mockito.when(db.getId()).thenReturn(dbId);
            Mockito.when(db.getRemoteName()).thenReturn(remoteDbName);
            Mockito.when(db.getFullName()).thenReturn(localDbName);
            return db;
        }

        private static Map<String, String> properties(String filterKey, String filterValue) {
            Map<String, String> properties = new HashMap<>();
            properties.put("type", "hms");
            properties.put(filterKey, filterValue);
            return properties;
        }
    }
}

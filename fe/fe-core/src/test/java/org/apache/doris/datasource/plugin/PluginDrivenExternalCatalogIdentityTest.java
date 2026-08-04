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

import org.apache.doris.common.util.Util;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.log.InitCatalogLog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class PluginDrivenExternalCatalogIdentityTest {

    @Test
    public void testModeZeroUsesRemoteNameWhenNoMappingExists() {
        assertRegisteredIdentity(0, "SalesDB", null, "SalesDB");
    }

    @Test
    public void testModeOneUsesLowerCaseCanonicalName() {
        assertRegisteredIdentity(1, "SalesDB", null, "salesdb");
    }

    @Test
    public void testModeTwoPreservesRemoteNameCase() {
        assertRegisteredIdentity(2, "SalesDB", null, "SalesDB");
    }

    @Test
    public void testNamingHookUsesMappedLocalName() {
        assertRegisteredIdentity(0, "RemoteDB", "LocalDB", "LocalDB");
    }

    @Test
    public void testModeOneLowerCasesMappedLocalName() {
        assertRegisteredIdentity(1, "RemoteDB", "MappedDB", "mappeddb");
    }

    private static void assertRegisteredIdentity(
            int mode, String remoteName, String mappedName, String expectedLocalName) {
        RecordingCatalog catalog = new RecordingCatalog(mode, mappedName);

        catalog.registerDatabase(remoteName);

        Assertions.assertEquals(remoteName, catalog.registeredRemoteName);
        Assertions.assertEquals(expectedLocalName, catalog.registeredLocalName);
        Assertions.assertEquals(
                Util.genIdByName(catalog.getName(), expectedLocalName), catalog.registeredId);
        Assertions.assertFalse(catalog.registeredCheckExists);
        Assertions.assertEquals(expectedLocalName, catalog.canonicalNameForTest(remoteName));
        Assertions.assertFalse(catalog.hasMetaCacheEntriesForTest());
    }

    private static class RecordingCatalog extends PluginDrivenExternalCatalog {
        private final String mappedName;
        private String registeredRemoteName;
        private String registeredLocalName;
        private long registeredId;
        private boolean registeredCheckExists;

        RecordingCatalog(int mode, String mappedName) {
            super(1L, "identity_test_catalog", null, properties(mode), "",
                    Mockito.mock(Connector.class));
            this.mappedName = mappedName;
        }

        @Override
        public String fromRemoteDatabaseName(String remoteDatabaseName) {
            return mappedName == null ? remoteDatabaseName : mappedName;
        }

        @Override
        protected ExternalDatabase<? extends ExternalTable> buildDbForInit(
                String remoteDbName, String localDbName, long dbId,
                InitCatalogLog.Type logType, boolean checkExists) {
            registeredRemoteName = remoteDbName;
            registeredLocalName = localDbName;
            registeredId = dbId;
            registeredCheckExists = checkExists;
            return Mockito.mock(ExternalDatabase.class);
        }

        String canonicalNameForTest(String remoteDbName) {
            return canonicalLocalDatabaseNameFromRemote(remoteDbName);
        }

        boolean hasMetaCacheEntriesForTest() {
            return databaseNames != null || databases != null;
        }

        private static Map<String, String> properties(int mode) {
            Map<String, String> properties = new HashMap<>();
            properties.put("type", "iceberg");
            properties.put(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, String.valueOf(mode));
            return properties;
        }
    }
}

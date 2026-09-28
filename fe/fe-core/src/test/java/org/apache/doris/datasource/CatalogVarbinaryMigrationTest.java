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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CatalogVarbinaryMigrationTest {
    private static final String MARKER = CatalogProperty.ENABLE_MAPPING_VARBINARY;

    @Test
    void testTimestampMarkerIsJournaledAndCannotBeDisabledByAlter() throws Exception {
        String timestampMarker = CatalogProperty.ENABLE_MAPPING_TIMESTAMP_TZ;
        CatalogMgr manager = new CatalogMgr();
        Map<String, String> properties = new HashMap<>();
        properties.put(MARKER, "true");
        properties.put(timestampMarker, "false");
        BinaryCatalog catalog = new BinaryCatalog(48, properties);
        catalog.gsonPostProcess();
        Assertions.assertEquals("false", catalog.getProperties().get(timestampMarker));
        addCatalog(manager, catalog);
        List<CatalogLog> persisted = new ArrayList<>();
        Env env = journalEnv(persisted);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            manager.migrateVarbinaryMappingProperties();
            manager.migrateVarbinaryMappingProperties();
            Assertions.assertEquals(1, persisted.size());
            Assertions.assertEquals(Collections.singletonMap(timestampMarker, "true"), persisted.get(0).getNewProps());
            Assertions.assertEquals("true", catalog.getProperties().get(timestampMarker));
            Map<String, String> requested = Collections.singletonMap(timestampMarker, "false");
            manager.alterCatalogProps(catalog.getName(), requested);
            Assertions.assertEquals("false", requested.get(timestampMarker));
            Assertions.assertEquals("true", persisted.get(1).getNewProps().get(timestampMarker));
            Assertions.assertTrue(catalog.getEnableMappingTimestampTz());
        }
    }

    @Test
    void testAlterPersistsNormalizedMarkerWithoutMutatingCaller() throws Exception {
        CatalogMgr manager = new CatalogMgr();
        BinaryCatalog catalog = new BinaryCatalog(41, Collections.singletonMap(MARKER, "true"));
        addCatalog(manager, catalog);
        List<CatalogLog> persisted = new ArrayList<>();
        Env env = journalEnv(persisted);
        Map<String, String> requested = Collections.singletonMap(MARKER, "false");
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            manager.alterCatalogProps(catalog.getName(), requested);
        }
        Assertions.assertEquals("false", requested.get(MARKER));
        Assertions.assertEquals(1, persisted.size());
        Assertions.assertEquals("true", persisted.get(0).getNewProps().get(MARKER));
        Assertions.assertEquals("true", catalog.getProperties().get(MARKER));
        // An older follower applies the serialized map verbatim, without the new getter.
        Map<String, String> legacyFollowerProperties = new HashMap<>(Collections.singletonMap(MARKER, "false"));
        legacyFollowerProperties.putAll(persisted.get(0).getNewProps());
        Assertions.assertTrue(Boolean.parseBoolean(legacyFollowerProperties.get(MARKER)));
    }

    @Test
    void testLoadingLegacyCatalogDoesNotHidePendingMigration() throws Exception {
        BinaryCatalog catalog = new BinaryCatalog(42, Collections.singletonMap(MARKER, "false"));
        catalog.gsonPostProcess();
        Assertions.assertTrue(catalog.getEnableMappingVarbinary());
        Assertions.assertEquals("false", catalog.getProperties().get(MARKER));
        catalog.tryModifyCatalogProps(Collections.singletonMap(MARKER, "false"));
        Assertions.assertEquals("false", catalog.getProperties().get(MARKER));
    }

    @Test
    void testMigrationIsReplicatedAndIdempotentAcrossPromotion() throws Exception {
        CatalogMgr manager = new CatalogMgr();
        Map<Long, Map<String, String>> legacyFollowers = new HashMap<>();
        for (long id = 43; id <= 45; id++) {
            Map<String, String> properties = new HashMap<>();
            properties.put("custom.property", "preserved");
            properties.put(CatalogProperty.ENABLE_MAPPING_TIMESTAMP_TZ, "true");
            if (id != 44) {
                properties.put(MARKER, id == 43 ? "false" : "true");
            }
            legacyFollowers.put(id, new HashMap<>(properties));
            BinaryCatalog catalog = new BinaryCatalog(id, properties);
            catalog.gsonPostProcess();
            addCatalog(manager, catalog);
        }
        List<CatalogLog> persisted = new ArrayList<>();
        Env env = journalEnv(persisted);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            manager.migrateVarbinaryMappingProperties();
            manager.migrateVarbinaryMappingProperties();
            Assertions.assertEquals(2, persisted.size());

            CatalogMgr follower = new CatalogMgr();
            for (Map.Entry<Long, Map<String, String>> entry : legacyFollowers.entrySet()) {
                addCatalog(follower, new BinaryCatalog(entry.getKey(), entry.getValue()));
            }
            for (CatalogLog log : persisted) {
                Assertions.assertEquals(Collections.singletonMap(MARKER, "true"), log.getNewProps());
                // Model the old FE's verbatim property replay, as well as the current replay path.
                legacyFollowers.get(log.getCatalogId()).putAll(log.getNewProps());
                follower.replayAlterCatalogProps(log, null, true);
            }
            for (Map<String, String> properties : legacyFollowers.values()) {
                Assertions.assertTrue(Boolean.parseBoolean(properties.get(MARKER)));
                Assertions.assertEquals("preserved", properties.get("custom.property"));
            }
            follower.migrateVarbinaryMappingProperties();
            Assertions.assertEquals(2, persisted.size());
        }
    }

    @Test
    void testFailedJournalWriteRemainsEligibleForMigration() throws Exception {
        CatalogMgr manager = new CatalogMgr();
        BinaryCatalog catalog = new BinaryCatalog(46, Collections.singletonMap(MARKER, "false"));
        addCatalog(manager, catalog);
        Env env = journalEnv(new ArrayList<>());
        EditLog editLog = env.getEditLog();
        Mockito.doThrow(new IllegalStateException("journal unavailable")).doNothing()
                .when(editLog).logCatalogLog(Mockito.eq(OperationType.OP_ALTER_CATALOG_PROPS), Mockito.any());
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(IllegalStateException.class, manager::migrateVarbinaryMappingProperties);
            Assertions.assertEquals("false", catalog.getProperties().get(MARKER));
            manager.migrateVarbinaryMappingProperties();
            Assertions.assertEquals("true", catalog.getProperties().get(MARKER));
            Mockito.verify(env.getEditLog(), Mockito.times(2))
                    .logCatalogLog(Mockito.eq(OperationType.OP_ALTER_CATALOG_PROPS), Mockito.any());
        }
    }

    @Test
    void testNewCatalogAlreadyIncludesMigrationMarker() {
        BinaryCatalog catalog = new BinaryCatalog(47, Collections.singletonMap(MARKER, "false"));
        catalog.setDefaultPropsIfMissing(false);
        Assertions.assertEquals("true", catalog.getProperties().get(MARKER));
        Assertions.assertEquals("true", catalog.getProperties().get(CatalogProperty.ENABLE_MAPPING_TIMESTAMP_TZ));
    }

    private static Env journalEnv(List<CatalogLog> persisted) throws Exception {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(cacheMgr.withCatalogLifecycleLock(Mockito.anyLong(), Mockito.any()))
                .thenAnswer(invocation -> ((java.util.function.Supplier<?>) invocation.getArgument(1)).get());
        Mockito.doAnswer(invocation -> {
            CatalogLog log = invocation.getArgument(1);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            log.write(new DataOutputStream(bytes));
            persisted.add(CatalogLog.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))));
            return null;
        }).when(editLog).logCatalogLog(Mockito.eq(OperationType.OP_ALTER_CATALOG_PROPS), Mockito.any());
        return env;
    }

    @SuppressWarnings("unchecked")
    private static void addCatalog(CatalogMgr manager, ExternalCatalog catalog) throws Exception {
        Field ids = CatalogMgr.class.getDeclaredField("idToCatalog");
        ids.setAccessible(true);
        ((Map<Long, CatalogIf>) ids.get(manager)).put(catalog.getId(), catalog);
        Field names = CatalogMgr.class.getDeclaredField("nameToCatalog");
        names.setAccessible(true);
        ((Map<String, CatalogIf>) names.get(manager)).put(catalog.getName(), catalog);
    }

    private static class BinaryCatalog extends ExternalCatalog {
        BinaryCatalog(long id, Map<String, String> properties) {
            super(id, "binary_catalog_" + id, InitCatalogLog.Type.TEST, "");
            catalogProperty = new CatalogProperty(null, new HashMap<>(properties));
        }

        @Override
        public boolean validatePropertiesBeforeUpdate(Map<String, String> current, Map<String, String> updates) {
            return true;
        }

        @Override
        public void notifyPropertiesUpdated(Map<String, String> updates) {
            // No remote clients are needed to exercise journal persistence and replay.
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext context, String database) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext context, String database, String table) {
            return false;
        }
    }
}

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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.lance.LanceExternalCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Coverage for the surviving Lance target-identity guard in {@link CatalogMgr}:
 * {@code hasLanceIdentityKeyChange} decides whether a property rewrite touches the
 * namespace or storage-routing keys that can land the same table names on a different
 * dataset target, and the ALTER-catalog replay path answers such a change by advancing
 * the Lance admission epoch so in-flight admission snapshots fail revalidation.
 * Same-value rewrites, credential-only changes, and unrelated keys stay no-ops.
 */
public class CatalogMgrLanceGuardTest {

    @Test
    public void testIdentityKeyChangeSemantics() {
        Map<String, String> persisted = new HashMap<>();
        persisted.put("s3.endpoint", "s3://a");
        persisted.put("type", "hms");
        persisted.put("s3.access_key", "old");

        Assertions.assertTrue(CatalogMgr.hasLanceIdentityKeyChange(persisted, props("s3.endpoint", "s3://b")),
                "a value change of an identity key is a target change");
        Assertions.assertFalse(CatalogMgr.hasLanceIdentityKeyChange(persisted, props("s3.endpoint", "s3://a")),
                "a same-value rewrite is an idempotent no-op");
        Assertions.assertTrue(CatalogMgr.hasLanceIdentityKeyChange(persisted, props("s3.region", "us-west")),
                "a newly supplied identity key the persisted properties never set is a change");
        Assertions.assertTrue(CatalogMgr.hasLanceIdentityKeyChange(persisted, props("S3.ENDPOINT", "s3://b")),
                "identity keys match case-insensitively");
        Assertions.assertFalse(CatalogMgr.hasLanceIdentityKeyChange(persisted, props("s3.access_key", "new")),
                "a credential-only change is not a target change");
        Assertions.assertFalse(CatalogMgr.hasLanceIdentityKeyChange(persisted, props("type", "jdbc")),
                "an unrelated property change is not a target change");
        Map<String, String> withNullKey = new HashMap<>();
        withNullKey.put(null, "x");
        Assertions.assertFalse(CatalogMgr.hasLanceIdentityKeyChange(persisted, withNullKey),
                "a null key is never an identity key");
    }

    @Test
    public void testAlterReplayAdvancesAdmissionEpochOnlyOnTargetChange() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        LanceExternalCatalog catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(10L);
        Mockito.when(catalog.getProperties()).thenReturn(props("s3.endpoint", "s3://a"));
        registerCatalog(catalogMgr, catalog);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr metaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
        Mockito.when(metaCacheMgr.withCatalogLifecycleLock(Mockito.anyLong(), Mockito.any()))
                .thenAnswer(invocation -> {
                    Supplier<?> action = invocation.getArgument(1);
                    return action.get();
                });

        try (MockedStatic<Env> currentEnv = Mockito.mockStatic(Env.class)) {
            currentEnv.when(Env::getCurrentEnv).thenReturn(env);

            replayAlter(catalogMgr, 10L, props("s3.access_key", "new"));
            Mockito.verify(catalog, Mockito.never()).advanceIndexTargetVersion();

            replayAlter(catalogMgr, 10L, props("s3.endpoint", "s3://a"));
            Mockito.verify(catalog, Mockito.never()).advanceIndexTargetVersion();

            replayAlter(catalogMgr, 10L, props("s3.endpoint", "s3://b"));
            Mockito.verify(catalog, Mockito.times(1)).advanceIndexTargetVersion();
        }
    }

    private static void replayAlter(CatalogMgr catalogMgr, long catalogId, Map<String, String> newProps)
            throws DdlException {
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalogId);
        log.setNewProps(newProps);
        catalogMgr.replayAlterCatalogProps(log, null, true);
    }

    @SuppressWarnings("unchecked")
    private static void registerCatalog(CatalogMgr catalogMgr, LanceExternalCatalog catalog) throws Exception {
        Field idToCatalogField = CatalogMgr.class.getDeclaredField("idToCatalog");
        idToCatalogField.setAccessible(true);
        ((ConcurrentMap<Long, CatalogIf<? extends DatabaseIf<? extends TableIf>>>)
                idToCatalogField.get(catalogMgr)).put(catalog.getId(), catalog);
    }

    private static Map<String, String> props(String... keyValue) {
        Map<String, String> properties = new HashMap<>();
        for (int i = 0; i < keyValue.length; i += 2) {
            properties.put(keyValue[i], keyValue[i + 1]);
        }
        return properties;
    }
}

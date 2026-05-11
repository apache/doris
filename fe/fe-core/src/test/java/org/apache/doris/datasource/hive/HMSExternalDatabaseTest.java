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

package org.apache.doris.datasource.hive;

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.qe.BDPAuthContext;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Tests for the multi-tenant isolation behavior between
 * {@link HMSExternalDatabase#getTableNullable(String)} and the parent
 * {@link ExternalDatabase} meta cache loader.
 *
 * Invariants:
 *  - The metaCache key carries auth info (hadoopUserName + viewBased) so that
 *    different auth identities get independent HMSExternalTable instances and
 *    metadata caches.
 *  - The HMSExternalTable.id MUST be derived from the pure table name only,
 *    so that the same physical Hive table keeps a stable id across different
 *    auth identities. Otherwise BaseTableInfo / MTMV id-based lookups break
 *    and async materialized view routing fails across users.
 */
public class HMSExternalDatabaseTest {

    private static final String CATALOG_NAME = "hms_ctl";
    private static final String DB_NAME = "hms_db";
    private static final String TABLE_NAME = "mytable";

    private boolean originalRunningUnitTest;

    @BeforeEach
    public void setUp() {
        originalRunningUnitTest = FeConstants.runningUnitTest;
        // skip the remote tableExist check inside buildTableForInit
        FeConstants.runningUnitTest = true;
        BDPAuthContext.clear();

        // Bypass real init chains. ExternalDatabase.makeSureInitialized() would
        // otherwise call extCatalog.makeSureInitialized(), which initializes the
        // HMS client and dereferences hmsProperties (NPE in unit tests).
        new MockUp<ExternalDatabase>() {
            @Mock
            public void makeSureInitialized() {
                // do nothing
            }
        };
        new MockUp<ExternalCatalog>() {
            @Mock
            public void makeSureInitialized() {
                // do nothing
            }
        };
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = originalRunningUnitTest;
        BDPAuthContext.clear();
    }

    private HMSExternalCatalog newCatalog() {
        HMSExternalCatalog catalog = new HMSExternalCatalog();
        setField(catalog, "name", CATALOG_NAME);
        return catalog;
    }

    private HMSExternalDatabase newDatabase(HMSExternalCatalog catalog) {
        HMSExternalDatabase db = new HMSExternalDatabase(catalog, 10000L, DB_NAME, DB_NAME);
        // bypass real init in case any code path still triggers it.
        setField(db, "initialized", true);
        return db;
    }

    /**
     * Reflective field setter. Walks up the class hierarchy to find the field.
     */
    private static void setField(Object target, String fieldName, Object value) {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field f = clazz.getDeclaredField(fieldName);
                f.setAccessible(true);
                f.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Cannot set field " + fieldName, e);
            }
        }
        throw new RuntimeException("Field not found: " + fieldName
                + " on " + target.getClass().getName());
    }

    /**
     * Allocate an instance without invoking any constructor. Used to inject a
     * non-null MetaCache placeholder while every method on it is intercepted
     * by JMockit MockUp.
     */
    @SuppressWarnings("unchecked")
    private static <T> T newUninitializedInstance(Class<T> clazz) {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Object unsafe = theUnsafe.get(null);
            return (T) unsafeClass.getMethod("allocateInstance", Class.class)
                    .invoke(unsafe, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate instance of " + clazz, e);
        }
    }

    private static BDPAuthContext authOf(String hadoopUserName, boolean viewBased) {
        BDPAuthContext ctx = new BDPAuthContext();
        ctx.setHadoopUserName(hadoopUserName);
        ctx.setViewBased(viewBased);
        return ctx;
    }

    /**
     * Verify that getTableNullable():
     *  - feeds the metaCache with an auth-suffixed cacheKey (preserving
     *    per-tenant cache isolation);
     *  - feeds the metaCache with a stable id derived from the pure table
     *    name only (so MV / id-based lookups work across users).
     */
    @Test
    public void testGetTableNullableCacheKeyAndIdAcrossAuthIdentities() {
        HMSExternalCatalog catalog = newCatalog();
        HMSExternalDatabase db = newDatabase(catalog);

        // Capture the (cacheKey, id) pairs that getTableNullable passes to
        // metaCache.getMetaObj. We do not care about the actual cache entry,
        // only about the inputs that the multi-tenant logic produces.
        final List<String> capturedKeys = new ArrayList<>();
        final List<Long> capturedIds = new ArrayList<>();

        new MockUp<MetaCache<HMSExternalTable>>() {
            @Mock
            public Optional<HMSExternalTable> getMetaObj(String name, long id) {
                capturedKeys.add(name);
                capturedIds.add(id);
                return Optional.empty();
            }
        };

        // Inject a non-null metaCache placeholder so the field access is safe.
        // The MockUp above intercepts every getMetaObj call regardless of instance.
        setField(db, "metaCache", newUninitializedInstance(MetaCache.class));

        // First request: as user "root"
        BDPAuthContext rootCtx = authOf("root", false);
        rootCtx.setThreadLocalInfo();
        Assertions.assertNull(db.getTableNullable(TABLE_NAME));

        // Second request: as user "test"
        BDPAuthContext testCtx = authOf("test", false);
        testCtx.setThreadLocalInfo();
        Assertions.assertNull(db.getTableNullable(TABLE_NAME));

        Assertions.assertEquals(2, capturedKeys.size());
        Assertions.assertEquals(2, capturedIds.size());

        // cacheKey must carry auth info -> per-tenant cache isolation preserved
        Assertions.assertEquals(TABLE_NAME + "$root$false", capturedKeys.get(0));
        Assertions.assertEquals(TABLE_NAME + "$test$false", capturedKeys.get(1));
        Assertions.assertNotEquals(capturedKeys.get(0), capturedKeys.get(1));

        // id must NOT carry auth info -> stable id across auth identities
        long expectedId = Util.genIdByName(CATALOG_NAME, DB_NAME, TABLE_NAME);
        Assertions.assertEquals(expectedId, capturedIds.get(0).longValue(),
                "tableId for root must be derived from the pure table name");
        Assertions.assertEquals(expectedId, capturedIds.get(1).longValue(),
                "tableId for test must be derived from the pure table name");
        Assertions.assertEquals(capturedIds.get(0), capturedIds.get(1),
                "tableId must be stable across different auth identities");
    }

    /**
     * If a composite cache key (already containing '$') is passed in directly
     * (e.g. by the parent metaCache loader path), getTableNullable must keep
     * it as the cacheKey but still derive the id from the pure table name
     * before the first '$'.
     */
    @Test
    public void testGetTableNullableWithCompositeKey() {
        HMSExternalCatalog catalog = newCatalog();
        HMSExternalDatabase db = newDatabase(catalog);

        final List<String> capturedKeys = new ArrayList<>();
        final List<Long> capturedIds = new ArrayList<>();
        new MockUp<MetaCache<HMSExternalTable>>() {
            @Mock
            public Optional<HMSExternalTable> getMetaObj(String name, long id) {
                capturedKeys.add(name);
                capturedIds.add(id);
                return Optional.empty();
            }
        };
        setField(db, "metaCache", newUninitializedInstance(MetaCache.class));

        // No BDPAuthContext set; with a composite key, getTableNullable must
        // not require BDPAuthContext.
        String compositeKey = TABLE_NAME + "$root$true";
        Assertions.assertNull(db.getTableNullable(compositeKey));

        Assertions.assertEquals(1, capturedKeys.size());
        Assertions.assertEquals(compositeKey, capturedKeys.get(0));

        long expectedId = Util.genIdByName(CATALOG_NAME, DB_NAME, TABLE_NAME);
        Assertions.assertEquals(expectedId, capturedIds.get(0).longValue(),
                "tableId should always be derived from the pure table name "
                        + "(part before the first '$')");
    }

    /**
     * Without a BDPAuthContext bound, plain table names must be rejected so
     * that callers always go through the multi-tenant aware path.
     */
    @Test
    public void testGetTableNullableRequiresAuthContextForPlainName() {
        HMSExternalCatalog catalog = newCatalog();
        HMSExternalDatabase db = newDatabase(catalog);

        // Even without metaCache being touched, the auth precondition must fire first.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> db.getTableNullable(TABLE_NAME));
    }

    /**
     * Sanity wiring: ensure ExternalCatalog.getName() round-trips so that the
     * id derivation we assert against uses the same catalog name.
     */
    @Test
    public void testCatalogNameWiring() {
        ExternalCatalog catalog = newCatalog();
        Assertions.assertEquals(CATALOG_NAME, catalog.getName());
    }
}

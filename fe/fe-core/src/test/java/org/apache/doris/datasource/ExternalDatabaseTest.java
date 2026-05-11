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

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.metacache.MetaCache;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for the parent {@link ExternalDatabase}.
 *
 * Focuses on the meta-object cache loader configured by
 * {@code ExternalDatabase.buildMetaCache}, which is shared by every external
 * source (HMS / Iceberg / JDBC / Paimon / ES / MaxCompute / Trino / LakeSoul
 * / RemoteDoris / InfoSchema / Mysql / Test).
 *
 * Invariant under test:
 *
 *   When the metaCache is queried with a cacheKey, the cache loader MUST
 *   strip everything after the first '$' (the multi-tenant suffix used by
 *   HMS) before deriving:
 *     - the localTableName argument passed to buildTableForInit, and
 *     - the tblId argument passed to buildTableForInit.
 *
 *   For non-HMS sources whose cacheKey never contains '$', this is a
 *   no-op. For HMS, this guarantees that the resulting external table has
 *   a stable id across different auth identities, which is required by
 *   BaseTableInfo / MTMV id-based lookups and async materialized view
 *   routing.
 *
 *   We exercise the loader through HMSExternalDatabase as the concrete
 *   subclass; the assertions only target parent-class behavior.
 */
public class ExternalDatabaseTest {

    private static final String CATALOG_NAME = "hms_ctl";
    private static final String DB_NAME = "hms_db";
    private static final String TABLE_NAME = "mytable";

    private boolean originalRunningUnitTest;

    @BeforeEach
    public void setUp() {
        originalRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;

        // ExternalDatabase.makeSureInitialized() reaches into the catalog
        // implementation, which is not available in unit tests; bypass it.
        new MockUp<ExternalDatabase>() {
            @Mock
            public void makeSureInitialized() {
                // no-op
            }
        };
        new MockUp<ExternalCatalog>() {
            @Mock
            public void makeSureInitialized() {
                // no-op
            }
        };
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = originalRunningUnitTest;
    }

    /**
     * Reflective field setter that walks up the class hierarchy.
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
     * Force-build the metaCache and trigger its meta-object cache loader for
     * the given cacheKey. Mirrors what MetaCache.getMetaObj does internally
     * on a cache miss.
     */
    @SuppressWarnings("unchecked")
    private static void invokeCacheLoader(ExternalDatabase<?> db, String cacheKey) {
        try {
            Method m = ExternalDatabase.class.getDeclaredMethod("buildMetaCache");
            m.setAccessible(true);
            m.invoke(db);

            Field f = ExternalDatabase.class.getDeclaredField("metaCache");
            f.setAccessible(true);
            MetaCache<HMSExternalTable> metaCache = (MetaCache<HMSExternalTable>) f.get(db);

            metaCache.getMetaObj(cacheKey, 0L);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke cache loader for key " + cacheKey, e);
        }
    }

    /**
     * Verify that the cache loader registered by buildMetaCache derives both
     * the localTableName argument and the tblId argument from the pure name
     * (everything before the first '$').
     */
    @Test
    public void testBuildMetaCacheLoaderUsesPureNameForBothNameAndId() {
        HMSExternalCatalog catalog = new HMSExternalCatalog();
        setField(catalog, "name", CATALOG_NAME);
        HMSExternalDatabase db = new HMSExternalDatabase(catalog, 10000L, DB_NAME, DB_NAME);
        setField(db, "initialized", true);

        AtomicReference<String> capturedLocalName = new AtomicReference<>();
        AtomicReference<Long> capturedTblId = new AtomicReference<>();

        new MockUp<HMSExternalDatabase>() {
            @Mock
            public HMSExternalTable buildTableForInit(String remoteTableName, String localTableName,
                    long tblId, ExternalCatalog catalog0, ExternalDatabase db0, boolean checkExists) {
                capturedLocalName.set(localTableName);
                capturedTblId.set(tblId);
                return null;
            }
        };

        long expectedId = Util.genIdByName(CATALOG_NAME, DB_NAME, TABLE_NAME);

        // Auth-suffixed cacheKey (HMS multi-tenant style): both name and id
        // must be stripped to the pure form.
        invokeCacheLoader(db, TABLE_NAME + "$root$false");
        Assertions.assertEquals(TABLE_NAME, capturedLocalName.get());
        Assertions.assertEquals(expectedId, capturedTblId.get().longValue(),
                "tblId must be derived from the pure table name "
                        + "(part before the first dollar sign)");

        // Different auth identity must yield the same id.
        invokeCacheLoader(db, TABLE_NAME + "$test$true");
        Assertions.assertEquals(TABLE_NAME, capturedLocalName.get());
        Assertions.assertEquals(expectedId, capturedTblId.get().longValue(),
                "tblId must stay stable across different auth identities");

        // Non-HMS-style cacheKey (no dollar sign): split is a no-op.
        invokeCacheLoader(db, TABLE_NAME);
        Assertions.assertEquals(TABLE_NAME, capturedLocalName.get());
        Assertions.assertEquals(expectedId, capturedTblId.get().longValue(),
                "for cacheKeys without a dollar sign, the loader behavior is unchanged");
    }
}

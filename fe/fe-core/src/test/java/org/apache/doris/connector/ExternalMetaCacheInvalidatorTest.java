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

package org.apache.doris.connector;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.ExternalMetaCacheMgr;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;

/**
 * Verifies {@link ExternalMetaCacheInvalidator} routes each SPI invalidate*
 * call to the right method on {@link ExternalMetaCacheMgr}, scoped to the
 * catalog id captured at construction time.
 *
 * <p>The static {@code Env.getCurrentEnv()} is stubbed via Mockito so the
 * test runs without bringing up the full FE.
 */
public class ExternalMetaCacheInvalidatorTest {

    private static final long CATALOG_ID = 42L;

    @Test
    public void invalidateAllRoutesToInvalidateCatalog() {
        runWithMockedMgr(mgr -> {
            new ExternalMetaCacheInvalidator(CATALOG_ID).invalidateAll();
            Mockito.verify(mgr).invalidateCatalog(CATALOG_ID);
            Mockito.verifyNoMoreInteractions(mgr);
        });
    }

    @Test
    public void invalidateDatabaseRoutesToInvalidateDb() {
        runWithMockedMgr(mgr -> {
            new ExternalMetaCacheInvalidator(CATALOG_ID).invalidateDatabase("sales");
            Mockito.verify(mgr).invalidateDb(CATALOG_ID, "sales");
            Mockito.verifyNoMoreInteractions(mgr);
        });
    }

    @Test
    public void invalidateTableRoutesToInvalidateTable() {
        runWithMockedMgr(mgr -> {
            new ExternalMetaCacheInvalidator(CATALOG_ID).invalidateTable("sales", "orders");
            Mockito.verify(mgr).invalidateTable(CATALOG_ID, "sales", "orders");
            Mockito.verifyNoMoreInteractions(mgr);
        });
    }

    /**
     * Partition-scope invalidation currently falls back to table-level invalidation
     * because the SPI carries partition column values, not names — see the inline
     * comment in {@link ExternalMetaCacheInvalidator#invalidatePartition}. This
     * test pins the documented behavior so a future SPI extension that allows the
     * scope to narrow is forced to update the bridge AND this test together.
     */
    @Test
    public void invalidatePartitionFallsBackToInvalidateTable() {
        runWithMockedMgr(mgr -> {
            new ExternalMetaCacheInvalidator(CATALOG_ID)
                    .invalidatePartition("sales", "orders", Arrays.asList("2024", "01"));
            Mockito.verify(mgr).invalidateTable(CATALOG_ID, "sales", "orders");
            Mockito.verifyNoMoreInteractions(mgr);
        });
    }

    /**
     * Stats-only invalidation is intentionally a no-op today — see the inline
     * comment in {@link ExternalMetaCacheInvalidator#invalidateStatistics}.
     * Verifying zero interactions makes any silent change visible.
     */
    @Test
    public void invalidateStatisticsIsNoopForNow() {
        runWithMockedMgr(mgr -> {
            new ExternalMetaCacheInvalidator(CATALOG_ID).invalidateStatistics("sales", "orders");
            Mockito.verifyNoInteractions(mgr);
        });
    }

    private static void runWithMockedMgr(java.util.function.Consumer<ExternalMetaCacheMgr> body) {
        ExternalMetaCacheMgr mgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(mgr);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            body.accept(mgr);
        }
    }
}

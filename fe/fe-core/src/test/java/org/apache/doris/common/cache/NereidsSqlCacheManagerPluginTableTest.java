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

package org.apache.doris.common.cache;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.nereids.SqlCacheContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

/**
 * Unit tests for the lookup-time freshness re-check the SQL-result-cache migration wired into
 * {@link NereidsSqlCacheManager} for flipped lakehouse tables. The stored data-version token
 * ({@code getNewestUpdateVersionOrTime()}) is compared against the live one: equal &rArr; NOT_CHANGED
 * (cache may be served), different &rArr; CHANGED_AND_INVALIDATE_CACHE. This is the correctness core of the
 * feature — a cache must invalidate exactly when the underlying data changes. RED on the pre-cutover HEAD,
 * whose gate rejected {@code PLUGIN_EXTERNAL_TABLE} outright (always invalidate, never a hit) and re-checked
 * only {@code instanceof HMSExternalTable}.
 */
public class NereidsSqlCacheManagerPluginTableTest {

    private static final String CTL = "hms_ctl";
    private static final String DB = "hms_db";
    private static final String TBL = "t";
    private static final long TABLE_ID = 42L;

    private PluginDrivenMvccExternalTable mockTable(long liveToken) {
        PluginDrivenMvccExternalTable table = Mockito.mock(PluginDrivenMvccExternalTable.class);
        DatabaseIf db = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.isInternalCatalog()).thenReturn(false);
        Mockito.when(catalog.getName()).thenReturn(CTL);
        Mockito.when(catalog.getProperties()).thenReturn(new java.util.HashMap<>());
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(db.getFullName()).thenReturn(DB);
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(table.getName()).thenReturn(TBL);
        Mockito.when(table.isTemporary()).thenReturn(false);
        Mockito.when(table.getType()).thenReturn(TableType.PLUGIN_EXTERNAL_TABLE);
        Mockito.when(table.getNewestUpdateVersionOrTime()).thenReturn(liveToken);
        return table;
    }

    /** Wires a mock Env so that findTableIf(env, {CTL,DB,TBL}) resolves to the given live table. */
    private Env mockEnvResolvingTo(PluginDrivenMvccExternalTable liveTable) {
        Env env = Mockito.mock(Env.class);
        CatalogMgr mgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf db = Mockito.mock(DatabaseIf.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(mgr);
        Mockito.when(mgr.getCatalog(CTL)).thenReturn(catalog);
        Mockito.doReturn(Optional.of(db)).when(catalog).getDb(DB);
        Mockito.doReturn(Optional.of(liveTable)).when(db).getTable(TBL);
        return env;
    }

    private boolean isChangedField(Object verdict, String field) {
        return Deencapsulation.getField(verdict, field);
    }

    /** Same stored and live token &rArr; the cache is fresh (NOT_CHANGED). */
    @Test
    public void testNotChangedWhenTokenUnchanged() {
        long token = 1_700_000_000_000L;
        SqlCacheContext context = new SqlCacheContext(UserIdentity.ROOT);
        context.addUsedTable(mockTable(token));                 // stores TableVersion(id, token, PLUGIN)

        Env env = mockEnvResolvingTo(mockTable(token));         // live token unchanged
        Object verdict = Deencapsulation.invoke(new NereidsSqlCacheManager(),
                "tablesOrDataChanged", env, context);

        Assertions.assertFalse(isChangedField(verdict, "changed"),
                "an unchanged token must keep the cache (NOT_CHANGED)");
    }

    /** Live token advanced past the stored one &rArr; data changed &rArr; invalidate. */
    @Test
    public void testInvalidateWhenTokenChanged() {
        long storedToken = 1_700_000_000_000L;
        SqlCacheContext context = new SqlCacheContext(UserIdentity.ROOT);
        context.addUsedTable(mockTable(storedToken));

        Env env = mockEnvResolvingTo(mockTable(storedToken + 1000L)); // data mutated: newer token
        Object verdict = Deencapsulation.invoke(new NereidsSqlCacheManager(),
                "tablesOrDataChanged", env, context);

        Assertions.assertTrue(isChangedField(verdict, "changed"),
                "a changed token must invalidate the cache");
        Assertions.assertTrue(isChangedField(verdict, "invalidCache"),
                "a data change must evict the stale entry, not just miss");
    }
}

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

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.mvcc.ConnectorSqlCacheVersion;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.mvcc.MvccReadSelector;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;

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

    @Test
    public void nativeVersionInvalidatesByIdentityWithoutOrderingOrPartitionFreshness() {
        PluginDrivenMvccExternalTable stored = mockTable(123L);
        Mockito.when(stored.getSqlCacheVersion(Optional.empty()))
                .thenReturn(Optional.of(new ConnectorSqlCacheVersion("uuid:9:schema1:metadata1", 1000L)));
        SqlCacheContext context = new SqlCacheContext(UserIdentity.ROOT);
        context.addUsedTable(stored);
        PluginDrivenMvccExternalTable live = mockTable(123L);
        Env env = mockEnvResolvingTo(live);
        for (String token : new String[] {"uuid:9:schema1:metadata1", "uuid:2:schema1:metadata2",
                "uuid:9:schema2:metadata3", "new-uuid:9:schema1:metadata1"}) {
            Mockito.when(live.getSqlCacheVersion(Optional.empty()))
                    .thenReturn(Optional.of(new ConnectorSqlCacheVersion(token, 1000L)));
            Object verdict = Deencapsulation.invoke(new NereidsSqlCacheManager(), "tablesOrDataChanged",
                    env, context, new StatementContext());
            Assertions.assertEquals(!token.equals("uuid:9:schema1:metadata1"), isChangedField(verdict, "changed"));
        }
        Mockito.when(live.getSqlCacheVersion(Optional.empty())).thenReturn(Optional.empty());
        Object verdict = Deencapsulation.invoke(new NereidsSqlCacheManager(), "tablesOrDataChanged",
                    env, context, new StatementContext());
        Assertions.assertTrue(isChangedField(verdict, "invalidCache"));
        Mockito.verify(stored, Mockito.never()).getNewestUpdateVersionOrTime();
        Mockito.verify(live, Mockito.never()).getNewestUpdateVersionOrTime();
    }

    @Test
    public void revalidationResolvesEachOriginalReferenceAndInvalidatesOnlyWhenItsVersionChanges() {
        PluginDrivenMvccExternalTable table = mockTable(999L);
        MvccSnapshot mainPin = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot devPin = Mockito.mock(MvccSnapshot.class);
        ConnectorSqlCacheVersion main = new ConnectorSqlCacheVersion("uuid:100:schema3:metadata", 1000L);
        ConnectorSqlCacheVersion dev = new ConnectorSqlCacheVersion("uuid:80:schema3:metadata", 800L);
        Mockito.when(table.getSqlCacheVersion(Optional.of(mainPin))).thenReturn(Optional.of(main));
        Mockito.when(table.getSqlCacheVersion(Optional.of(devPin))).thenReturn(Optional.of(dev));
        Mockito.when(table.getSqlCacheVersion(Optional.empty())).thenReturn(Optional.of(main));
        MvccReadSelector selector = MvccReadSelector.of(Optional.of(TableSnapshot.versionOf("dev")), Optional.empty());
        SqlCacheContext context = new SqlCacheContext(UserIdentity.ROOT);
        context.addUsedTable(table, Optional.of(mainPin));
        context.addUsedTable(table, Optional.of(devPin), selector);
        StatementContext statement = Mockito.mock(StatementContext.class);
        Mockito.when(statement.getSnapshot(Mockito.eq(table), Mockito.any(), Mockito.any()))
                .thenAnswer(call -> {
                    Optional<TableSnapshot> requested = call.getArgument(1);
                    Assertions.assertEquals("dev", requested.orElseThrow().getValue());
                    return Optional.of(devPin);
                });
        Env env = mockEnvResolvingTo(table);
        Object same = Deencapsulation.invoke(new NereidsSqlCacheManager(), "tablesOrDataChanged", env, context, statement);
        Assertions.assertFalse(isChangedField(same, "changed"));
        Mockito.verify(statement).loadSnapshots(Mockito.eq(table), Mockito.any(), Mockito.any());
        Mockito.when(table.getSqlCacheVersion(Optional.of(devPin)))
                .thenReturn(Optional.of(new ConnectorSqlCacheVersion("uuid:90:schema3:metadata", 800L)));
        Object moved = Deencapsulation.invoke(new NereidsSqlCacheManager(), "tablesOrDataChanged", env, context, statement);
        Assertions.assertTrue(isChangedField(moved, "invalidCache"));
        Mockito.verify(table, Mockito.never()).getNewestUpdateVersionOrTime();
    }

    @Test
    public void expiredSnapshotCannotPassVersionValidation() {
        PluginDrivenMvccExternalTable table = mockTable(999L);
        MvccSnapshot pin = Mockito.mock(MvccSnapshot.class);
        Mockito.when(table.getSqlCacheVersion(Optional.of(pin)))
                .thenReturn(Optional.of(new ConnectorSqlCacheVersion("uuid:80:schema3:metadata", 800L)));
        SqlCacheContext context = new SqlCacheContext(UserIdentity.ROOT);
        context.addUsedTable(table, Optional.of(pin),
                MvccReadSelector.of(Optional.of(TableSnapshot.versionOf("80")), Optional.empty()));
        StatementContext statement = Mockito.mock(StatementContext.class);
        Mockito.doThrow(new IllegalStateException("can't find snapshot by id: 80"))
                .when(statement).loadSnapshots(Mockito.eq(table), Mockito.any(), Mockito.any());
        Env env = mockEnvResolvingTo(table);
        Assertions.assertThrows(IllegalStateException.class,
                () -> Deencapsulation.invoke(new NereidsSqlCacheManager(), "tablesOrDataChanged", env, context, statement));
        Mockito.verify(table, Mockito.never()).getSqlCacheVersion(Optional.empty());
    }

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
                "tablesOrDataChanged",
                    env, context, new StatementContext());

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
                "tablesOrDataChanged",
                    env, context, new StatementContext());

        Assertions.assertTrue(isChangedField(verdict, "changed"),
                "a changed token must invalidate the cache");
        Assertions.assertTrue(isChangedField(verdict, "invalidCache"),
                "a data change must evict the stale entry, not just miss");
    }
}

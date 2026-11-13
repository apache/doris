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
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.ConnectorStatementScopeImpl;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.mvcc.ConnectorSqlCacheVersion;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.mvcc.MvccReadSelector;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void publicationResolvesBranchAgainWithoutChangingTheExecutingStatement(boolean moved) {
        PluginDrivenMvccExternalTable table = mockTable(999L);
        MvccSnapshot oldPin = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot currentPin = Mockito.mock(MvccSnapshot.class);
        TableSnapshot branch = TableSnapshot.versionOf("dev");
        Mockito.when(table.loadSnapshot(Mockito.any(), Mockito.eq(Optional.empty()))).thenReturn(oldPin);
        Mockito.when(table.getSqlCacheVersion(Optional.of(oldPin)))
                .thenReturn(Optional.of(new ConnectorSqlCacheVersion("dev:80", 800L)));
        Mockito.when(table.getSqlCacheVersion(Optional.of(currentPin)))
                .thenReturn(Optional.of(new ConnectorSqlCacheVersion(moved ? "dev:90" : "dev:80", 800L)));
        ConnectContext connection = Mockito.spy(new ConnectContext());
        connection.getSessionVariable().setEnableSqlCache(false);
        connection.setEnv(mockEnvResolvingTo(table));
        try (StatementContext executing = new StatementContext(connection, null)) {
            connection.setStatementContext(executing);
            executing.loadSnapshots(table, Optional.of(branch), Optional.empty());
            SqlCacheContext candidate = new SqlCacheContext(UserIdentity.ROOT);
            candidate.addUsedTable(table, Optional.of(oldPin),
                    MvccReadSelector.of(Optional.of(branch), Optional.empty()));
            ConnectorStatementScope originalScope = executing.getOrCreateConnectorStatementScope();
            originalScope.computeIfAbsent("pinned-table", () -> oldPin);
            ConnectorStatementScope[] validationScope = new ConnectorStatementScope[1];
            Mockito.when(table.loadSnapshot(Mockito.any(), Mockito.eq(Optional.empty()))).thenAnswer(call -> {
                Optional<TableSnapshot> requested = call.getArgument(0);
                Assertions.assertEquals("dev", requested.orElseThrow().getValue());
                ConnectorStatementScope scope = ConnectorSessionBuilder.from(connection).build().getStatementScope();
                validationScope[0] = scope;
                return scope.computeIfAbsent("pinned-table", () -> currentPin);
            });
            // Arrow Flight defers query teardown; publication's temporary scope must still close immediately.
            Mockito.doReturn(false).when(connection).isReturnResultFromLocal();

            Assertions.assertTrue(candidate.supportSqlCache());
            Assertions.assertTrue(candidate.metadataVersionsMatch());
            Assertions.assertEquals(1, candidate.getUsedTables().size());
            NereidsSqlCacheManager manager = new NereidsSqlCacheManager();
            Deencapsulation.invoke(manager, "publishIfDependenciesValid", connection, "branch", candidate);

            Assertions.assertEquals(!moved, manager.getSqlCaches().getIfPresent("branch") != null);
            Assertions.assertSame(executing, connection.getStatementContext());
            Assertions.assertNotSame(originalScope, validationScope[0]);
            Assertions.assertTrue(((ConnectorStatementScopeImpl) validationScope[0]).isClosed());
            Assertions.assertFalse(((ConnectorStatementScopeImpl) originalScope).isClosed());
            Assertions.assertSame(oldPin, executing.getSnapshot(table, Optional.of(branch), Optional.empty()).get());
            Mockito.verify(table, Mockito.times(2)).loadSnapshot(Mockito.any(), Mockito.eq(Optional.empty()));
            executing.closeConnectorStatementScope();
        }
    }

    @Test
    public void publicationRejectsMetadataInvalidatedDuringVersionValidation() {
        PluginDrivenMvccExternalTable table = mockTable(999L);
        ConnectorSqlCacheVersion token = new ConnectorSqlCacheVersion("uuid:80", 800L);
        Mockito.when(table.getSqlCacheVersion(Optional.empty())).thenReturn(Optional.of(token));
        NereidsSqlCacheManager manager = new NereidsSqlCacheManager();
        SqlCacheContext candidate = new SqlCacheContext(UserIdentity.ROOT, manager.newMetadataSnapshot());
        candidate.addUsedTable(table);
        Mockito.when(table.getSqlCacheVersion(Optional.empty())).thenAnswer(call -> {
            manager.invalidateAboutTable(table);
            return Optional.of(token);
        });
        ConnectContext connection = new ConnectContext();
        connection.getSessionVariable().setEnableSqlCache(false);
        connection.setEnv(mockEnvResolvingTo(table));

        Deencapsulation.invoke(manager, "publishIfDependenciesValid", connection, "changed", candidate);

        Assertions.assertFalse(candidate.metadataVersionsMatch());
        Assertions.assertNull(manager.getSqlCaches().getIfPresent("changed"));
    }

    @Test
    public void publicationValidationFailureDoesNotFailTheQuery() {
        PluginDrivenMvccExternalTable table = mockTable(999L);
        Mockito.when(table.getSqlCacheVersion(Optional.empty()))
                .thenReturn(Optional.of(new ConnectorSqlCacheVersion("uuid:80", 800L)));
        SqlCacheContext candidate = new SqlCacheContext(UserIdentity.ROOT);
        candidate.addUsedTable(table);
        ConnectContext connection = new ConnectContext();
        connection.getSessionVariable().setEnableSqlCache(false);
        connection.setEnv(mockEnvResolvingTo(table));
        ConnectorStatementScope[] validationScope = new ConnectorStatementScope[1];
        Mockito.when(table.getSqlCacheVersion(Optional.empty())).thenAnswer(call -> {
            validationScope[0] = ConnectorSessionBuilder.from(connection).build().getStatementScope();
            throw new IllegalStateException("unavailable");
        });
        NereidsSqlCacheManager manager = new NereidsSqlCacheManager();
        Assertions.assertDoesNotThrow(() -> Deencapsulation.invoke(
                manager, "publishIfDependenciesValid", connection, "failed", candidate));
        Assertions.assertNull(manager.getSqlCaches().getIfPresent("failed"));
        Assertions.assertNull(connection.getStatementContext());
        Assertions.assertTrue(((ConnectorStatementScopeImpl) validationScope[0]).isClosed());
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
        InternalCatalog internal = Mockito.mock(InternalCatalog.class);
        Mockito.when(internal.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(internal);
        Mockito.when(env.getCatalogMgr()).thenReturn(mgr);
        Mockito.when(mgr.getCatalog(CTL)).thenReturn(catalog);
        Mockito.when(catalog.getProperties()).thenReturn(new java.util.HashMap<>());
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

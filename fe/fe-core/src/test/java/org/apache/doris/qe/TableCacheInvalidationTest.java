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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Config;
import org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.mtmv.MTMVRelationManager;
import org.apache.doris.mtmv.MTMVService;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.SqlCacheContext.FullTableName;
import org.apache.doris.nereids.SqlCacheContext.ScanTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.stats.SimpleAggCacheMgr;
import org.apache.doris.persist.AlterViewInfo;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.ReplaceTableOperationLog;
import org.apache.doris.persist.SetPartitionVersionOperationLog;
import org.apache.doris.persist.TableInfo;
import org.apache.doris.persist.TableRenameColumnInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.SqlCache;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.PartitionCommitInfo;
import org.apache.doris.transaction.TableCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class TableCacheInvalidationTest extends TestWithFeService {
    private final RecordingSimpleAggCache aggregates = new RecordingSimpleAggCache();
    private int sqlCapacity;
    private int partitionCapacity;
    private Database database;
    private OlapTable target;
    private OlapTable control;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("cache_replay");
    }

    @BeforeEach
    void prepareTables() throws Exception {
        sqlCapacity = Config.sql_cache_manage_num;
        partitionCapacity = Config.cache_partition_meta_table_manage_num;
        Config.sql_cache_manage_num = 100;
        Config.cache_partition_meta_table_manage_num = 100;
        NereidsSqlCacheManager.updateConfig();
        NereidsSortedPartitionsCacheManager.updateConfig();
        connectContext.setDatabase("cache_replay");
        connectContext.getSessionVariable().setEnableSqlCache(true);
        connectContext.getSessionVariable().cacheSortedPartitionIntervalSecond = -1;
        dropTable("cache_replay.target", false);
        dropTable("cache_replay.control", false);
        dropTable("cache_replay.renamed", false);
        dropTable("cache_replay.plain", false);
        for (String name : ImmutableList.of("target", "control")) {
            createTable("CREATE TABLE cache_replay." + name + " (id INT, v INT) DUPLICATE KEY(id) "
                    + "PARTITION BY RANGE(id) (PARTITION p1 VALUES LESS THAN (10), "
                    + "PARTITION p2 VALUES LESS THAN (20)) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                    + "PROPERTIES ('replication_num'='1')");
        }
        createTable("CREATE TABLE cache_replay.plain (id INT, v INT) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        database = Env.getCurrentInternalCatalog().getDbOrMetaException("cache_replay");
        target = (OlapTable) database.getTableOrMetaException("target");
        control = (OlapTable) database.getTableOrMetaException("control");
        Env.getCurrentEnv().getSqlCacheManager().invalidateAll();
        Env.getCurrentEnv().getSortedPartitionsCacheManager().invalidateAll();
        aggregates.invalidated.clear();
        aggregates.onInvalidate = () -> { };
        SimpleAggCacheMgr.setTestInstance(aggregates);
    }

    @AfterEach
    void restoreAggregateCache() {
        SimpleAggCacheMgr.clearTestInstance();
        Config.sql_cache_manage_num = sqlCapacity;
        Config.cache_partition_meta_table_manage_num = partitionCapacity;
        NereidsSqlCacheManager.updateConfig();
        NereidsSortedPartitionsCacheManager.updateConfig();
    }

    @Test
    void replayLegacyCreateTableInvalidatesAllTableCaches() throws Exception {
        warmBothTables();
        JsonObject legacy = GsonUtils.GSON.toJsonTree(
                new CreateTableInfo(database.getFullName(), database.getId(), target)).getAsJsonObject();
        for (String field : ImmutableList.of("ctl", "tbl", "dbId")) {
            legacy.remove(field);
        }
        CreateTableInfo info = GsonUtils.GSON.fromJson(legacy, CreateTableInfo.class);
        Assertions.assertNull(info.getCtlName());
        Assertions.assertNull(info.getTblName());
        Assertions.assertEquals(-1L, info.getDbId());
        database.unregisterTable(target.getId());

        replay(OperationType.OP_CREATE_TABLE, info);

        Assertions.assertEquals(target.getId(), database.getTableOrMetaException("target").getId());
        Assertions.assertNotSame(target, database.getTableNullable("target"));
        assertTargetInvalidated();
    }

    @Test
    void replayRenameColumnInvalidatesAllTableCaches() throws Exception {
        warmBothTables();
        long dataVersion = target.getVisibleVersion();
        TableRenameColumnInfo info = new TableRenameColumnInfo(database.getId(), target.getId(), "v", "new_v",
                ImmutableMap.of(target.getBaseIndexId(), target.getBaseSchemaVersion() + 1));
        replay(OperationType.OP_RENAME_COLUMN, info);
        Assertions.assertNull(target.getColumn("v"));
        Assertions.assertNotNull(target.getColumn("new_v"));
        Assertions.assertEquals(dataVersion, target.getVisibleVersion());
        assertTargetInvalidated();
    }

    @Test
    void replayRenameTableRemovesSortedEntryUnderOldName() throws Exception {
        warmBothTables();
        List<String> namesAtNotification = new ArrayList<>();
        aggregates.onInvalidate = () -> namesAtNotification.add(target.getName());
        replay(OperationType.OP_RENAME_TABLE,
                TableInfo.createForTableRename(database.getId(), target.getId(), "target", "renamed"));
        Assertions.assertEquals("renamed", target.getName());
        Assertions.assertEquals(ImmutableList.of("renamed"), namesAtNotification);
        assertTargetInvalidated();
        getSQLPlanOrErrorMsg("SELECT id FROM cache_replay.renamed WHERE id=1");
        Assertions.assertEquals(2, Env.getCurrentEnv().getSortedPartitionsCacheManager()
                .getPartitionCaches().estimatedSize());
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    void replayReplaceTableInvalidatesBothIdentities(boolean swap) throws Exception {
        warmBothTables();
        List<Long> identitiesAtNotification = new ArrayList<>();
        aggregates.onInvalidate = () -> identitiesAtNotification.add(database.getTableNullable("target").getId());
        replay(OperationType.OP_REPLACE_TABLE, new ReplaceTableOperationLog(database.getId(),
                target.getId(), "target", control.getId(), "control", swap, false));
        Assertions.assertSame(control, database.getTableNullable("target"));
        Assertions.assertFalse(identitiesAtNotification.isEmpty());
        Assertions.assertTrue(identitiesAtNotification.stream().allMatch(id -> id == control.getId()));
        if (swap) {
            Assertions.assertSame(target, database.getTableNullable("control"));
        } else {
            Assertions.assertNull(database.getTableNullable("control"));
        }
        Assertions.assertEquals(0, Env.getCurrentEnv().getSqlCacheManager().getSqlCaches().estimatedSize());
        Assertions.assertEquals(0, Env.getCurrentEnv().getSortedPartitionsCacheManager()
                .getPartitionCaches().estimatedSize());
        Assertions.assertTrue(aggregates.invalidated.containsAll(ImmutableList.of(target.getId(), control.getId())));
    }

    @Test
    void replayViewDefinitionInvalidatesQueriesUsingTheView() throws Exception {
        executeNereidsSql("DROP VIEW IF EXISTS cache_replay.cached_view");
        createView("CREATE VIEW cache_replay.cached_view AS SELECT id FROM cache_replay.target");
        View view = (View) database.getTableOrMetaException("cached_view");
        SqlCacheContext context = warmSql("view", target);
        context.addUsedView(view, view.getInlineViewDef());
        warmSql("control", control);
        replay(OperationType.OP_MODIFY_VIEW_DEF, new AlterViewInfo(database.getId(), view.getId(),
                "SELECT v FROM cache_replay.target", view.getFullSchema(), ImmutableMap.of(), ""));
        Assertions.assertNull(Env.getCurrentEnv().getSqlCacheManager().getSqlCaches().getIfPresent("view"));
        Assertions.assertNotNull(Env.getCurrentEnv().getSqlCacheManager().getSqlCaches().getIfPresent("control"));
    }

    @Test
    void visibleReplayDoesNotScanCachesAndValidatesDependenciesOnLookup() throws Exception {
        NereidsSqlCacheManager manager = Env.getCurrentEnv().getSqlCacheManager();
        String first = "SELECT id FROM target PARTITION(p1)";
        String second = "SELECT id FROM target PARTITION(p2)";
        String whole = "SELECT id FROM target";
        String other = "SELECT id FROM control PARTITION(p1)";
        prepareCachedQuery(first, target);
        publishCachedQuery(false, first);
        prepareCachedQuery(second, target, "p2");
        publishCachedQuery(false, second);
        prepareCachedQuery(whole, target, null);
        publishCachedQuery(false, whole);
        prepareCachedQuery(other, control);
        publishCachedQuery(false, other);
        Assertions.assertEquals(4, manager.getSqlCaches().estimatedSize());
        manager.getSqlCaches().asMap().replaceAll((key, cached) -> Mockito.spy(cached));
        List<SqlCacheContext> cachedContexts = new ArrayList<>(manager.getSqlCaches().asMap().values());

        TransactionState transaction = committedTransaction(target, "p2");
        transaction.setTransactionStatus(TransactionStatus.VISIBLE);
        replay(OperationType.OP_UPSERT_TRANSACTION_STATE, transaction);

        for (SqlCacheContext cached : cachedContexts) {
            Mockito.verify(cached, Mockito.never()).getUsedTables();
        }
        Assertions.assertEquals(4, manager.getSqlCaches().estimatedSize(),
                "VISIBLE must publish versions without walking or evicting cached queries under table write locks");
        assertCacheHit(first, true);
        assertCacheHit(second, false);
        assertCacheHit(whole, false);
        assertCacheHit(other, true);
        Assertions.assertEquals(2, manager.getSqlCaches().estimatedSize());
        Assertions.assertTrue(aggregates.invalidated.isEmpty());
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    void transactionReplayChecksDataWhenVisibleButNotWhenCommitted(boolean partitioned) throws Exception {
        OlapTable table = partitioned ? target : (OlapTable) database.getTableOrMetaException("plain");
        String partitionName = partitioned ? "p1" : table.getName();
        String query = "SELECT id FROM " + table.getName();
        prepareCachedQuery(query, table);
        publishCachedQuery(false, query);
        long oldVersion = table.getVisibleVersion();
        TransactionState transaction = committedTransaction(table, partitionName);
        replay(OperationType.OP_UPSERT_TRANSACTION_STATE, transaction);
        Assertions.assertEquals(oldVersion, table.getVisibleVersion());
        assertCacheHit(query, true);

        transaction.setTransactionStatus(TransactionStatus.VISIBLE);
        replay(OperationType.OP_UPSERT_TRANSACTION_STATE, transaction);
        Assertions.assertEquals(oldVersion + 1, table.getVisibleVersion());
        Assertions.assertEquals(1, Env.getCurrentEnv().getSqlCacheManager().getSqlCaches().estimatedSize());
        assertCacheHit(query, false);
        Assertions.assertEquals(0, Env.getCurrentEnv().getSqlCacheManager().getSqlCaches().estimatedSize());
        Assertions.assertTrue(aggregates.invalidated.isEmpty());
    }

    private TransactionState committedTransaction(OlapTable table, String partitionName) throws Exception {
        long now = System.currentTimeMillis();
        TransactionState transaction = new TransactionState(database.getId(), ImmutableList.of(table.getId()),
                Env.getCurrentEnv().getNextId(), "cache_replay_visible", null,
                TransactionState.LoadJobSourceType.FRONTEND,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, 0, "127.0.0.1", now),
                -1, 10000);
        TableCommitInfo commit = new TableCommitInfo(table.getId());
        commit.setVersion(table.getVisibleVersion() + 1);
        commit.setVersionTime(now);
        Partition partition = table.getPartition(partitionName);
        commit.addPartitionCommitInfo(new PartitionCommitInfo(partition.getId(), "",
                partition.getVisibleVersion() + 1, now, false));
        transaction.putIdToTableCommitInfo(table.getId(), commit);
        transaction.setCommitTime(now);
        transaction.setFinishTime(now);
        transaction.setTransactionStatus(TransactionStatus.COMMITTED);
        return transaction;
    }

    private void assertCacheHit(String query, boolean expected) {
        try (StatementContext lookup = new StatementContext(connectContext, new OriginStatement(query, 0))) {
            connectContext.setStatementContext(lookup);
            Assertions.assertEquals(expected,
                    Env.getCurrentEnv().getSqlCacheManager().tryParseSql(connectContext, query).isPresent(), query);
        }
    }

    @Test
    void nonPartitionedCacheChecksSchemaAndChangesBackendKey() throws Exception {
        OlapTable table = (OlapTable) database.getTableOrMetaException("plain");
        String query = "SELECT * FROM plain";
        SqlCacheContext cached = prepareCachedQuery(query, table);
        Env env = Env.getCurrentEnv();
        env.getSqlCacheManager().tryAddFeSqlCache(connectContext, query);
        String oldKey = cached.getUsedTables().values().iterator().next().getCacheKey();
        try (StatementContext lookup = new StatementContext(connectContext, new OriginStatement(query, 0))) {
            connectContext.setStatementContext(lookup);
            Assertions.assertTrue(env.getSqlCacheManager().tryParseSql(connectContext, query).isPresent());
        }
        // Model a plan published after the eager invalidation has already run.
        table.getIndexMetaByIndexId(table.getBaseIndexId()).setSchemaVersion(table.getBaseSchemaVersion() + 1);
        SqlCacheContext current = new SqlCacheContext(UserIdentity.ROOT);
        current.addUsedTable(table);
        Assertions.assertNotEquals(oldKey, current.getUsedTables().values().iterator().next().getCacheKey());
        try (StatementContext lookup = new StatementContext(connectContext, new OriginStatement(query, 0))) {
            connectContext.setStatementContext(lookup);
            Assertions.assertFalse(env.getSqlCacheManager().tryParseSql(connectContext, query).isPresent());
        }
        Assertions.assertEquals(0, env.getSqlCacheManager().getSqlCaches().estimatedSize());
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void metadataReplayRejectsInFlightPublication(boolean rename, boolean backend) throws Exception {
        target.getPartition("p1").updateVisibleVersionAndTime(3L, 123456L);
        String query = "SELECT id FROM target PARTITION(p1)";
        SqlCacheContext cached = prepareCachedQuery(query, target);
        StatementContext statement = connectContext.getStatementContext();
        SqlCacheContext unrelated = prepareCachedQuery("SELECT id FROM control", control);
        connectContext.setStatementContext(statement);
        NereidsSqlCacheManager manager = Env.getCurrentEnv().getSqlCacheManager();
        long tableVersion = target.getVisibleVersion();
        long topologyVersion = target.getPartitionTopologyVersion();
        int schemaVersion = target.getBaseSchemaVersion();

        replayPartitionMetadataChange(rename);

        Assertions.assertEquals(tableVersion, target.getVisibleVersion());
        Assertions.assertEquals(topologyVersion, target.getPartitionTopologyVersion());
        Assertions.assertEquals(schemaVersion, target.getBaseSchemaVersion());
        publishCachedQuery(backend, query);
        Assertions.assertEquals(0, manager.getSqlCaches().estimatedSize(),
                "an executing query must not republish its old metadata dependencies");
        Assertions.assertTrue(cached.supportSqlCache());
        Assertions.assertFalse(cached.metadataVersionsMatch());
        Assertions.assertTrue(unrelated.metadataVersionsMatch());
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    void metadataReplayRejectsContextPutAfterEviction(boolean rename) throws Exception {
        target.getPartition("p1").updateVisibleVersionAndTime(3L, 123456L);
        String query = "SELECT id FROM target PARTITION(p1)";
        SqlCacheContext cached = prepareCachedQuery(query, target);
        NereidsSqlCacheManager manager = Env.getCurrentEnv().getSqlCacheManager();
        manager.tryAddFeSqlCache(connectContext, query);
        String key = manager.getSqlCaches().asMap().keySet().iterator().next();

        replayPartitionMetadataChange(rename);
        Assertions.assertEquals(0, manager.getSqlCaches().estimatedSize());
        // Model invalidation between the publisher's final check and its put.
        manager.getSqlCaches().put(key, cached);

        try (StatementContext lookup = new StatementContext(connectContext, new OriginStatement(query, 0))) {
            connectContext.setStatementContext(lookup);
            Assertions.assertFalse(manager.tryParseSql(connectContext, query).isPresent(),
                    "a late put must not make an invalidated context usable again");
        }
        Assertions.assertEquals(0, manager.getSqlCaches().estimatedSize());
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    void publicationValidatesDataWithoutAnInvalidationNotification(boolean backend) throws Exception {
        target.getPartition("p1").updateVisibleVersionAndTime(3L, 123456L);
        String query = "SELECT id FROM target PARTITION(p1)";
        SqlCacheContext cached = prepareCachedQuery(query, target);
        target.getPartition("p1").updateVisibleVersionAndTime(4L, 123457L);
        target.updateVisibleVersionAndTime(target.getVisibleVersion() + 1, 123457L);
        Assertions.assertTrue(cached.metadataVersionsMatch());
        NereidsSqlCacheManager manager = Env.getCurrentEnv().getSqlCacheManager();
        publishCachedQuery(backend, query);
        Assertions.assertEquals(0, manager.getSqlCaches().estimatedSize());
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void publicationPreservesUnchangedPartitionDependencies(boolean backend, boolean writeOtherPartition)
            throws Exception {
        String query = "SELECT id FROM target PARTITION(p1)";
        SqlCacheContext cached = prepareCachedQuery(query, target);
        if (writeOtherPartition) {
            target.writeLock();
            try {
                Partition p2 = target.getPartition("p2");
                p2.updateVisibleVersionAndTime(p2.getVisibleVersion() + 1, 123457L);
                target.updateVisibleVersionAndTime(target.getVisibleVersion() + 1, 123457L);
            } finally {
                target.writeUnlock();
            }
        }
        publishCachedQuery(backend, query);
        Assertions.assertTrue(cached.metadataVersionsMatch());
        Assertions.assertEquals(1, Env.getCurrentEnv().getSqlCacheManager().getSqlCaches().estimatedSize());
    }

    private void publishCachedQuery(boolean backend, String query) {
        NereidsSqlCacheManager manager = Env.getCurrentEnv().getSqlCacheManager();
        if (backend) {
            CacheAnalyzer analyzer = Mockito.mock(CacheAnalyzer.class);
            Mockito.when(analyzer.getCache()).thenReturn(Mockito.mock(SqlCache.class));
            Mockito.when(analyzer.getScanTables()).thenReturn(ImmutableList.of());
            manager.tryAddBeCache(connectContext, query, analyzer);
        } else {
            manager.tryAddFeSqlCache(connectContext, query);
        }
    }

    private void replayPartitionMetadataChange(boolean rename) throws Exception {
        Partition partition = target.getPartition("p1");
        if (rename) {
            replay(OperationType.OP_RENAME_PARTITION, TableInfo.createForPartitionRename(
                    database.getId(), target.getId(), partition.getId(), "p1", "renamed_partition"));
        } else {
            replay(OperationType.OP_SET_PARTITION_VERSION, new SetPartitionVersionOperationLog(
                    database.getFullName(), target.getName(), partition.getId(), 2L));
        }
    }

    @ParameterizedTest
    @CsvSource({"database", "catalog", "all"})
    void namespaceInvalidationReachesExecutingQueries(String scope) {
        SqlCacheContext cached = prepareCachedQuery("SELECT id FROM target", target);
        StatementContext statement = connectContext.getStatementContext();
        // A constant statement has no table dependencies.
        StatementContext constantStatement = new StatementContext(connectContext, new OriginStatement("SELECT 1", 0));
        SqlCacheContext constant = constantStatement.getSqlCacheContext().get();
        connectContext.setStatementContext(statement);
        NereidsSqlCacheManager manager = Env.getCurrentEnv().getSqlCacheManager();
        // Replacing the result-cache container must preserve dependency revisions.
        NereidsSqlCacheManager.updateConfig();
        switch (scope) {
            case "database":
                Env.getCurrentEnv().onDatabaseChanged(InternalCatalog.INTERNAL_CATALOG_NAME, "cache_replay");
                break;
            case "catalog":
                Env.getCurrentEnv().onCatalogChanged(InternalCatalog.INTERNAL_CATALOG_NAME);
                break;
            case "all":
                manager.invalidateAll();
                break;
            default:
                throw new AssertionError(scope);
        }
        manager.tryAddFeSqlCache(connectContext, "SELECT id FROM target");
        Assertions.assertEquals(0, manager.getSqlCaches().estimatedSize());
        Assertions.assertFalse(cached.metadataVersionsMatch());
        Assertions.assertEquals(!scope.equals("all"), constant.metadataVersionsMatch());
    }

    private SqlCacheContext prepareCachedQuery(String query, OlapTable table) {
        return prepareCachedQuery(query, table, "p1");
    }

    private SqlCacheContext prepareCachedQuery(String query, OlapTable table, String partitionName) {
        StatementContext statement = new StatementContext(connectContext, new OriginStatement(query, 0));
        connectContext.setStatementContext(statement);
        SqlCacheContext cached = statement.getSqlCacheContext().get();
        cached.addUsedTable(table);
        if (table.isPartitionedTable() && partitionName != null) {
            Partition partition = table.getPartition(partitionName);
            cached.addOlapScan(table, ImmutableMap.of(partition.getId(), partition.getCachedVisibleVersion()));
            ScanTable scan = new ScanTable(new FullTableName(
                    InternalCatalog.INTERNAL_CATALOG_NAME, database.getFullName(), table.getName()));
            scan.addScanPartition(partition.getId());
            cached.addScanTable(scan, table);
        }
        cached.setPhysicalPlan("cached plan");
        cached.setColLabels(ImmutableList.of("id", "v"));
        cached.setFieldInfos(ImmutableList.of());
        cached.setResultExprs(ImmutableList.of());
        cached.setQueryId(new TUniqueId(1, 1));
        cached.setResultSetInFe(new CommonResultSet(
                new CommonResultSet.CommonResultSetMetaData(ImmutableList.of()), ImmutableList.of()));
        connectContext.setQueryId(new TUniqueId(1, 1));
        return cached;
    }

    @Test
    void databaseInvalidationPreservesOtherNamespaces() {
        NereidsSqlCacheManager manager = Env.getCurrentEnv().getSqlCacheManager();
        warmSql("target", target);
        SqlCacheContext other = new SqlCacheContext(UserIdentity.ROOT);
        manager.getSqlCaches().put("constant", other);
        Env.getCurrentEnv().onDatabaseChanged(InternalCatalog.INTERNAL_CATALOG_NAME, "cache_replay");
        Assertions.assertNull(manager.getSqlCaches().getIfPresent("target"));
        Assertions.assertNotNull(manager.getSqlCaches().getIfPresent("constant"));
    }

    @Test
    void invalidationFailuresAreIsolatedPerCache() {
        NereidsSqlCacheManager sql = Mockito.mock(NereidsSqlCacheManager.class);
        NereidsSortedPartitionsCacheManager sorted = Mockito.mock(NereidsSortedPartitionsCacheManager.class);
        MTMVService mtmv = Mockito.mock(MTMVService.class);
        MTMVRelationManager relations = Mockito.mock(MTMVRelationManager.class);
        Env env = Mockito.mock(Env.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(env.getMtmvService()).thenReturn(mtmv);
        Mockito.when(mtmv.getRelationManager()).thenReturn(relations);
        Mockito.when(env.getSqlCacheManager()).thenReturn(sql);
        Mockito.when(env.getSortedPartitionsCacheManager()).thenReturn(sorted);
        Mockito.doThrow(new IllegalStateException("SQL invalidation failed"))
                .when(sql).invalidateAboutTable(Mockito.eq(target.getId()), Mockito.any(FullTableName.class));
        Mockito.doThrow(new IllegalStateException("partition invalidation failed"))
                .when(sorted).invalidateTable(target.getId());
        Mockito.doThrow(new AssertionError("MV invalidation failed")).when(relations)
                .invalidateRewriteCaches(InternalCatalog.INTERNAL_CATALOG_NAME, "cache_replay", "target");
        Assertions.assertDoesNotThrow(() -> env.onTableChanged(target));
        Mockito.verify(sorted).invalidateTable(target.getId());
        Mockito.verify(relations).invalidateRewriteCaches(
                InternalCatalog.INTERNAL_CATALOG_NAME, "cache_replay", "target");
        Assertions.assertTrue(aggregates.invalidated.contains(target.getId()));
    }

    private SqlCacheContext warmSql(String key, OlapTable table) {
        SqlCacheContext context = new SqlCacheContext(UserIdentity.ROOT,
                Env.getCurrentEnv().getSqlCacheManager().newMetadataSnapshot());
        context.addUsedTable(table);
        Env.getCurrentEnv().getSqlCacheManager().getSqlCaches().put(key, context);
        return context;
    }

    private void warmBothTables() throws Exception {
        getSQLPlanOrErrorMsg("SELECT id FROM cache_replay.target WHERE id=1");
        getSQLPlanOrErrorMsg("SELECT id FROM cache_replay.control WHERE id=1");
        warmSql("target", target);
        warmSql("control", control);
        Assertions.assertEquals(2, Env.getCurrentEnv().getSortedPartitionsCacheManager()
                .getPartitionCaches().estimatedSize());
    }

    private void assertTargetInvalidated() {
        NereidsSqlCacheManager manager = Env.getCurrentEnv().getSqlCacheManager();
        Assertions.assertNull(manager.getSqlCaches().getIfPresent("target"));
        Assertions.assertNotNull(manager.getSqlCaches().getIfPresent("control"));
        Assertions.assertEquals(1, Env.getCurrentEnv().getSortedPartitionsCacheManager()
                .getPartitionCaches().estimatedSize());
        Assertions.assertTrue(aggregates.invalidated.contains(target.getId()));
        Assertions.assertFalse(aggregates.invalidated.contains(control.getId()));
    }

    private void replay(short operation, Writable payload) throws Exception {
        JournalEntity written = new JournalEntity();
        written.setOpCode(operation);
        written.setData(payload);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        written.write(new DataOutputStream(bytes));
        JournalEntity read = new JournalEntity();
        read.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        EditLog.loadJournal(Env.getCurrentEnv(), 1L, read);
    }

    private static class RecordingSimpleAggCache extends SimpleAggCacheMgr {
        private final Set<Long> invalidated = new HashSet<>();
        private Runnable onInvalidate = () -> { };

        @Override
        public void invalidateTable(long tableId) {
            invalidated.add(tableId);
            onInvalidate.run();
        }
    }
}

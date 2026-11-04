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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.stats.SimpleAggCacheMgr;
import org.apache.doris.persist.AlterViewInfo;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.ReplaceTableOperationLog;
import org.apache.doris.persist.TableInfo;
import org.apache.doris.persist.TableRenameColumnInfo;
import org.apache.doris.persist.gson.GsonUtils;
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
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashSet;
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
        replay(OperationType.OP_RENAME_TABLE,
                TableInfo.createForTableRename(database.getId(), target.getId(), "target", "renamed"));
        Assertions.assertEquals("renamed", target.getName());
        assertTargetInvalidated();
        getSQLPlanOrErrorMsg("SELECT id FROM cache_replay.renamed WHERE id=1");
        Assertions.assertEquals(2, Env.getCurrentEnv().getSortedPartitionsCacheManager()
                .getPartitionCaches().estimatedSize());
    }

    @Test
    void replayReplaceTableInvalidatesBothIdentities() throws Exception {
        warmBothTables();
        replay(OperationType.OP_REPLACE_TABLE, new ReplaceTableOperationLog(database.getId(),
                target.getId(), "target", control.getId(), "control", false, false));
        Assertions.assertSame(control, database.getTableNullable("target"));
        Assertions.assertNull(database.getTableNullable("control"));
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
    void visibleDataInvalidatesOnlyChangedPartitionDependencies() throws Exception {
        Partition p1 = target.getPartition("p1");
        Partition p2 = target.getPartition("p2");
        SqlCacheContext first = warmSql("p1", target);
        first.addOlapScan(target, ImmutableMap.of(p1.getId(), p1.getVisibleVersion()));
        SqlCacheContext second = warmSql("p2", target);
        second.addOlapScan(target, ImmutableMap.of(p2.getId(), p2.getVisibleVersion()));
        warmSql("whole_table", target);
        warmSql("control", control);
        target.writeLock();
        try {
            p2.updateVisibleVersionAndTime(p2.getVisibleVersion() + 1, 123456L);
            target.updateVisibleVersionAndTime(target.getVisibleVersion() + 1, 123456L);
            Env.getCurrentEnv().onTableChanged(target, Env.TableChangeType.DATA);
        } finally {
            target.writeUnlock();
        }
        NereidsSqlCacheManager manager = Env.getCurrentEnv().getSqlCacheManager();
        Assertions.assertNotNull(manager.getSqlCaches().getIfPresent("p1"));
        Assertions.assertNull(manager.getSqlCaches().getIfPresent("p2"));
        Assertions.assertNull(manager.getSqlCaches().getIfPresent("whole_table"));
        Assertions.assertNotNull(manager.getSqlCaches().getIfPresent("control"));
        Assertions.assertTrue(aggregates.invalidated.isEmpty());
    }

    @Test
    void transactionReplayInvalidatesWhenVisibleButNotWhenCommitted() throws Exception {
        warmBothTables();
        long oldVersion = target.getVisibleVersion();
        long now = System.currentTimeMillis();
        TransactionState transaction = new TransactionState(database.getId(), ImmutableList.of(target.getId()),
                Env.getCurrentEnv().getNextId(), "cache_replay_visible", null,
                TransactionState.LoadJobSourceType.FRONTEND,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, 0, "127.0.0.1", now),
                -1, 10000);
        TableCommitInfo table = new TableCommitInfo(target.getId());
        table.setVersion(oldVersion + 1);
        table.setVersionTime(now);
        Partition partition = target.getPartition("p1");
        table.addPartitionCommitInfo(new PartitionCommitInfo(partition.getId(), "",
                partition.getVisibleVersion() + 1, now, false));
        transaction.putIdToTableCommitInfo(target.getId(), table);
        transaction.setCommitTime(now);
        transaction.setTransactionStatus(TransactionStatus.COMMITTED);
        replay(OperationType.OP_UPSERT_TRANSACTION_STATE, transaction);
        Assertions.assertEquals(oldVersion, target.getVisibleVersion());
        Assertions.assertNotNull(Env.getCurrentEnv().getSqlCacheManager().getSqlCaches().getIfPresent("target"));
        transaction.setFinishTime(now);
        transaction.setTransactionStatus(TransactionStatus.VISIBLE);
        replay(OperationType.OP_UPSERT_TRANSACTION_STATE, transaction);
        Assertions.assertEquals(oldVersion + 1, target.getVisibleVersion());
        Assertions.assertNull(Env.getCurrentEnv().getSqlCacheManager().getSqlCaches().getIfPresent("target"));
        Assertions.assertNotNull(Env.getCurrentEnv().getSqlCacheManager().getSqlCaches().getIfPresent("control"));
        Assertions.assertEquals(2, Env.getCurrentEnv().getSortedPartitionsCacheManager()
                .getPartitionCaches().estimatedSize());
        Assertions.assertTrue(aggregates.invalidated.isEmpty());
    }

    @Test
    void nonPartitionedCacheChecksSchemaAndChangesBackendKey() throws Exception {
        OlapTable table = (OlapTable) database.getTableOrMetaException("plain");
        String query = "SELECT * FROM plain";
        StatementContext statement = new StatementContext(connectContext, new OriginStatement(query, 0));
        connectContext.setStatementContext(statement);
        SqlCacheContext cached = statement.getSqlCacheContext().get();
        cached.addUsedTable(table);
        cached.setPhysicalPlan("cached plan");
        cached.setColLabels(ImmutableList.of("id", "v"));
        cached.setFieldInfos(ImmutableList.of());
        cached.setResultExprs(ImmutableList.of());
        cached.setQueryId(new TUniqueId(1, 1));
        cached.setResultSetInFe(new CommonResultSet(
                new CommonResultSet.CommonResultSetMetaData(ImmutableList.of()), ImmutableList.of()));
        connectContext.setQueryId(new TUniqueId(1, 1));
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
        SqlCacheContext context = new SqlCacheContext(UserIdentity.ROOT);
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

        @Override
        public void invalidateTable(long tableId) {
            invalidated.add(tableId);
        }
    }
}

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

package org.apache.doris.alter;

import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.alter.CloudSchemaChangeHandler;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyTablePropertiesOp;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.ModifyTablePropertyOperationLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.CloneTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TCloneReq;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RowBinlogConfigPublicationTest {
    private Database db;
    private OlapTable table;
    private EditLog editLog;
    private MockedStatic<Env> envMock;
    private final List<BinlogConfig> journal = new ArrayList<>();
    private final Map<String, BinlogConfig> applied = new HashMap<>();
    private final List<Set<Long>> acknowledgedReplicaIds = new ArrayList<>();
    private RecordingHandler handler;

    @BeforeEach
    public void setUp() throws Exception {
        db = Mockito.mock(Database.class);
        Mockito.when(db.getBinlogConfig()).thenReturn(new BinlogConfig());
        table = new OlapTable(2, "tbl", Collections.emptyList(), KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(1));
        table.setBinlogConfig(new BinlogConfig(true, 86400, 1024, 10, BinlogConfig.BinlogFormat.ROW, false));
        table.addPartition(partition(3, "p1"));
        table.addPartition(partition(4, "p2"));
        Mockito.when(db.getTableOrMetaException("tbl", Table.TableType.OLAP)).thenReturn(table);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSqlCacheManager()).thenReturn(Mockito.mock(NereidsSqlCacheManager.class));
        editLog = Mockito.mock(EditLog.class);
        Field field = Env.class.getDeclaredField("editLog");
        field.setAccessible(true);
        field.set(env, editLog);
        // Exercise the real catalog mutation and EditLog call, mocking only durable IO.
        Mockito.doCallRealMethod().when(env).updateBinlogConfig(Mockito.eq(db), Mockito.eq(table), Mockito.any());
        Mockito.when(editLog.logUpdateBinlogConfig(Mockito.any())).thenAnswer(invocation -> {
            ModifyTablePropertyOperationLog log = invocation.getArgument(0);
            journal.add(BinlogConfig.fromProperties(log.getProperties()));
            return (long) journal.size();
        });
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        SystemInfoService info = Mockito.mock(SystemInfoService.class);
        Mockito.when(info.getAllBackendsByAllCluster()).thenReturn(ImmutableMap.of());
        envMock.when(Env::getCurrentSystemInfo).thenReturn(info);
        handler = new RecordingHandler();
    }

    @AfterEach
    public void tearDown() {
        envMock.close();
    }

    private Partition partition(long id, String name) {
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getId()).thenReturn(id);
        Mockito.when(partition.getName()).thenReturn(name);
        return partition;
    }

    private void alter(SchemaChangeHandler target, long ttl) throws UserException {
        target.updateBinlogConfig(db, table, Collections.singletonList(new ModifyTablePropertiesOp(
                ImmutableMap.of(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS, Long.toString(ttl)))));
    }

    private BinlogConfig published() {
        return journal.get(journal.size() - 1);
    }

    private class RecordingHandler extends SchemaChangeHandler {
        int failAfter = Integer.MAX_VALUE;
        int calls;
        Runnable duringUpdate = () -> { };

        @Override
        protected void updatePartitionProperties(Database database, OlapTable targetTable, Partition partition,
                long storagePolicyId, int inMemory, BinlogConfig config, String policy, Map<String, Long> timeSeries,
                int skipIndex, int disableCompaction, int verticalColumns) throws UserException {
            Assertions.assertFalse(table.isWriteLockHeldByCurrentThread(), "RPC must run outside the table lock");
            Assertions.assertEquals(config.getConfigVersion(), published().getConfigVersion());
            Assertions.assertTrue(published().getTtlSeconds() <= config.getTtlSeconds());
            if (calls++ == failAfter) {
                throw new DdlException("injected partition failure");
            }
            applied.put(Long.toString(partition.getId()), new BinlogConfig(config));
            Set<Long> replicaIds = new HashSet<>();
            for (MaterializedIndex index : partition.getMaterializedIndices(
                    MaterializedIndex.IndexExtState.VISIBLE, true)) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replicaIds.add(replica.getId());
                    }
                }
            }
            acknowledgedReplicaIds.add(replicaIds);
            duringUpdate.run();
        }
    }

    private List<Replica> installTopology(long indexId, long tabletId, Replica replica) {
        List<Replica> replicas = new ArrayList<>(Collections.singletonList(replica));
        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        Mockito.when(index.getId()).thenReturn(indexId);
        Tablet tablet = Mockito.mock(Tablet.class);
        Mockito.when(tablet.getId()).thenReturn(tabletId);
        Mockito.when(tablet.getReplicas()).thenReturn(replicas);
        Mockito.when(index.getTablets()).thenReturn(Collections.singletonList(tablet));
        Mockito.when(table.getPartition(3L).getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE, true))
                .thenReturn(Collections.singletonList(index));
        return replicas;
    }

    private Replica replica(long replicaId, long backendId, ReplicaState state) {
        return new LocalReplica(replicaId, backendId, 10L, 100, 100L, 0L, 1L, state, -1L, 10L);
    }

    private void changeTopologyAfterFirstRound(Runnable change) {
        int partitionsInRound = table.getAllPartitions().size();
        handler.duringUpdate = () -> {
            Assertions.assertEquals(60, table.getBinlogConfig().getTtlSeconds());
            Assertions.assertEquals(3600, table.getBinlogConfig().getPendingTtlSeconds());
            if (handler.calls == partitionsInRound) {
                table.writeLock();
                try {
                    change.run();
                } finally {
                    table.writeUnlock();
                }
            }
        };
    }

    private void replayConfig(BinlogConfig config) throws Exception {
        Env replayEnv = Mockito.mock(Env.class);
        Mockito.when(replayEnv.getSqlCacheManager()).thenReturn(Mockito.mock(NereidsSqlCacheManager.class));
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(replayEnv.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbOrMetaException(db.getId())).thenReturn(db);
        Mockito.when(db.getTableOrMetaException(table.getId(), Table.TableType.OLAP)).thenReturn(table);
        ModifyTablePropertyOperationLog log = new ModifyTablePropertyOperationLog(
                db.getId(), table.getId(), table.getName(), config.toProperties());
        Mockito.doCallRealMethod().when(replayEnv).replayModifyTableProperty(
                OperationType.OP_UPDATE_BINLOG_CONFIG, log);
        replayEnv.replayModifyTableProperty(OperationType.OP_UPDATE_BINLOG_CONFIG, log);
    }

    @Test
    public void shrinkIsJournaledBeforeAnyTabletAndIdenticalRetryRepairsPartialFailure() throws Exception {
        handler.failAfter = 1;
        Assertions.assertThrows(DdlException.class, () -> alter(handler, 60));
        Assertions.assertEquals(60, published().getTtlSeconds());
        Assertions.assertEquals(1, applied.size());
        long failedVersion = published().getConfigVersion();
        // Restore the state that a new master would replay, then retry the same SQL.
        table.setBinlogConfig(BinlogConfig.fromProperties(published().toProperties()));
        handler.failAfter = Integer.MAX_VALUE;
        alter(handler, 60);
        Assertions.assertEquals(2, applied.size());
        Assertions.assertTrue(published().getConfigVersion() > failedVersion);
        applied.values().forEach(config -> Assertions.assertEquals(published(), config));
    }

    @Test
    public void extensionIsNotPublishedUntilAllTabletsAcceptIt() throws Exception {
        table.getBinlogConfig().setTtlSeconds(60);
        handler.failAfter = 1;
        Assertions.assertThrows(DdlException.class, () -> alter(handler, 3600));
        Assertions.assertEquals(60, published().getTtlSeconds());
        Assertions.assertEquals(60, table.getBinlogConfig().getTtlSeconds());
        handler.failAfter = Integer.MAX_VALUE;
        alter(handler, 3600);
        Assertions.assertEquals(3600, published().getTtlSeconds());
        applied.values().forEach(config -> Assertions.assertEquals(published(), config));
    }

    @Test
    public void failedExtensionReplayPreservesCloneTargetAndCompletionClearsPending() throws Exception {
        table.getBinlogConfig().setTtlSeconds(60);
        BinlogConfig original = new BinlogConfig(table.getBinlogConfig());
        handler.failAfter = 1;
        Assertions.assertThrows(DdlException.class, () -> alter(handler, 3600));
        BinlogConfig pending = new BinlogConfig(published());
        Assertions.assertEquals(60, pending.getTtlSeconds());

        table.setBinlogConfig(original);
        replayConfig(pending);
        Assertions.assertEquals(pending, table.getBinlogConfig());
        TBackend backend = new TBackend("127.0.0.1", 9050, 8040);
        CloneTask task = new CloneTask(backend, 10L, db.getId(), table.getId(), 3L, 4L, 5L, 6L,
                100, Collections.singletonList(backend), TStorageMedium.HDD, 10L, 60, table.getBinlogConfig());
        TCloneReq request = task.toThrift();
        Assertions.assertEquals(3600, request.getBinlogConfig().getTtlSeconds());
        Assertions.assertEquals(pending.getConfigVersion(), request.getBinlogConfig().getConfigVersion());
        Assertions.assertEquals(60, table.getBinlogConfig().getTtlSeconds());

        handler.failAfter = Integer.MAX_VALUE;
        alter(handler, 3600);
        BinlogConfig completed = new BinlogConfig(published());
        Assertions.assertEquals(3600, completed.getTtlSeconds());
        Assertions.assertEquals(0, completed.getPendingTtlSeconds());
        table.setBinlogConfig(pending);
        replayConfig(completed);
        Assertions.assertEquals(completed, table.getBinlogConfig());
        Assertions.assertEquals(completed, table.getTableProperty().buildBinlogConfig().getBinlogConfig());
    }

    @Test
    public void cloneAddedDuringAlterReceivesAnotherAckBeforeLongerTtlIsPublished() throws Exception {
        table.getBinlogConfig().setTtlSeconds(60);
        List<Replica> replicas = installTopology(7L, 8L, replica(9L, 10L, ReplicaState.NORMAL));
        changeTopologyAfterFirstRound(() -> replicas.add(replica(11L, 12L, ReplicaState.CLONE)));

        alter(handler, 3600);

        Assertions.assertEquals(4, handler.calls);
        Assertions.assertTrue(acknowledgedReplicaIds.subList(0, 2).stream().noneMatch(ids -> ids.contains(11L)));
        Assertions.assertTrue(acknowledgedReplicaIds.subList(2, 4).stream().anyMatch(ids -> ids.contains(11L)));
        Assertions.assertEquals(2, journal.size());
        Assertions.assertEquals(3600, published().getTtlSeconds());
        Assertions.assertEquals(0, published().getPendingTtlSeconds());
    }

    @Test
    public void replacementReplicaOnSameBackendReceivesAnotherAck() throws Exception {
        table.getBinlogConfig().setTtlSeconds(60);
        List<Replica> replicas = installTopology(7L, 8L, replica(9L, 10L, ReplicaState.NORMAL));
        changeTopologyAfterFirstRound(() -> replicas.set(0, replica(11L, 10L, ReplicaState.CLONE)));

        alter(handler, 3600);

        Assertions.assertEquals(4, handler.calls);
        Assertions.assertTrue(acknowledgedReplicaIds.subList(0, 2).stream().anyMatch(ids -> ids.contains(9L)));
        Assertions.assertTrue(acknowledgedReplicaIds.subList(2, 4).stream().noneMatch(ids -> ids.contains(9L)));
        Assertions.assertTrue(acknowledgedReplicaIds.subList(2, 4).stream().anyMatch(ids -> ids.contains(11L)));
        Assertions.assertEquals(3600, published().getTtlSeconds());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void replacedIndexOrTabletReceivesAnotherAck(boolean replaceIndex) throws Exception {
        table.getBinlogConfig().setTtlSeconds(60);
        Replica replica = replica(9L, 10L, ReplicaState.NORMAL);
        installTopology(7L, 8L, replica);
        changeTopologyAfterFirstRound(() -> installTopology(replaceIndex ? 17L : 7L,
                replaceIndex ? 8L : 18L, replica));

        alter(handler, 3600);

        Assertions.assertEquals(4, handler.calls);
        Assertions.assertEquals(3600, published().getTtlSeconds());
        Assertions.assertEquals(0, published().getPendingTtlSeconds());
    }

    @Test
    public void topologyTimeoutDoesNotPublishAnUnacknowledgedExtension() {
        table.getBinlogConfig().setTtlSeconds(60);
        List<Replica> replicas = installTopology(7L, 8L, replica(9L, 10L, ReplicaState.NORMAL));
        changeTopologyAfterFirstRound(() -> replicas.add(replica(11L, 12L, ReplicaState.CLONE)));
        int previousTimeout = Config.max_create_table_timeout_second;
        Config.max_create_table_timeout_second = 0;
        try {
            DdlException error = Assertions.assertThrows(DdlException.class, () -> alter(handler, 3600));
            Assertions.assertTrue(error.getMessage().contains("Replicas changed"));
            Assertions.assertEquals(2, handler.calls);
            Assertions.assertEquals(1, journal.size());
            Assertions.assertEquals(60, table.getBinlogConfig().getTtlSeconds());
            Assertions.assertEquals(3600, published().getPendingTtlSeconds());
        } finally {
            Config.max_create_table_timeout_second = previousTimeout;
        }
    }

    @Test
    public void additionalAckFailureStillFailsAlterAndKeepsPendingTarget() {
        table.getBinlogConfig().setTtlSeconds(60);
        List<Replica> replicas = installTopology(7L, 8L, replica(9L, 10L, ReplicaState.NORMAL));
        changeTopologyAfterFirstRound(() -> replicas.add(replica(11L, 12L, ReplicaState.CLONE)));
        handler.failAfter = 2;

        DdlException error = Assertions.assertThrows(DdlException.class, () -> alter(handler, 3600));

        Assertions.assertEquals("injected partition failure", error.getDetailMessage());
        Assertions.assertEquals(3, handler.calls);
        Assertions.assertEquals(1, journal.size());
        Assertions.assertEquals(60, table.getBinlogConfig().getTtlSeconds());
        Assertions.assertEquals(3600, published().getPendingTtlSeconds());
    }

    @Test
    public void newerJournaledGenerationDuringAdditionalAckCannotBeOverwritten() {
        table.getBinlogConfig().setTtlSeconds(60);
        List<Replica> replicas = installTopology(7L, 8L, replica(9L, 10L, ReplicaState.NORMAL));
        handler.duringUpdate = () -> {
            table.writeLock();
            try {
                if (handler.calls == 2) {
                    replicas.add(replica(11L, 12L, ReplicaState.CLONE));
                }
                if (handler.calls == 4) {
                    BinlogConfig newer = new BinlogConfig(table.getBinlogConfig());
                    newer.setConfigVersion(newer.getConfigVersion() + 1);
                    newer.setTtlSeconds(30);
                    newer.setPendingTtlSeconds(0);
                    Env.getCurrentEnv().updateBinlogConfig(db, table, newer);
                }
            } finally {
                table.writeUnlock();
            }
        };

        DdlException error = Assertions.assertThrows(DdlException.class, () -> alter(handler, 3600));

        Assertions.assertTrue(error.getMessage().contains("changed concurrently"));
        Assertions.assertEquals(4, handler.calls);
        Assertions.assertEquals(2, journal.size());
        Assertions.assertEquals(2, published().getConfigVersion());
        Assertions.assertEquals(30, table.getBinlogConfig().getTtlSeconds());
        Assertions.assertEquals(published(), table.getBinlogConfig());
    }

    @Test
    public void failedPublicationCanBeReplayedAndRetriedByANewMaster() throws Exception {
        table.getBinlogConfig().setTtlSeconds(0); // Legacy ROW table with cached incremental reads.
        BinlogConfig original = new BinlogConfig(table.getBinlogConfig());
        handler.failAfter = 1;
        Assertions.assertThrows(DdlException.class, () -> alter(handler, 60));
        BinlogConfig committed = new BinlogConfig(published());
        table.setBinlogConfig(original);
        Env replayEnv = Mockito.mock(Env.class);
        NereidsSqlCacheManager cache = new NereidsSqlCacheManager();
        SqlCacheContext cachedRead = Mockito.mock(SqlCacheContext.class);
        SqlCacheContext.TableVersion version = new SqlCacheContext.TableVersion(table.getId(), 1, Table.TableType.OLAP);
        Mockito.when(cachedRead.getUsedTables()).thenReturn(ImmutableMap.of(
                new SqlCacheContext.FullTableName("internal", "db", "tbl"), version));
        cache.getSqlCaches().put("cached-incr-read", cachedRead);
        Mockito.when(replayEnv.getSqlCacheManager()).thenReturn(cache);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        envMock.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        Mockito.when(replayEnv.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbOrMetaException(db.getId())).thenReturn(db);
        Mockito.when(db.getTableOrMetaException(table.getId(), Table.TableType.OLAP)).thenReturn(table);
        ModifyTablePropertyOperationLog log = new ModifyTablePropertyOperationLog(
                db.getId(), table.getId(), table.getName(), committed.toProperties());
        Mockito.doCallRealMethod().when(replayEnv).replayModifyTableProperty(
                OperationType.OP_UPDATE_BINLOG_CONFIG, log);
        replayEnv.replayModifyTableProperty(OperationType.OP_UPDATE_BINLOG_CONFIG, log);
        Assertions.assertNull(cache.getSqlCaches().getIfPresent("cached-incr-read"));
        Assertions.assertEquals(committed, table.getBinlogConfig());
        handler.failAfter = Integer.MAX_VALUE;
        alter(handler, 60);
        Assertions.assertEquals(committed.getConfigVersion() + 1, published().getConfigVersion());
        applied.values().forEach(config -> Assertions.assertEquals(published(), config));
    }

    @Test
    public void legacyUnlimitedRetentionIsPublishedBeforeEnablingCleanup() throws Exception {
        table.getBinlogConfig().setTtlSeconds(BinlogConfig.NO_TTL);
        alter(handler, 60);
        Assertions.assertEquals(60, published().getTtlSeconds());
        Assertions.assertEquals(1, journal.size());
    }

    @Test
    public void journalFailureCannotSendAnyTabletUpdate() {
        Mockito.doThrow(new IllegalStateException("journal failure")).when(editLog)
                .logUpdateBinlogConfig(Mockito.any());
        Assertions.assertThrows(IllegalStateException.class, () -> alter(handler, 60));
        Assertions.assertEquals(0, handler.calls);
    }

    @Test
    public void concurrentAlterCannotPublishAnOlderExtension() {
        table.getBinlogConfig().setTtlSeconds(60);
        handler.duringUpdate = () -> {
            BinlogConfig newer = new BinlogConfig(table.getBinlogConfig());
            newer.setConfigVersion(newer.getConfigVersion() + 1);
            newer.setTtlSeconds(30);
            table.setBinlogConfig(newer);
            handler.duringUpdate = () -> { };
        };
        Assertions.assertThrows(DdlException.class, () -> alter(handler, 3600));
        Assertions.assertEquals(30, table.getBinlogConfig().getTtlSeconds());
    }

    @Test
    public void newPartitionCannotBeSkippedWhenPublishingAnExtension() {
        table.getBinlogConfig().setTtlSeconds(60);
        handler.duringUpdate = () -> table.addPartition(partition(5, "p3"));
        Assertions.assertThrows(DdlException.class, () -> alter(handler, 3600));
        Assertions.assertEquals(60, published().getTtlSeconds());
    }

    @Test
    public void temporaryPartitionWithTheSameNameAlsoReceivesRetentionUpdates() throws Exception {
        table.addTempPartition(partition(5, "p1"));
        alter(handler, 60);
        Assertions.assertEquals(3, applied.size());
        Assertions.assertEquals(published(), applied.get("5"));
    }

    @Test
    public void cloudPublicationAcknowledgesLogicalReplicaWithoutChoosingQueryBackend() throws Exception {
        table.getBinlogConfig().setTtlSeconds(60);
        CloudReplica replica = Mockito.mock(CloudReplica.class);
        Mockito.when(replica.getId()).thenReturn(9L);
        Mockito.when(replica.getBackendId()).thenThrow(new AssertionError("must not choose a query backend"));
        installTopology(7L, 8L, replica);
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        Cloud.UpdateTabletResponse ok = Cloud.UpdateTabletResponse.newBuilder().setStatus(
                Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK)).build();
        Mockito.when(proxy.updateTablet(Mockito.any())).thenAnswer(invocation -> {
            Assertions.assertFalse(table.isWriteLockHeldByCurrentThread());
            Assertions.assertEquals(60, table.getBinlogConfig().getTtlSeconds());
            Assertions.assertEquals(3600, table.getBinlogConfig().getPendingTtlSeconds());
            Cloud.UpdateTabletRequest request = invocation.getArgument(0);
            Assertions.assertEquals(8L, request.getTabletMetaInfos(0).getTabletId());
            Assertions.assertEquals(3600, request.getTabletMetaInfos(0).getBinlogConfig().getTtlSeconds());
            return ok;
        });
        String previousCloudId = Config.cloud_unique_id;
        Config.cloud_unique_id = "row-binlog-publication-test";
        try (MockedStatic<MetaServiceProxy> mock = Mockito.mockStatic(MetaServiceProxy.class)) {
            mock.when(MetaServiceProxy::getInstance).thenReturn(proxy);

            alter(new CloudSchemaChangeHandler(), 3600);

            Mockito.verify(proxy).updateTablet(Mockito.any());
            Mockito.verify(replica, Mockito.never()).getBackendId();
            Assertions.assertEquals(3600, published().getTtlSeconds());
            Assertions.assertEquals(0, published().getPendingTtlSeconds());
        } finally {
            Config.cloud_unique_id = previousCloudId;
        }
    }

    @ParameterizedTest
    @CsvSource({"86400, 60", "60, 3600"})
    public void cloudBatchFailureKeepsThePublishedShorterPolicyAndCanBeRetried(long oldTtl, long newTtl)
            throws Exception {
        table.getBinlogConfig().setTtlSeconds(oldTtl);
        int oldBatchSize = Config.cloud_txn_tablet_batch_size;
        Config.cloud_txn_tablet_batch_size = 1;
        try {
            for (Partition partition : table.getPartitions()) {
                MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
                Tablet tablet = Mockito.mock(Tablet.class);
                long partitionId = partition.getId();
                Mockito.when(tablet.getId()).thenReturn(partitionId + 100);
                Tablet secondTablet = Mockito.mock(Tablet.class);
                Mockito.when(secondTablet.getId()).thenReturn(partitionId + 200);
                Mockito.when(index.getTablets()).thenReturn(java.util.Arrays.asList(tablet, secondTablet));
                Mockito.when(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE, true))
                        .thenReturn(Collections.singletonList(index));
            }
            MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
            Cloud.UpdateTabletResponse ok = Cloud.UpdateTabletResponse.newBuilder().setStatus(
                    Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK)).build();
            List<Cloud.UpdateTabletRequest> accepted = new ArrayList<>();
            Mockito.when(proxy.updateTablet(Mockito.any())).thenAnswer(invocation -> {
                Cloud.UpdateTabletRequest request = invocation.getArgument(0);
                Assertions.assertEquals(60, published().getTtlSeconds());
                Assertions.assertEquals(published().getConfigVersion(),
                        request.getTabletMetaInfos(0).getBinlogConfig().getConfigVersion());
                if (accepted.size() == 1) {
                    throw new RuntimeException("injected Meta Service batch failure");
                }
                accepted.add(request);
                return ok;
            });
            try (MockedStatic<MetaServiceProxy> mock = Mockito.mockStatic(MetaServiceProxy.class)) {
                mock.when(MetaServiceProxy::getInstance).thenReturn(proxy);
                CloudSchemaChangeHandler cloud = new CloudSchemaChangeHandler();
                Assertions.assertThrows(UserException.class, () -> alter(cloud, newTtl));
                Assertions.assertEquals(1, accepted.size());
                Assertions.assertEquals(60, table.getBinlogConfig().getTtlSeconds());
                Mockito.doReturn(ok).when(proxy).updateTablet(Mockito.any());
                alter(cloud, newTtl);
                Mockito.verify(proxy, Mockito.times(6)).updateTablet(Mockito.any());
                Assertions.assertEquals(2, published().getConfigVersion());
                Assertions.assertEquals(newTtl, published().getTtlSeconds());
            }
        } finally {
            Config.cloud_txn_tablet_batch_size = oldBatchSize;
        }
    }
}

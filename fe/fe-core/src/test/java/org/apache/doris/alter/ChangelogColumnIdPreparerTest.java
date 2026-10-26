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

import org.apache.doris.binlog.BinlogTestUtils;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.UserException;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.ChangelogColumnIdentityInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService.PFetchColIdsRequest;
import org.apache.doris.proto.InternalService.PFetchColIdsRequest.PFetchColIdParam;
import org.apache.doris.proto.InternalService.PFetchColIdsResponse;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.transaction.TxnWriteSchemaSnapshot;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ChangelogColumnIdPreparerTest {
    @Test
    public void testColumnIdentityJournalEnvelopeRoundTrip() throws Exception {
        OlapTable table = table();
        ChangelogColumnIdentityInfo info = ChangelogColumnIdPreparer.prepareInfo(1, table,
                Map.of(11L, Map.of("k", 4, "v", 9)));
        JournalEntity written = new JournalEntity();
        written.setOpCode(OperationType.OP_CHANGELOG_COLUMN_IDENTITY);
        written.setData(info);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        written.write(new DataOutputStream(bytes));
        JournalEntity restored = new JournalEntity();
        restored.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assertions.assertEquals(OperationType.OP_CHANGELOG_COLUMN_IDENTITY, restored.getOpCode());
        ((ChangelogColumnIdentityInfo) restored.getData()).apply(table);
        Assertions.assertEquals(4, table.getBaseSchema().get(0).getUniqueId());
        Assertions.assertEquals(8, table.getBaseSchemaVersion());
    }

    private OlapTable table() {
        Column key = new Column("k", PrimitiveType.INT);
        key.setIsKey(true);
        Column value = new Column("v", PrimitiveType.INT);
        OlapTable table = new OlapTable(10, "legacy", Arrays.asList(key, value), KeysType.DUP_KEYS, null, null);
        table.setBaseIndexId(11);
        table.setIndexMeta(11, "legacy", Arrays.asList(key, value), 7, 17,
                (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setEnableLightSchemaChange(false);
        table.setChangelogConfig(BinlogTestUtils.newTestChangelogConfig(true, false));
        return table;
    }

    @Test
    public void testLegacyImageAlignmentAndReplayPreserveProperty() throws Exception {
        OlapTable original = table();
        OlapTable restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original), OlapTable.class);
        Assertions.assertEquals(-1, restored.getBaseSchema().get(0).getUniqueId());
        ChangelogColumnIdentityInfo info = ChangelogColumnIdPreparer.prepareInfo(1, restored,
                Map.of(11L, Map.of("k", 4, "v", 9)));
        info.apply(restored);
        Assertions.assertFalse(restored.getEnableLightSchemaChange());
        Assertions.assertTrue(restored.requiresColumnUniqueIds());
        Assertions.assertEquals(4, restored.getBaseSchema().get(0).getUniqueId());
        Assertions.assertEquals(9, restored.getBaseIndexMeta().getMaxColUniqueId());
        Assertions.assertEquals(8, restored.getBaseIndexMeta().getSchemaVersion());
        ChangelogColumnIdentityInfo replay = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(info),
                ChangelogColumnIdentityInfo.class);
        replay.apply(original);
        replay.apply(original);
        Assertions.assertEquals(8, original.getBaseIndexMeta().getSchemaVersion());
        Assertions.assertEquals(9, original.getBaseSchema().get(1).getUniqueId());
        Assertions.assertFalse(original.getEnableLightSchemaChange());
    }

    @Test
    public void testLegacyCatalogBuildsWriteSnapshotAfterAlignment() throws Exception {
        OlapTable original = table();
        MaterializedIndexMeta companion = new MaterializedIndexMeta(12, original.generateTableChangelogSchema(),
                1, 18, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS, null);
        companion.initSchemaColumnUniqueId();
        original.setChangelogIndexMeta(companion, "changelog");
        OlapTable restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original), OlapTable.class);
        Assertions.assertThrows(IllegalArgumentException.class, () -> TxnWriteSchemaSnapshot.capture(restored, false));
        Map<String, Integer> companionIds = companion.getSchema(true).stream()
                .collect(Collectors.toMap(Column::getName, Column::getUniqueId));
        ChangelogColumnIdPreparer.prepareInfo(1, restored,
                Map.of(11L, Map.of("k", 4, "v", 9), 12L, companionIds)).apply(restored);
        TxnWriteSchemaSnapshot snapshot = TxnWriteSchemaSnapshot.capture(restored, false);
        Assertions.assertFalse(restored.getEnableLightSchemaChange());
        Assertions.assertEquals(Map.of(11L, 8, 12L, 1), snapshot.getIndexSchemaVersions());
        Assertions.assertEquals(4, snapshot.getChangelogMappings().get(11L).toThrift()
                .getEntries().get(0).getSourceColumnUniqueId());
        Assertions.assertEquals(9, snapshot.getChangelogMappings().get(11L).toThrift()
                .getEntries().get(1).getSourceColumnUniqueId());
        Assertions.assertEquals(companionIds, restored.getIndexMetaByIndexId(12).getSchema(true).stream()
                .collect(Collectors.toMap(Column::getName, Column::getUniqueId)));
    }

    @Test
    public void testMismatchDoesNotPartiallyUpdate() {
        OlapTable table = table();
        table.getBaseSchema().get(1).setUniqueId(8);
        Assertions.assertThrows(UserException.class, () -> ChangelogColumnIdPreparer.prepareInfo(1, table,
                Map.of(11L, Map.of("k", 4, "v", 9))));
        Assertions.assertEquals(-1, table.getBaseSchema().get(0).getUniqueId());
        Assertions.assertEquals(8, table.getBaseSchema().get(1).getUniqueId());
        Assertions.assertEquals(7, table.getBaseIndexMeta().getSchemaVersion());
    }

    @Test
    public void testReplayRejectsChangedSchemaWithoutPartialMutation() throws Exception {
        OlapTable table = table();
        ChangelogColumnIdentityInfo info = ChangelogColumnIdPreparer.prepareInfo(1, table,
                Map.of(11L, Map.of("k", 4, "v", 9)));
        table.getBaseSchema().get(1).setUniqueId(8);
        Assertions.assertThrows(IllegalStateException.class, () -> info.apply(table));
        Assertions.assertEquals(-1, table.getBaseSchema().get(0).getUniqueId());
        Assertions.assertEquals(7, table.getBaseIndexMeta().getSchemaVersion());
    }

    @Test
    public void testMissingAndDuplicateIdsRejected() {
        OlapTable table = table();
        Assertions.assertThrows(UserException.class, () -> ChangelogColumnIdPreparer.prepareInfo(1, table,
                Map.of(11L, Map.of("k", 4))));
        Assertions.assertThrows(UserException.class, () -> ChangelogColumnIdPreparer.prepareInfo(1, table,
                Map.of(11L, Map.of("k", 4, "v", 4))));
        Assertions.assertThrows(UserException.class, () -> ChangelogColumnIdPreparer.prepareInfo(1, table,
                Map.of()));
        Assertions.assertEquals(-1, table.getBaseSchema().get(0).getUniqueId());
    }

    @Test
    public void testCompanionIdentityAndVersionPreserved() throws Exception {
        OlapTable table = table();
        Column companionColumn = new Column("v", PrimitiveType.INT);
        companionColumn.setUniqueId(27);
        table.setIndexMeta(12, "companion", Arrays.asList(companionColumn), 3, 18,
                (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        MaterializedIndexMeta companion = table.getIndexMetaByIndexId(12);
        companion.setMaxColUniqueId(31);
        ChangelogColumnIdPreparer.prepareInfo(1, table,
                Map.of(11L, Map.of("k", 4, "v", 9), 12L, Map.of("v", 27))).apply(table);
        Assertions.assertEquals(27, companion.getSchema().get(0).getUniqueId());
        Assertions.assertEquals(31, companion.getMaxColUniqueId());
        Assertions.assertEquals(3, companion.getSchemaVersion());
    }

    @Test
    public void testBackendResponseCoverageAndReplicaConsistency() {
        PFetchColIdsRequest request = PFetchColIdsRequest.newBuilder().addParams(
                PFetchColIdParam.newBuilder().setIndexId(11).addTabletIds(100)).build();
        PFetchColIdsResponse complete = PFetchColIdsResponse.newBuilder().addEntries(
                PFetchColIdsResponse.PFetchColIdsResultEntry.newBuilder().setIndexId(11)
                        .putColNameToId("k", 4).putColNameToId("v", 9)).build();
        PFetchColIdsResponse conflicting = PFetchColIdsResponse.newBuilder().addEntries(
                PFetchColIdsResponse.PFetchColIdsResultEntry.newBuilder().setIndexId(11)
                        .putColNameToId("k", 5).putColNameToId("v", 9)).build();
        Assertions.assertThrows(IllegalStateException.class, () -> AlterLightSchChangeHelper.validateCoverage(
                request, PFetchColIdsResponse.getDefaultInstance()));
        AlterLightSchChangeHelper.validateCoverage(request, complete);
        Assertions.assertThrows(IllegalStateException.class,
                () -> new AlterLightSchChangeHelper(null, null).compactToColumnInfo(
                        Arrays.asList(complete, conflicting)));
    }

    @Test
    public void testPreparationUsesUnlockedRpcAndDurableJournal() throws Exception {
        exerciseRpc(false);
    }

    @Test
    public void testConcurrentSchemaChangeRejectsPreparation() throws Exception {
        exerciseRpc(true);
    }

    private void exerciseRpc(boolean changeSchemaDuringRpc) throws Exception {
        OlapTable table = table();
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getId()).thenReturn(1L);
        Tablet tablet = Mockito.mock(Tablet.class);
        Mockito.when(tablet.getId()).thenReturn(100L);
        Mockito.when(tablet.getNormalReplicaBackendIds()).thenReturn(Arrays.asList(20L));
        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        Mockito.when(index.getId()).thenReturn(11L);
        Mockito.when(index.getTablets()).thenReturn(Arrays.asList(tablet));
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getId()).thenReturn(30L);
        Mockito.when(partition.getName()).thenReturn("p");
        Mockito.when(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL, true))
                .thenReturn(Arrays.asList(index));
        table.addPartition(partition);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(true);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        EditLog.EditLogItem item = Mockito.mock(EditLog.EditLogItem.class);
        Mockito.when(editLog.logChangelogColumnIdentity(Mockito.any())).thenAnswer(invocation -> {
            Assertions.assertTrue(table.isWriteLockHeldByCurrentThread());
            Assertions.assertEquals(4, table.getBaseSchema().get(0).getUniqueId());
            return item;
        });
        Mockito.when(item.await()).thenAnswer(invocation -> {
            Assertions.assertFalse(table.isWriteLockHeldByCurrentThread());
            return 1L;
        });
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Backend backend = new Backend(20, "127.0.0.1", 9050);
        backend.setBrpcPort(8060);
        Mockito.when(systemInfo.getAllBackendsByAllCluster()).thenReturn(ImmutableMap.of(20L, backend));
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.getColumnIdsByTabletIds(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
            boolean unlocked = CompletableFuture.supplyAsync(() -> {
                boolean locked = table.tryWriteLock(1, TimeUnit.SECONDS);
                if (locked) {
                    table.writeUnlock();
                }
                return locked;
            }).get(2, TimeUnit.SECONDS);
            Assertions.assertTrue(unlocked, "BE RPC must run outside all table locks");
            PFetchColIdsRequest request = invocation.getArgument(1);
            Assertions.assertEquals(Arrays.asList(100L), request.getParams(0).getTabletIdsList());
            if (changeSchemaDuringRpc) {
                table.writeLock();
                try {
                    table.getBaseIndexMeta().setSchemaVersion(8);
                } finally {
                    table.writeUnlock();
                }
            }
            return CompletableFuture.completedFuture(PFetchColIdsResponse.newBuilder()
                    .setStatus(PStatus.newBuilder().setStatusCode(0))
                    .addEntries(PFetchColIdsResponse.PFetchColIdsResultEntry.newBuilder().setIndexId(11)
                            .putColNameToId("k", 4).putColNameToId("v", 9)).build());
        });
        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> proxyMock = Mockito.mockStatic(BackendServiceProxy.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            envMock.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
            proxyMock.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            if (changeSchemaDuringRpc) {
                Assertions.assertThrows(UserException.class, () -> ChangelogColumnIdPreparer.ensureReady(db, table));
                Assertions.assertEquals(-1, table.getBaseSchema().get(0).getUniqueId());
                Mockito.verifyNoInteractions(editLog);
            } else {
                ChangelogColumnIdPreparer.ensureReady(db, table);
                ChangelogColumnIdPreparer.ensureReady(db, table);
                Mockito.verify(item).await();
                Mockito.verify(editLog).logChangelogColumnIdentity(Mockito.any());
                Mockito.verify(proxy).getColumnIdsByTabletIds(Mockito.any(), Mockito.any());
                Assertions.assertFalse(table.getEnableLightSchemaChange());
            }
        }
    }

    @Test
    public void testExistingValidIdPreserved() throws Exception {
        OlapTable table = table();
        table.getBaseSchema().get(0).setUniqueId(4);
        ChangelogColumnIdPreparer.prepareInfo(1, table, Map.of(11L, Map.of("k", 4, "v", 9))).apply(table);
        Assertions.assertEquals(4, table.getBaseSchema().get(0).getUniqueId());
        Assertions.assertEquals(9, table.getBaseSchema().get(1).getUniqueId());
    }
}

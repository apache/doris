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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.doris.FeServiceClient;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.doris.RemoteDorisExternalDatabase;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.master.MasterImpl;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.RemoteOlapTableSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TCommitRemoteTxnRequest;
import org.apache.doris.thrift.TCommitRemoteTxnResult;
import org.apache.doris.thrift.TOlapTableIndexSchema;
import org.apache.doris.thrift.TOlapTableSchemaParam;
import org.apache.doris.thrift.TRowBinlogWriteColumnMapping;
import org.apache.doris.thrift.TRowBinlogWriteColumnMappings;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class RemoteOlapInsertExecutorTest {
    private final TransactionState owner = new TransactionState(1L, Collections.singletonList(2L), 100L,
            "remote", null, LoadJobSourceType.BACKEND_STREAMING,
            new TxnCoordinator(TxnSourceType.FE, 0, "127.0.0.1", 0), -1, 60000);
    private final GlobalTransactionMgrIface manager = Mockito.mock(GlobalTransactionMgrIface.class);
    private final FeServiceClient client = Mockito.mock(FeServiceClient.class);
    private final RemoteOlapTable table = Mockito.mock(RemoteOlapTable.class);
    private MockedStatic<Env> envMock;
    private MockedStatic<EnvFactory> factoryMock;
    private FrontendServiceImpl service;

    @BeforeEach
    void setUp() throws Exception {
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getTokenManager().checkAuthToken("test-token")).thenReturn(true);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getId()).thenReturn(1L);
        Mockito.when(db.getTableOrMetaException("target", TableType.OLAP)).thenReturn(Mockito.mock(OlapTable.class));
        Mockito.when(internalCatalog.getDbNullable("remote_db")).thenReturn(db);
        Mockito.when(manager.getTransactionState(1L, 100L)).thenReturn(owner);
        Mockito.when(manager.commitAndPublishTransaction(Mockito.eq(db), Mockito.anyList(), Mockito.eq(100L),
                Mockito.anyList(), Mockito.anyLong(), Mockito.isNull())).thenReturn(false);
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        envMock.when(Env::getCurrentInternalCatalog).thenReturn(internalCatalog);
        envMock.when(Env::getCurrentGlobalTransactionMgr).thenReturn(manager);

        RemoteDorisExternalCatalog catalog = Mockito.mock(RemoteDorisExternalCatalog.class);
        RemoteDorisExternalDatabase externalDb = Mockito.mock(RemoteDorisExternalDatabase.class);
        Mockito.when(table.getDatabase()).thenReturn(externalDb);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getName()).thenReturn("target");
        Mockito.when(externalDb.getId()).thenReturn(99L);
        Mockito.when(externalDb.getFullName()).thenReturn("remote_db");
        Mockito.when(externalDb.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("remote_catalog");
        Mockito.when(catalog.getFeServiceClient()).thenReturn(client);
        EnvFactory factory = Mockito.mock(EnvFactory.class);
        Mockito.when(factory.createCoordinator(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong()))
                .thenReturn(Mockito.mock(Coordinator.class));
        factoryMock = Mockito.mockStatic(EnvFactory.class);
        factoryMock.when(EnvFactory::getInstance).thenReturn(factory);
        try (MockedConstruction<MasterImpl> master = Mockito.mockConstruction(MasterImpl.class)) {
            service = new FrontendServiceImpl(null);
        }
    }

    @AfterEach
    void tearDown() {
        factoryMock.close();
        envMock.close();
        ConnectContext.remove();
    }

    @Test
    void testSenderSnapshotReachesOwnerBeforeCommitAndSurvivesReplay() throws Exception {
        TOlapTableIndexSchema index = new TOlapTableIndexSchema(10L, Collections.emptyList(), 1)
                .setRowBinlogId(20L).setRowBinlogNeedHistoricalValue(true)
                .setRowBinlogColumnMappings(Arrays.asList(new TRowBinlogWriteColumnMapping(1, 11),
                        new TRowBinlogWriteColumnMapping(2, 12).setBeforeColumnUniqueId(22)));
        RemoteOlapInsertExecutor executor = prepareExecutor(index);
        // The writer's schema is mutable; commit must retain the snapshot made during finalizeSink.
        index.getRowBinlogColumnMappings().get(1).setCurrentColumnUniqueId(99);
        TRowBinlogWriteColumnMappings expected = historicalMapping();
        Mockito.when(manager.commitAndPublishTransaction(Mockito.any(), Mockito.anyList(), Mockito.eq(100L),
                Mockito.anyList(), Mockito.anyLong(), Mockito.isNull())).thenAnswer(invocation -> {
                    Assertions.assertEquals(Collections.singletonMap(10L, expected),
                            owner.getRowBinlogColumnMappings(100L));
                    owner.setTransactionStatus(TransactionStatus.COMMITTED);
                    return false;
                });
        Mockito.when(client.commitRemoteTxn(Mockito.any())).thenAnswer(invocation -> {
            TCommitRemoteTxnRequest sent = invocation.getArgument(0);
            Assertions.assertTrue(sent.isSetRowBinlogColumnMappings());
            Assertions.assertEquals(Collections.singletonMap(10L, expected), sent.getRowBinlogColumnMappings());
            TCommitRemoteTxnRequest received = new TCommitRemoteTxnRequest();
            new TDeserializer().deserialize(received, new TSerializer().serialize(sent));
            return service.commitRemoteTxn(received.setToken("test-token"));
        });
        executor.onComplete();
        executor.onComplete(); // The existing RPC retry is idempotent even after COMMITTED.
        Assertions.assertEquals(TransactionStatus.COMMITTED, executor.txnStatus);

        MetaContext context = new MetaContext();
        context.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        context.setThreadLocalInfo();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        owner.write(new DataOutputStream(bytes));
        TransactionState replayed = TransactionState.read(new DataInputStream(
                new ByteArrayInputStream(bytes.toByteArray())));
        PublishVersionTask publish = new PublishVersionTask(1L, 100L, 1L, Collections.emptyList(), 0);
        publish.setRowBinlogColumnMappings(replayed.getRowBinlogColumnMappings(100L));
        Assertions.assertEquals(Collections.singletonMap(10L, expected),
                publish.toThrift().getRowBinlogColumnMappings());
        Mockito.verify(manager, Mockito.never()).getTransactionState(99L, 100L);
    }

    @Test
    void testSenderExplicitlySendsEmptySnapshot() throws Exception {
        RemoteOlapInsertExecutor executor = prepareExecutor(
                new TOlapTableIndexSchema(10L, Collections.emptyList(), 1));
        Mockito.when(client.commitRemoteTxn(Mockito.any())).thenAnswer(invocation -> {
            TCommitRemoteTxnRequest request = invocation.getArgument(0);
            Assertions.assertTrue(request.isSetRowBinlogColumnMappings());
            Assertions.assertTrue(request.getRowBinlogColumnMappings().isEmpty());
            return service.commitRemoteTxn(request.setToken("test-token"));
        });
        executor.onComplete();
        Assertions.assertEquals(TransactionStatus.COMMITTED, executor.txnStatus);
    }

    @Test
    void testCloudSenderStillSendsSnapshotToLocalOwner() throws Exception {
        RemoteOlapInsertExecutor executor;
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS)) {
            config.when(Config::isCloudMode).thenReturn(true);
            executor = prepareExecutor(new TOlapTableIndexSchema(10L, Collections.emptyList(), 1)
                    .setRowBinlogId(20L).setRowBinlogNeedHistoricalValue(true)
                    .setRowBinlogColumnMappings(historicalMapping().getEntries()));
        }
        Mockito.when(client.commitRemoteTxn(Mockito.any())).thenAnswer(invocation -> {
            TCommitRemoteTxnRequest request = invocation.getArgument(0);
            return service.commitRemoteTxn(request.setToken("test-token"));
        });
        executor.onComplete();
        Assertions.assertEquals(Collections.singletonMap(10L, historicalMapping()),
                owner.getRowBinlogColumnMappings(100L));
    }

    @Test
    void testOwnerRejectsMissingOrIncompleteSnapshotBeforeCommit() throws Exception {
        assertRejected(request(), "mapping");
        assertRejected(request().setRowBinlogColumnMappings(Collections.singletonMap(10L,
                new TRowBinlogWriteColumnMappings().setEntries(Collections.emptyList()))), "mapping");
        assertRejected(request().setRowBinlogColumnMappings(Collections.singletonMap(10L,
                new TRowBinlogWriteColumnMappings().setNeedHistoricalValue(true))), "mapping");
        assertRejected(request().setRowBinlogColumnMappings(Collections.singletonMap(10L,
                new TRowBinlogWriteColumnMappings().setNeedHistoricalValue(true)
                        .setEntries(Collections.singletonList(new TRowBinlogWriteColumnMapping())))), "mapping");
        Assertions.assertTrue(owner.getRowBinlogColumnMappings(100L).isEmpty());
        Mockito.verify(manager, Mockito.never()).commitAndPublishTransaction(Mockito.any(), Mockito.anyList(),
                Mockito.anyLong(), Mockito.anyList(), Mockito.anyLong(), Mockito.isNull());
    }

    @Test
    void testOwnerRejectsChangedRetriesIncludingAfterCommit() throws Exception {
        TCommitRemoteTxnRequest original = request().setRowBinlogColumnMappings(
                Collections.singletonMap(10L, historicalMapping()));
        Assertions.assertEquals(TStatusCode.OK, service.commitRemoteTxn(original).getStatus().getStatusCode());
        for (TransactionStatus status : Arrays.asList(TransactionStatus.PREPARE, TransactionStatus.COMMITTED,
                TransactionStatus.VISIBLE)) {
            owner.setTransactionStatus(status);
            Assertions.assertEquals(TStatusCode.OK, service.commitRemoteTxn(original).getStatus().getStatusCode());
            TCommitRemoteTxnRequest changed = original.deepCopy();
            changed.getRowBinlogColumnMappings().get(10L).setNeedHistoricalValue(false);
            assertRejected(changed, "changed");
            assertRejected(request().setRowBinlogColumnMappings(Collections.emptyMap()), "changed");
            Assertions.assertEquals(original.getRowBinlogColumnMappings(), owner.getRowBinlogColumnMappings(100L));
        }
    }

    @Test
    void testOwnerPreservesKeyOnlyHistoricalAndExplicitFalse() throws Exception {
        TRowBinlogWriteColumnMappings keyOnly = new TRowBinlogWriteColumnMappings()
                .setNeedHistoricalValue(true).setEntries(Collections.singletonList(
                        new TRowBinlogWriteColumnMapping(1, 11)));
        Map<Long, TRowBinlogWriteColumnMappings> mappings = new HashMap<>();
        mappings.put(10L, keyOnly);
        mappings.put(30L, keyOnly.deepCopy().setNeedHistoricalValue(false));
        Assertions.assertEquals(TStatusCode.OK,
                service.commitRemoteTxn(request().setRowBinlogColumnMappings(mappings)).getStatus().getStatusCode());
        keyOnly.setNeedHistoricalValue(false);
        Assertions.assertTrue(owner.getRowBinlogColumnMappings(100L).get(10L).isNeedHistoricalValue());
        Assertions.assertTrue(owner.getRowBinlogColumnMappings(100L).get(30L).isSetNeedHistoricalValue());
        Assertions.assertFalse(owner.getRowBinlogColumnMappings(100L).get(30L).isNeedHistoricalValue());
        Assertions.assertFalse(owner.getRowBinlogColumnMappings(100L).get(10L).getEntries().get(0)
                .isSetBeforeColumnUniqueId());
    }

    @Test
    void testOwnerRejectsLateFirstSnapshotAndMissingTransaction() throws Exception {
        owner.setTransactionStatus(TransactionStatus.COMMITTED);
        TCommitRemoteTxnRequest request = request().setRowBinlogColumnMappings(
                Collections.singletonMap(10L, historicalMapping()));
        assertRejected(request, "COMMITTED");
        Assertions.assertTrue(owner.getRowBinlogColumnMappings(100L).isEmpty());
        Mockito.when(manager.getTransactionState(1L, 100L)).thenReturn(null);
        assertRejected(request, "100");
        Mockito.verify(manager, Mockito.never()).commitAndPublishTransaction(Mockito.any(), Mockito.anyList(),
                Mockito.anyLong(), Mockito.anyList(), Mockito.anyLong(), Mockito.isNull());
    }

    @Test
    void testCloudOwnerDoesNotPersistFeSnapshot() throws Exception {
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS)) {
            config.when(Config::isCloudMode).thenReturn(true);
            Assertions.assertEquals(TStatusCode.OK, service.commitRemoteTxn(request().setRowBinlogColumnMappings(
                    Collections.singletonMap(10L, historicalMapping()))).getStatus().getStatusCode());
            Assertions.assertTrue(owner.getRowBinlogColumnMappings(100L).isEmpty());
            Mockito.verify(manager, Mockito.never()).getTransactionState(Mockito.anyLong(), Mockito.anyLong());
        }
    }

    private RemoteOlapInsertExecutor prepareExecutor(TOlapTableIndexSchema index) {
        RemoteOlapInsertExecutor executor = new RemoteOlapInsertExecutor(new ConnectContext(), table, "remote",
                Mockito.mock(NereidsPlanner.class), Optional.empty(), false, 123L);
        executor.txnId = 100L;
        RemoteOlapTableSink sink = Mockito.mock(RemoteOlapTableSink.class);
        Mockito.when(sink.getOlapTableSchemaParam()).thenReturn(new TOlapTableSchemaParam()
                .setIndexes(Collections.singletonList(index)));
        executor.finalizeSink(Mockito.mock(PlanFragment.class), sink, Mockito.mock(PhysicalOlapTableSink.class));
        return executor;
    }

    private TCommitRemoteTxnRequest request() {
        return new TCommitRemoteTxnRequest().setDb("remote_db").setTbl("target").setTxnId(100L)
                .setToken("test-token").setCommitInfos(Collections.emptyList()).setInsertVisibleTimeoutMs(1000L);
    }

    private TRowBinlogWriteColumnMappings historicalMapping() {
        return new TRowBinlogWriteColumnMappings().setNeedHistoricalValue(true).setEntries(Arrays.asList(
                new TRowBinlogWriteColumnMapping(1, 11),
                new TRowBinlogWriteColumnMapping(2, 12).setBeforeColumnUniqueId(22)));
    }

    private void assertRejected(TCommitRemoteTxnRequest request, String message) throws Exception {
        TCommitRemoteTxnResult result = service.commitRemoteTxn(request);
        Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, result.getStatus().getStatusCode());
        Assertions.assertTrue(result.getStatus().getErrorMsgs().get(0).contains(message), result.toString());
    }
}

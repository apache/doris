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

package org.apache.doris.cloud.transaction;

import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.stream.CloudOlapTableStreamUpdate;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.AbortTxnResponse;
import org.apache.doris.cloud.proto.Cloud.BeginTxnResponse;
import org.apache.doris.cloud.proto.Cloud.CheckTxnConflictResponse;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.cloud.proto.Cloud.GetCurrentMaxTxnResponse;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.TxnInfoPB;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.MakeCloudTmpRsVisibleTask;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.SubTransactionState;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnStateChangeCallback;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class CloudGlobalTransactionMgrTest {

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static GlobalTransactionMgrIface masterTransMgr;
    private static Env masterEnv;

    private TransactionState.TxnCoordinator transactionSource = new TransactionState.TxnCoordinator(
            TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());

    @BeforeEach
    public void setUp() throws Exception {

        Config.cloud_unique_id = "cloud_unique_id";
        Config.meta_service_endpoint = "127.0.0.1:20121";
        fakeEditLog = new FakeEditLog();
        fakeEnv = new FakeEnv();
        masterEnv = CatalogTestUtil.createTestCatalog();
        FakeEnv.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        masterTransMgr = masterEnv.getGlobalTransactionMgr();
    }

    @AfterEach
    public void tearDown() {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
    }

    @Test
    public void testBeginTransaction() throws Exception {
        AtomicLong id = new AtomicLong(1000);
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            Mockito.doAnswer(invocation -> {
                BeginTxnResponse.Builder beginTxnResponseBuilder = BeginTxnResponse.newBuilder();
                beginTxnResponseBuilder.setTxnId(id.getAndIncrement())
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK).setMsg("OK"));
                return beginTxnResponseBuilder.build();
            }).when(mockProxy).beginTxn(Mockito.any());

            long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                    CatalogTestUtil.testTxnLabel1,
                    transactionSource,
                    TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

            Assertions.assertEquals(transactionId + 1, id.get());
        }
    }

    @Test
    public void testBeginTransactionConflict() throws Exception {
        AtomicLong id = new AtomicLong(1000);
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            final int[] times = {1};
            Mockito.doAnswer(invocation -> {
                BeginTxnResponse.Builder beginTxnResponseBuilder = BeginTxnResponse.newBuilder();
                if (times[0] > 5) {
                    beginTxnResponseBuilder.setTxnId(id.getAndIncrement())
                            .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK).setMsg("OK"));
                } else {
                    beginTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.KV_TXN_CONFLICT).setMsg("kv txn conflict"));
                }
                times[0]++;
                return beginTxnResponseBuilder.build();
            }).when(mockProxy).beginTxn(Mockito.any());

            long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                    CatalogTestUtil.testTxnLabel1,
                    transactionSource,
                    TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

            Assertions.assertEquals(transactionId + 1, id.get());
        }
    }

    @Test
    public void testBeginTransactionLabelAlreadyUsedException() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            BeginTxnResponse response = BeginTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.TXN_LABEL_ALREADY_USED).setMsg("label already used"))
                    .build();
            Mockito.doReturn(response).when(mockProxy).beginTxn(Mockito.any());

            Assertions.assertThrows(LabelAlreadyUsedException.class,
                    () -> {
                            masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                                    CatalogTestUtil.testTxnLabel1,
                                    transactionSource,
                                    TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
                });
        }
    }

    @Test
    public void testBeginTransactionDuplicatedRequestException() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            BeginTxnResponse response = BeginTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.TXN_DUPLICATED_REQ).setMsg("duplicated request"))
                    .build();
            Mockito.doReturn(response).when(mockProxy).beginTxn(Mockito.any());

            Assertions.assertThrows(DuplicatedRequestException.class,
                    () -> {
                            masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                                    CatalogTestUtil.testTxnLabel1,
                                    transactionSource,
                                    TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
                });
        }
    }

    @Test
    public void testCommitTransaction() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
            txnInfoBuilder.setDbId(CatalogTestUtil.testTableId1);
            txnInfoBuilder.addAllTableIds(Lists.newArrayList(CatalogTestUtil.testTableId1));
            txnInfoBuilder.setLabel(CatalogTestUtil.testTxnLabel1);
            txnInfoBuilder.setListenerId(-1);
            CommitTxnResponse response = CommitTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"))
                    .setTxnInfo(txnInfoBuilder.build())
                    .build();
            Mockito.doReturn(response).when(mockProxy).commitTxn(Mockito.any());

            long transactionId = 123533;
            Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                    .getTableOrMetaException(CatalogTestUtil.testTableId1);
            masterTransMgr.commitTransactionWithoutLock(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1),
                    transactionId, null, null);
        }
    }

    @Test
    public void testCommitTransactionCarriesTableStreamUpdates() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            TxnInfoPB txnInfo = TxnInfoPB.newBuilder()
                    .setDbId(CatalogTestUtil.testDbId1)
                    .addTableIds(CatalogTestUtil.testTableId1)
                    .setTxnId(123533)
                    .setLabel(CatalogTestUtil.testTxnLabel1)
                    .setListenerId(-1)
                    .build();
            Mockito.doReturn(CommitTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"))
                    .setTxnInfo(txnInfo)
                    .build()).when(mockProxy).commitTxn(Mockito.any());

            Cloud.TableStreamIdentityPB identity = Cloud.TableStreamIdentityPB.newBuilder()
                    .setBaseDbId(10)
                    .setBaseTableId(20)
                    .setStreamDbId(30)
                    .setStreamId(40)
                    .build();
            Cloud.TableStreamPartitionUpdatePB partitionUpdate =
                    Cloud.TableStreamPartitionUpdatePB.newBuilder()
                            .setPartitionId(50)
                            .setExpectedState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED)
                            .setExpectedOffsetTso(60)
                            .setNextOffsetTso(70)
                            .build();
            CloudOlapTableStreamUpdate update = new CloudOlapTableStreamUpdate(identity,
                    java.util.Map.of(50L, partitionUpdate));
            TableStreamUpdateInfo updateInfo = new TableStreamUpdateInfo(30L, 40L, update);
            Cloud.TableStreamIdentityPB secondIdentity = Cloud.TableStreamIdentityPB.newBuilder()
                    .setBaseDbId(11)
                    .setBaseTableId(21)
                    .setStreamDbId(31)
                    .setStreamId(41)
                    .build();
            Cloud.TableStreamPartitionUpdatePB secondPartitionUpdate =
                    Cloud.TableStreamPartitionUpdatePB.newBuilder()
                            .setPartitionId(51)
                            .setExpectedState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_UNKNOWN)
                            .setNextOffsetTso(71)
                            .build();
            CloudOlapTableStreamUpdate secondUpdate = new CloudOlapTableStreamUpdate(secondIdentity,
                    java.util.Map.of(51L, secondPartitionUpdate));
            TableStreamUpdateInfo secondUpdateInfo = new TableStreamUpdateInfo(31L, 41L, secondUpdate);
            Table table = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                    .getTableOrMetaException(CatalogTestUtil.testTableId1);

            masterTransMgr.commitAndPublishTransaction(
                    masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1),
                    Lists.newArrayList(table), 123533, Lists.newArrayList(), 10_000, null,
                    Lists.newArrayList(updateInfo, secondUpdateInfo));

            ArgumentCaptor<Cloud.CommitTxnRequest> requestCaptor =
                    ArgumentCaptor.forClass(Cloud.CommitTxnRequest.class);
            Mockito.verify(mockProxy).commitTxn(requestCaptor.capture());
            Assertions.assertTrue(requestCaptor.getValue().hasCommitTso());
            Assertions.assertEquals(2, requestCaptor.getValue().getTableStreamUpdatesCount());
            Assertions.assertEquals(identity, requestCaptor.getValue().getTableStreamUpdates(0).getIdentity());
            Assertions.assertEquals(partitionUpdate,
                    requestCaptor.getValue().getTableStreamUpdates(0).getPartitionUpdates(0));
            Assertions.assertEquals(secondIdentity,
                    requestCaptor.getValue().getTableStreamUpdates(1).getIdentity());
            Assertions.assertEquals(secondPartitionUpdate,
                    requestCaptor.getValue().getTableStreamUpdates(1).getPartitionUpdates(0));
        }
    }

    @Test
    public void testSkipMakeTmpRsVisibleForIncompleteLazyCommit() throws Exception {
        CommitTxnResponse response = CommitTxnResponse.newBuilder()
                .setTxnInfo(TxnInfoPB.newBuilder().setTxnId(12345L).build())
                .setIsLazyCommit(true)
                .setIsLazyCommitIncomplete(true)
                .build();

        Assertions.assertFalse(invokeNotifyBesMakeTmpRsVisible(response));
    }

    @Test
    public void testMakeTmpRsVisibleForNonLazyCommitWithIncompleteFlag() throws Exception {
        CommitTxnResponse response = CommitTxnResponse.newBuilder()
                .setTxnInfo(TxnInfoPB.newBuilder().setTxnId(12346L).build())
                .setIsLazyCommit(false)
                .setIsLazyCommitIncomplete(true)
                .build();

        Assertions.assertTrue(invokeNotifyBesMakeTmpRsVisible(response));
    }

    @Test
    public void testMakeTmpRsVisibleForCompletedLazyCommit() throws Exception {
        CommitTxnResponse response = CommitTxnResponse.newBuilder()
                .setTxnInfo(TxnInfoPB.newBuilder().setTxnId(12347L).build())
                .setIsLazyCommit(true)
                .setIsLazyCommitIncomplete(false)
                .build();

        Assertions.assertTrue(invokeNotifyBesMakeTmpRsVisible(response));
    }

    @Test
    public void testMakeTmpRsVisibleForNonLazyCommit() throws Exception {
        CommitTxnResponse response = CommitTxnResponse.newBuilder()
                .setTxnInfo(TxnInfoPB.newBuilder().setTxnId(12348L)
                        .setLoadClusterId("cluster_b").build())
                .setIsLazyCommit(false)
                .setIsLazyCommitIncomplete(false)
                .addLastActiveTabletIds(10001L)
                .addLastActiveEpochs(9L)
                .build();

        Assertions.assertTrue(invokeNotifyBesMakeTmpRsVisible(response, false));
    }

    private boolean invokeNotifyBesMakeTmpRsVisible(CommitTxnResponse response) throws Exception {
        return invokeNotifyBesMakeTmpRsVisible(response, true);
    }

    private boolean invokeNotifyBesMakeTmpRsVisible(
            CommitTxnResponse response, boolean enableRowsetNotification) throws Exception {
        boolean originalEnableNotify = Config.enable_notify_be_after_load_txn_commit;
        try {
            Config.enable_notify_be_after_load_txn_commit = enableRowsetNotification;
            AtomicBoolean notified = new AtomicBoolean(false);
            CloudGlobalTransactionMgr transactionMgr = new CloudGlobalTransactionMgr() {
                @Override
                public void sendMakeCloudTmpRsVisibleTasks(long txnId,
                        List<TTabletCommitInfo> commitInfos, Map<Long, Long> partitionVersionMap,
                        long updateVersionVisibleTime, String loadClusterId,
                        List<Long> lastActiveTabletIds, List<Long> lastActiveEpochs) {
                    String expectedLoadClusterId = response.getTxnInfo().hasLoadClusterId()
                            ? response.getTxnInfo().getLoadClusterId() : "";
                    Assertions.assertEquals(expectedLoadClusterId, loadClusterId);
                    Assertions.assertEquals(response.getLastActiveTabletIdsList(), lastActiveTabletIds);
                    Assertions.assertEquals(response.getLastActiveEpochsList(), lastActiveEpochs);
                    notified.set(true);
                }
            };
            Method notifyMethod = CloudGlobalTransactionMgr.class.getDeclaredMethod(
                    "notifyBesMakeTmpRsVisible", CommitTxnResponse.class, List.class);
            notifyMethod.setAccessible(true);

            List<TabletCommitInfo> tabletCommitInfos =
                    Lists.newArrayList(new TabletCommitInfo(10001L, 10002L));

            notifyMethod.invoke(transactionMgr, response, tabletCommitInfos);
            return notified.get();
        } finally {
            Config.enable_notify_be_after_load_txn_commit = originalEnableNotify;
        }
    }

    @Test
    public void testOwnerRefreshDoesNotRequireCommitInfos() throws Exception {
        CommitTxnResponse response = CommitTxnResponse.newBuilder()
                .setTxnInfo(TxnInfoPB.newBuilder().setTxnId(12349L)
                        .setLoadClusterId("cluster_b").build())
                .addLastActiveTabletIds(10001L)
                .addLastActiveEpochs(10L)
                .build();
        AtomicBoolean notified = new AtomicBoolean(false);
        CloudGlobalTransactionMgr transactionMgr = new CloudGlobalTransactionMgr() {
            @Override
            public void sendMakeCloudTmpRsVisibleTasks(long txnId,
                    List<TTabletCommitInfo> commitInfos, Map<Long, Long> partitionVersionMap,
                    long updateVersionVisibleTime, String loadClusterId,
                    List<Long> lastActiveTabletIds, List<Long> lastActiveEpochs) {
                Assertions.assertTrue(commitInfos.isEmpty());
                Assertions.assertEquals(List.of(10001L), lastActiveTabletIds);
                Assertions.assertEquals(List.of(10L), lastActiveEpochs);
                notified.set(true);
            }
        };
        Method notifyMethod = CloudGlobalTransactionMgr.class.getDeclaredMethod(
                "notifyBesMakeTmpRsVisible", CommitTxnResponse.class, List.class);
        notifyMethod.setAccessible(true);

        notifyMethod.invoke(transactionMgr, response, null);

        Assertions.assertTrue(notified.get());
    }

    @Test
    public void testSubTransactionCommitPreservesBackendTabletMappingForVisibility() throws Exception {
        Table table = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        TTabletCommitInfo thriftCommitInfo = new TTabletCommitInfo();
        thriftCommitInfo.setTabletId(CatalogTestUtil.testTabletId1);
        thriftCommitInfo.setBackendId(10001L);
        SubTransactionState subTransactionState = new SubTransactionState(
                12353L, table, List.of(thriftCommitInfo), SubTransactionState.SubTransactionType.INSERT);
        AtomicReference<List<TTabletCommitInfo>> notifiedCommitInfos = new AtomicReference<>();
        CloudGlobalTransactionMgr transactionMgr = new CloudGlobalTransactionMgr() {
            @Override
            public void sendMakeCloudTmpRsVisibleTasks(long txnId,
                    List<TTabletCommitInfo> commitInfos, Map<Long, Long> partitionVersionMap,
                    long updateVersionVisibleTime, String loadClusterId,
                    List<Long> lastActiveTabletIds, List<Long> lastActiveEpochs) {
                notifiedCommitInfos.set(commitInfos);
            }
        };

        TxnInfoPB txnInfo = TxnInfoPB.newBuilder()
                .setDbId(CatalogTestUtil.testDbId1)
                .addTableIds(CatalogTestUtil.testTableId1)
                .setTxnId(12353L)
                .setLabel("subtxn")
                .setListenerId(-1)
                .build();
        CommitTxnResponse response = CommitTxnResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                .setTxnInfo(txnInfo)
                .build();
        boolean oldNotify = Config.enable_notify_be_after_load_txn_commit;
        Config.enable_notify_be_after_load_txn_commit = true;
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(proxy);
            Mockito.when(proxy.commitTxn(Mockito.any())).thenReturn(response);

            Assertions.assertTrue(transactionMgr.commitAndPublishTransaction(
                    masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1),
                    12353L, List.of(subTransactionState), 10_000));
        } finally {
            Config.enable_notify_be_after_load_txn_commit = oldNotify;
        }

        Assertions.assertNotNull(notifiedCommitInfos.get());
        Assertions.assertEquals(1, notifiedCommitInfos.get().size());
        Assertions.assertEquals(CatalogTestUtil.testTabletId1,
                notifiedCommitInfos.get().get(0).getTabletId());
        Assertions.assertEquals(10001L, notifiedCommitInfos.get().get(0).getBackendId());
    }

    @Test
    public void testTwoPhaseCommitReplayRefreshesOwnerBeforeReturningDuplicateError() throws Exception {
        CommitTxnResponse response = CommitTxnResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.TXN_ALREADY_VISIBLE).setMsg("txn already visible"))
                .setTxnInfo(TxnInfoPB.newBuilder().setTxnId(12351L)
                        .setLoadClusterId("cluster_b").build())
                .addLastActiveTabletIds(10001L)
                .addLastActiveEpochs(13L)
                .build();
        AtomicBoolean notified = new AtomicBoolean(false);
        CloudGlobalTransactionMgr transactionMgr = new CloudGlobalTransactionMgr() {
            @Override
            public void sendMakeCloudTmpRsVisibleTasks(long txnId,
                    List<TTabletCommitInfo> commitInfos, Map<Long, Long> partitionVersionMap,
                    long updateVersionVisibleTime, String loadClusterId,
                    List<Long> lastActiveTabletIds, List<Long> lastActiveEpochs) {
                Assertions.assertEquals("cluster_b", loadClusterId);
                Assertions.assertEquals(List.of(10001L), lastActiveTabletIds);
                Assertions.assertEquals(List.of(13L), lastActiveEpochs);
                notified.set(true);
            }
        };
        Method commitTxnMethod = CloudGlobalTransactionMgr.class.getDeclaredMethod(
                "commitTxn", Cloud.CommitTxnRequest.class, long.class, boolean.class,
                List.class, List.class);
        commitTxnMethod.setAccessible(true);

        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(proxy);
            Mockito.when(proxy.commitTxn(Mockito.any())).thenReturn(response);

            InvocationTargetException exception = Assertions.assertThrows(
                    InvocationTargetException.class,
                    () -> commitTxnMethod.invoke(transactionMgr, Cloud.CommitTxnRequest.newBuilder().build(),
                            12351L, true, null, Collections.emptyList()));

            Assertions.assertInstanceOf(UserException.class, exception.getCause());
            Assertions.assertTrue(notified.get());
        }
    }

    @Test
    public void testOwnerRefreshFansOutToEveryAliveBackendInLoadCluster() throws Exception {
        List<AgentTask> submittedTasks = new ArrayList<>();
        CloudGlobalTransactionMgr transactionMgr = new CloudGlobalTransactionMgr() {
            @Override
            protected List<Backend> getAliveBackendsByClusterId(String clusterId) {
                Assertions.assertEquals("cluster_b", clusterId);
                return List.of(new Backend(1L, "host1", 9000), new Backend(2L, "host2", 9000));
            }

            @Override
            protected void submitMakeCloudTmpRsVisibleTasks(AgentBatchTask batchTask) {
                submittedTasks.addAll(batchTask.getAllTasks());
            }
        };
        TTabletCommitInfo writer = new TTabletCommitInfo();
        writer.setBackendId(1L);
        writer.setTabletId(10001L);

        transactionMgr.sendMakeCloudTmpRsVisibleTasks(12350L, List.of(writer), Map.of(10L, 20L),
                30L, "cluster_b", List.of(10001L, 10002L), List.of(11L, 12L));

        Assertions.assertEquals(2, submittedTasks.size());
        Map<Long, MakeCloudTmpRsVisibleTask> tasksByBackend = submittedTasks.stream()
                .map(MakeCloudTmpRsVisibleTask.class::cast)
                .collect(Collectors.toMap(AgentTask::getBackendId, task -> task));
        Assertions.assertEquals(List.of(10001L), tasksByBackend.get(1L).toThrift().getTabletIds());
        Assertions.assertEquals(Collections.emptyList(), tasksByBackend.get(2L).toThrift().getTabletIds());
        for (MakeCloudTmpRsVisibleTask task : tasksByBackend.values()) {
            Assertions.assertEquals(List.of(10001L, 10002L), task.toThrift().getLastActiveTabletIds());
            Assertions.assertEquals(List.of(11L, 12L), task.toThrift().getLastActiveEpochs());
        }
    }

    @Test
    public void testOwnerBackendLookupFailureStillSubmitsVisibilityTask() {
        List<AgentTask> submittedTasks = new ArrayList<>();
        CloudGlobalTransactionMgr transactionMgr = new CloudGlobalTransactionMgr() {
            @Override
            protected List<Backend> getAliveBackendsByClusterId(String clusterId) throws AnalysisException {
                throw new AnalysisException("lookup failed");
            }

            @Override
            protected void submitMakeCloudTmpRsVisibleTasks(AgentBatchTask batchTask) {
                submittedTasks.addAll(batchTask.getAllTasks());
            }
        };
        TTabletCommitInfo writer = new TTabletCommitInfo();
        writer.setBackendId(1L);
        writer.setTabletId(10001L);

        transactionMgr.sendMakeCloudTmpRsVisibleTasks(12352L, List.of(writer), Map.of(),
                30L, "cluster_b", List.of(10001L), List.of(11L));

        Assertions.assertEquals(1, submittedTasks.size());
        MakeCloudTmpRsVisibleTask task = (MakeCloudTmpRsVisibleTask) submittedTasks.get(0);
        Assertions.assertEquals(List.of(10001L), task.toThrift().getTabletIds());
        Assertions.assertFalse(task.toThrift().isSetLastActiveTabletIds());
    }

    @Test
    public void testBeCoordinatorUsesBackendCluster() throws Exception {
        Backend backend = new Backend(42L, "host", 9000);
        backend.setTagMap(Map.of(Tag.TYPE_LOCATION, Tag.VALUE_DEFAULT_TAG,
                Tag.CLOUD_CLUSTER_ID, "cluster_b"));
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfoService.getBackend(42L)).thenReturn(backend);
        SystemInfoService originalSystemInfoService = Env.getCurrentSystemInfo();
        FakeEnv.setSystemInfo(systemInfoService);
        try {
            Method method = CloudGlobalTransactionMgr.class.getDeclaredMethod(
                    "getLoadClusterId", TransactionState.TxnCoordinator.class, String.class);
            method.setAccessible(true);
            TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(
                    TransactionState.TxnSourceType.BE, 42L, "host", 1L);

            Assertions.assertEquals("cluster_b", method.invoke(new CloudGlobalTransactionMgr(),
                    coordinator, "label"));
        } finally {
            FakeEnv.setSystemInfo(originalSystemInfoService);
        }
    }

    @Test
    public void testCommitTransactionAlreadyVisible() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
            txnInfoBuilder.setDbId(CatalogTestUtil.testTableId1);
            txnInfoBuilder.addAllTableIds(Lists.newArrayList(CatalogTestUtil.testTableId1));
            txnInfoBuilder.setLabel(CatalogTestUtil.testTxnLabel1);
            txnInfoBuilder.setListenerId(-1);
            CommitTxnResponse response = CommitTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.TXN_ALREADY_VISIBLE).setMsg("txn already visible"))
                    .setTxnInfo(txnInfoBuilder.build())
                    .build();
            Mockito.doReturn(response).when(mockProxy).commitTxn(Mockito.any());

            long transactionId = 123533;
            Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                    .getTableOrMetaException(CatalogTestUtil.testTableId1);
            masterTransMgr.commitTransactionWithoutLock(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1),
                    transactionId, null, null);
        }
    }

    @Test
    public void testCommitTransactionAlreadyAborted() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
            txnInfoBuilder.setDbId(CatalogTestUtil.testTableId1);
            txnInfoBuilder.addAllTableIds(Lists.newArrayList(CatalogTestUtil.testTableId1));
            txnInfoBuilder.setLabel(CatalogTestUtil.testTxnLabel1);
            txnInfoBuilder.setListenerId(-1);
            CommitTxnResponse response = CommitTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.TXN_ALREADY_ABORTED).setMsg("txn already aborted"))
                    .setTxnInfo(txnInfoBuilder.build())
                    .build();
            Mockito.doReturn(response).when(mockProxy).commitTxn(Mockito.any());

            Assertions.assertThrows(UserException.class,
                    () -> {
                            long transactionId = 123533;
                            Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                                    .getTableOrMetaException(CatalogTestUtil.testTableId1);
                            masterTransMgr.commitTransactionWithoutLock(
                                    CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId, null, null);
                });
        }
    }

    @Test
    public void testCommitTransactionConflict() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            final int[] times = {1};
            Mockito.doAnswer(invocation -> {
                CommitTxnResponse.Builder commitTxnResponseBuilder = CommitTxnResponse.newBuilder();
                if (times[0] > 5) {
                    TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
                    txnInfoBuilder.setDbId(CatalogTestUtil.testTableId1);
                    txnInfoBuilder.addAllTableIds(Lists.newArrayList(CatalogTestUtil.testTableId1));
                    txnInfoBuilder.setLabel(CatalogTestUtil.testTxnLabel1);
                    txnInfoBuilder.setListenerId(-1);
                    commitTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.TXN_ALREADY_VISIBLE).setMsg("txn already visible"))
                            .setTxnInfo(txnInfoBuilder.build());
                } else {
                    commitTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.KV_TXN_CONFLICT).setMsg("kv txn conflict"));
                }
                times[0]++;
                return commitTxnResponseBuilder.build();
            }).when(mockProxy).commitTxn(Mockito.any());
            long transactionId = 123533;
            Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                    .getTableOrMetaException(CatalogTestUtil.testTableId1);
            masterTransMgr.commitTransactionWithoutLock(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1),
                    transactionId, null, null);
        }
    }

    @Test
    public void testAbortTransaction() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            long transactionId = 123533;
            Cloud.GetTxnResponse getTxnResponse = Cloud.GetTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"))
                    .setTxnInfo(buildTxnInfo(transactionId))
                    .build();
            AbortTxnResponse abortTxnResponse = AbortTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"))
                    .build();
            Mockito.doReturn(getTxnResponse).when(mockProxy).getTxn(Mockito.any());
            Mockito.doReturn(abortTxnResponse).when(mockProxy).abortTxn(Mockito.any());
            masterTransMgr.abortTransaction(CatalogTestUtil.testDbId1, transactionId, "User Cancelled");
        }
    }

    @Test
    public void testAbortRoutineLoadTransactionWithAttachment() throws Exception {
        long transactionId = 123534;
        long jobId = 1001;
        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment(
                Cloud.RLTaskTxnCommitAttachmentPB.newBuilder()
                        .setJobId(jobId)
                        .setTaskId(Cloud.UniqueIdPB.newBuilder().setHi(1).setLo(2))
                        .setProgress(Cloud.RoutineLoadProgressPB.newBuilder())
                        .setFirstErrorMsg("invalid source row")
                        .build());
        TxnStateChangeCallback callback = Mockito.mock(TxnStateChangeCallback.class);
        Mockito.when(callback.getId()).thenReturn(jobId);
        masterTransMgr.getCallbackFactory().addCallback(callback);

        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            Mockito.doAnswer(invocation -> {
                Cloud.AbortTxnRequest request = invocation.getArgument(0);
                Assertions.assertTrue(request.hasCommitAttachment());
                Assertions.assertEquals("invalid source row", request.getCommitAttachment()
                        .getRlTaskTxnCommitAttachment().getFirstErrorMsg());
                return AbortTxnResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setTxnInfo(buildTxnInfo(transactionId).toBuilder()
                                .setStatus(Cloud.TxnStatusPB.TXN_STATUS_ABORTED)
                                .setReason("data quality error")
                                .setCommitAttachment(request.getCommitAttachment()))
                        .build();
            }).when(mockProxy).abortTxn(Mockito.any());

            masterTransMgr.abortTransaction(CatalogTestUtil.testDbId1, transactionId,
                    "data quality error", attachment, Lists.newArrayList());

            ArgumentCaptor<TransactionState> txnStateCaptor = ArgumentCaptor.forClass(TransactionState.class);
            Mockito.verify(callback).afterAborted(txnStateCaptor.capture(), Mockito.eq(true),
                    Mockito.eq("data quality error"));
            RLTaskTxnCommitAttachment callbackAttachment =
                    (RLTaskTxnCommitAttachment) txnStateCaptor.getValue().getTxnCommitAttachment();
            Assertions.assertEquals("invalid source row", callbackAttachment.getFirstErrorMsg());
        } finally {
            masterTransMgr.getCallbackFactory().removeCallback(jobId);
        }
    }

    @Test
    public void testAbortTransactionByLabel() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            AbortTxnResponse response = AbortTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"))
                    .build();
            Mockito.doReturn(response).when(mockProxy).abortTxn(Mockito.any());
            masterTransMgr.abortTransaction(CatalogTestUtil.testDbId1, CatalogTestUtil.testTxnLabel1, "User Cancelled");
        }
    }

    @Test
    public void testAbortTransactionConflict() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            long transactionId = 123533;
            Cloud.GetTxnResponse getTxnResponse = Cloud.GetTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"))
                    .setTxnInfo(buildTxnInfo(transactionId))
                    .build();
            final int[] times = {1};
            Mockito.doReturn(getTxnResponse).when(mockProxy).getTxn(Mockito.any());
            Mockito.doAnswer(invocation -> {
                AbortTxnResponse.Builder abortTxnResponseBuilder = AbortTxnResponse.newBuilder();
                if (times[0] > 5) {
                    abortTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"));
                    return abortTxnResponseBuilder.build();
                } else {
                    abortTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.KV_TXN_CONFLICT).setMsg("kv txn conflict"));
                }
                times[0]++;
                return abortTxnResponseBuilder.build();
            }).when(mockProxy).abortTxn(Mockito.any());
            masterTransMgr.abortTransaction(CatalogTestUtil.testDbId1, transactionId, "User Cancelled");
        }
    }

    @Test
    public void testAbortTransactionByLabelConflict() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            final int[] times = {1};
            Mockito.doAnswer(invocation -> {
                AbortTxnResponse.Builder abortTxnResponseBuilder = AbortTxnResponse.newBuilder();
                if (times[0] > 5) {
                    abortTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"));
                    return abortTxnResponseBuilder.build();
                } else {
                    abortTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.KV_TXN_CONFLICT).setMsg("kv txn conflict"));
                }
                times[0]++;
                return abortTxnResponseBuilder.build();
            }).when(mockProxy).abortTxn(Mockito.any());
            masterTransMgr.abortTransaction(CatalogTestUtil.testDbId1, CatalogTestUtil.testTxnLabel1, "User Cancelled");
        }
    }

    @Test
    public void testIsPreviousTransactionsFinished() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            CheckTxnConflictResponse response = CheckTxnConflictResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"))
                    .setFinished(true)
                    .build();
            Mockito.doReturn(response).when(mockProxy).checkTxnConflict(Mockito.any());
            boolean result = masterTransMgr.isPreviousTransactionsFinished(12131231,
                    CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1));
            Assertions.assertEquals(result, true);
        }
    }

    @Test
    public void testIsPreviousTransactionsFinishedException() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            CheckTxnConflictResponse response = CheckTxnConflictResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"))
                    .setFinished(false)
                    .build();
            Mockito.doReturn(response).when(mockProxy).checkTxnConflict(Mockito.any());
            boolean result = masterTransMgr.isPreviousTransactionsFinished(12131231,
                    CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1));
            Assertions.assertEquals(result, false);
        }
    }

    @Test
    public void testGetNextTransactionId() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            GetCurrentMaxTxnResponse response = GetCurrentMaxTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"))
                    .setCurrentMaxTxnId(1000)
                    .build();
            Mockito.doReturn(response).when(mockProxy).getCurrentMaxTxnId(Mockito.any());
            long result = masterTransMgr.getNextTransactionId();
            Assertions.assertEquals(1000, result);
        }
    }

    @Test
    public void testGetTransactionIdWatermarkUsesExclusiveMetaServiceBound() throws Exception {
        MetaServiceProxy mockProxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockProxy);
            GetCurrentMaxTxnResponse response = GetCurrentMaxTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"))
                    .setCurrentMaxTxnId(1000)
                    .build();
            Mockito.doReturn(response).when(mockProxy).getCurrentMaxTxnId(Mockito.any());

            long result = masterTransMgr.getTransactionIdWatermark();

            Assertions.assertEquals(1001, result);
        }
    }

    private TxnInfoPB buildTxnInfo(long transactionId) {
        return TxnInfoPB.newBuilder()
                .setDbId(CatalogTestUtil.testDbId1)
                .addAllTableIds(Lists.newArrayList(CatalogTestUtil.testTableId1))
                .setTxnId(transactionId)
                .setLabel(CatalogTestUtil.testTxnLabel1)
                .setListenerId(0L)
                .setStatus(Cloud.TxnStatusPB.TXN_STATUS_PREPARED)
                .build();
    }
}

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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.CloudFEVersionSynchronizer;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.AbortTxnResponse;
import org.apache.doris.cloud.proto.Cloud.BeginTxnResponse;
import org.apache.doris.cloud.proto.Cloud.CheckTxnConflictResponse;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.cloud.proto.Cloud.GetCurrentMaxTxnResponse;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.TxnInfoPB;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.rpc.VersionHelper;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.GenericPool;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TCloudVersionInfo;
import org.apache.doris.thrift.TFrontendSyncCloudVersionRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.transaction.TxnStateChangeCallback;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CloudGlobalTransactionMgrTest {

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static GlobalTransactionMgrIface masterTransMgr;
    private static Env masterEnv;

    private TransactionState.TxnCoordinator transactionSource = new TransactionState.TxnCoordinator(
            TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException {

        Config.cloud_unique_id = "cloud_unique_id";
        Config.meta_service_endpoint = "127.0.0.1:20121";
        fakeEditLog = new FakeEditLog();
        fakeEnv = new FakeEnv();
        Env catalog = CatalogTestUtil.createTestCatalog();
        masterEnv = Mockito.mock(CloudEnv.class, AdditionalAnswers.delegatesTo(catalog));
        Mockito.doReturn(new CloudFEVersionSynchronizer()).when((CloudEnv) masterEnv).getCloudFEVersionSynchronizer();
        // Env.getCurrentGlobalTransactionMgr reads the field directly rather than calling the delegated getter.
        Deencapsulation.setField(masterEnv, "globalTransactionMgr", catalog.getGlobalTransactionMgr());
        FakeEnv.setEnv(masterEnv);
        FakeEnv.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        masterTransMgr = masterEnv.getGlobalTransactionMgr();
    }

    @After
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testBeginTransaction() throws LabelAlreadyUsedException, AnalysisException,
            BeginTransactionException, DuplicatedRequestException, QuotaExceedException, MetaNotFoundException {
        AtomicLong id = new AtomicLong(1000);
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.BeginTxnResponse beginTxn(Cloud.BeginTxnRequest request) {
                BeginTxnResponse.Builder beginTxnResponseBuilder = BeginTxnResponse.newBuilder();
                beginTxnResponseBuilder.setTxnId(id.getAndIncrement())
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK).setMsg("OK"));

                return beginTxnResponseBuilder.build();
            }
        };

        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel1,
                transactionSource,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        Assert.assertEquals(transactionId + 1, id.get());
    }

    @Test
    public void testBeginTransactionConflict() throws LabelAlreadyUsedException, AnalysisException,
            BeginTransactionException, DuplicatedRequestException, QuotaExceedException, MetaNotFoundException {
        AtomicLong id = new AtomicLong(1000);
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            int times = 1;
            @Mock
            public Cloud.BeginTxnResponse beginTxn(Cloud.BeginTxnRequest request) {
                BeginTxnResponse.Builder beginTxnResponseBuilder = BeginTxnResponse.newBuilder();
                if (times > 5) {
                    beginTxnResponseBuilder.setTxnId(id.getAndIncrement())
                            .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK).setMsg("OK"));
                } else {
                    beginTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.KV_TXN_CONFLICT).setMsg("kv txn conflict"));
                }
                times++;
                return beginTxnResponseBuilder.build();
            }
        };

        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel1,
                transactionSource,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        Assert.assertEquals(transactionId + 1, id.get());
    }

    @Test
    public void testBeginTransactionLabelAlreadyUsedException() throws LabelAlreadyUsedException, AnalysisException,
            BeginTransactionException, DuplicatedRequestException, QuotaExceedException, MetaNotFoundException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.BeginTxnResponse beginTxn(Cloud.BeginTxnRequest request) {
                BeginTxnResponse.Builder beginTxnResponseBuilder = BeginTxnResponse.newBuilder();
                beginTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.TXN_LABEL_ALREADY_USED).setMsg("label already used"));
                return beginTxnResponseBuilder.build();
            }
        };

        Assertions.assertThrows(LabelAlreadyUsedException.class,
                () -> {
                        masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                                CatalogTestUtil.testTxnLabel1,
                                transactionSource,
                                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
            });
    }

    @Test
    public void testBeginTransactionDuplicatedRequestException() throws LabelAlreadyUsedException, AnalysisException,
            BeginTransactionException, DuplicatedRequestException, QuotaExceedException, MetaNotFoundException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.BeginTxnResponse beginTxn(Cloud.BeginTxnRequest request) {
                BeginTxnResponse.Builder beginTxnResponseBuilder = BeginTxnResponse.newBuilder();
                beginTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.TXN_DUPLICATED_REQ).setMsg("duplicated request"));
                return beginTxnResponseBuilder.build();
            }
        };

        Assertions.assertThrows(DuplicatedRequestException.class,
                () -> {
                        masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                                CatalogTestUtil.testTxnLabel1,
                                transactionSource,
                                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
            });
    }

    @Test
    public void testCommitTransaction() throws UserException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
                txnInfoBuilder.setDbId(CatalogTestUtil.testTableId1);
                txnInfoBuilder.addAllTableIds(Lists.newArrayList(CatalogTestUtil.testTableId1));
                txnInfoBuilder.setLabel(CatalogTestUtil.testTxnLabel1);
                txnInfoBuilder.setListenerId(-1);
                CommitTxnResponse.Builder commitTxnResponseBuilder = CommitTxnResponse.newBuilder();
                commitTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setTxnInfo(txnInfoBuilder.build());
                return commitTxnResponseBuilder.build();
            }
        };

        long transactionId = 123533;
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransactionWithoutLock(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1),
                transactionId, null, null);
    }

    @Test
    public void testSkipMakeTmpRsVisibleForIncompleteLazyCommit() throws Exception {
        CommitTxnResponse response = CommitTxnResponse.newBuilder()
                .setTxnInfo(TxnInfoPB.newBuilder().setTxnId(12345L).build())
                .setIsLazyCommit(true)
                .setIsLazyCommitIncomplete(true)
                .build();

        Assert.assertFalse(invokeNotifyBesMakeTmpRsVisible(response));
    }

    @Test
    public void testMakeTmpRsVisibleForNonLazyCommitWithIncompleteFlag() throws Exception {
        CommitTxnResponse response = CommitTxnResponse.newBuilder()
                .setTxnInfo(TxnInfoPB.newBuilder().setTxnId(12346L).build())
                .setIsLazyCommit(false)
                .setIsLazyCommitIncomplete(true)
                .build();

        Assert.assertTrue(invokeNotifyBesMakeTmpRsVisible(response));
    }

    @Test
    public void testMakeTmpRsVisibleForCompletedLazyCommit() throws Exception {
        CommitTxnResponse response = CommitTxnResponse.newBuilder()
                .setTxnInfo(TxnInfoPB.newBuilder().setTxnId(12347L).build())
                .setIsLazyCommit(true)
                .setIsLazyCommitIncomplete(false)
                .build();

        Assert.assertTrue(invokeNotifyBesMakeTmpRsVisible(response));
    }

    @Test
    public void testMakeTmpRsVisibleForNonLazyCommit() throws Exception {
        CommitTxnResponse response = CommitTxnResponse.newBuilder()
                .setTxnInfo(TxnInfoPB.newBuilder().setTxnId(12348L).build())
                .setIsLazyCommit(false)
                .setIsLazyCommitIncomplete(false)
                .build();

        Assert.assertTrue(invokeNotifyBesMakeTmpRsVisible(response));
    }

    private boolean invokeNotifyBesMakeTmpRsVisible(CommitTxnResponse response) throws Exception {
        boolean originalEnableNotify = Config.enable_notify_be_after_load_txn_commit;
        try {
            Config.enable_notify_be_after_load_txn_commit = true;
            AtomicBoolean notified = new AtomicBoolean(false);
            CloudGlobalTransactionMgr transactionMgr = new CloudGlobalTransactionMgr() {
                @Override
                public void sendMakeCloudTmpRsVisibleTasks(long txnId,
                        List<TTabletCommitInfo> commitInfos, Map<Long, Long> partitionVersionMap,
                        long updateVersionVisibleTime) {
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
    public void testCommitTransactionAlreadyVisible() throws UserException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
                txnInfoBuilder.setDbId(CatalogTestUtil.testTableId1);
                txnInfoBuilder.addAllTableIds(Lists.newArrayList(CatalogTestUtil.testTableId1));
                txnInfoBuilder.setLabel(CatalogTestUtil.testTxnLabel1);
                txnInfoBuilder.setListenerId(-1);
                CommitTxnResponse.Builder commitTxnResponseBuilder = CommitTxnResponse.newBuilder();
                commitTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.TXN_ALREADY_VISIBLE).setMsg("txn already visible"))
                        .setTxnInfo(txnInfoBuilder.build());
                return commitTxnResponseBuilder.build();
            }
        };

        long transactionId = 123533;
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransactionWithoutLock(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1),
                transactionId, null, null);
    }

    @Test
    public void testCommitTransactionAlreadyAborted() throws UserException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
                txnInfoBuilder.setDbId(CatalogTestUtil.testTableId1);
                txnInfoBuilder.addAllTableIds(Lists.newArrayList(CatalogTestUtil.testTableId1));
                txnInfoBuilder.setLabel(CatalogTestUtil.testTxnLabel1);
                txnInfoBuilder.setListenerId(-1);
                CommitTxnResponse.Builder commitTxnResponseBuilder = CommitTxnResponse.newBuilder();
                commitTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.TXN_ALREADY_ABORTED).setMsg("txn already aborted"))
                        .setTxnInfo(txnInfoBuilder.build());
                return commitTxnResponseBuilder.build();
            }
        };

        Assertions.assertThrows(UserException.class,
                () -> {
                        long transactionId = 123533;
                        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                                .getTableOrMetaException(CatalogTestUtil.testTableId1);
                        masterTransMgr.commitTransactionWithoutLock(
                                CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId, null, null);
            });
    }

    @Test
    public void testCommitTransactionConflict() throws UserException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            int times = 1;
            @Mock
            public Cloud.CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
                CommitTxnResponse.Builder commitTxnResponseBuilder = CommitTxnResponse.newBuilder();
                if (times > 5) {
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
                times++;
                return commitTxnResponseBuilder.build();
            }
        };
        long transactionId = 123533;
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransactionWithoutLock(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1),
                transactionId, null, null);
    }

    @Test
    public void testAbortTransaction() throws UserException {
        long transactionId = 123533;
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.GetTxnResponse getTxn(Cloud.GetTxnRequest request) {
                return Cloud.GetTxnResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setTxnInfo(buildTxnInfo(transactionId))
                        .build();
            }

            @Mock
            public Cloud.AbortTxnResponse abortTxn(Cloud.AbortTxnRequest request) {
                AbortTxnResponse.Builder abortTxnResponseBuilder = AbortTxnResponse.newBuilder();
                abortTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"));
                return abortTxnResponseBuilder.build();
            }
        };
        masterTransMgr.abortTransaction(CatalogTestUtil.testDbId1, transactionId, "User Cancelled");
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
                Assert.assertTrue(request.hasCommitAttachment());
                Assert.assertEquals("invalid source row", request.getCommitAttachment()
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
            Assert.assertEquals("invalid source row", callbackAttachment.getFirstErrorMsg());
        } finally {
            masterTransMgr.getCallbackFactory().removeCallback(jobId);
        }
    }

    @Test
    public void testAbortTransactionByLabel() throws UserException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.AbortTxnResponse abortTxn(Cloud.AbortTxnRequest request) {
                AbortTxnResponse.Builder abortTxnResponseBuilder = AbortTxnResponse.newBuilder();
                abortTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"));
                return abortTxnResponseBuilder.build();
            }
        };
        masterTransMgr.abortTransaction(CatalogTestUtil.testDbId1, CatalogTestUtil.testTxnLabel1, "User Cancelled");
    }

    @Test
    public void testAbortTransactionConflict() throws UserException {
        long transactionId = 123533;
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            int times = 1;

            @Mock
            public Cloud.GetTxnResponse getTxn(Cloud.GetTxnRequest request) {
                return Cloud.GetTxnResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setTxnInfo(buildTxnInfo(transactionId))
                        .build();
            }

            @Mock
            public Cloud.AbortTxnResponse abortTxn(Cloud.AbortTxnRequest request) {
                AbortTxnResponse.Builder abortTxnResponseBuilder = AbortTxnResponse.newBuilder();
                if (times > 5) {
                    abortTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"));
                    return abortTxnResponseBuilder.build();
                } else {
                    abortTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.KV_TXN_CONFLICT).setMsg("kv txn conflict"));
                }
                times++;
                return abortTxnResponseBuilder.build();
            }
        };
        masterTransMgr.abortTransaction(CatalogTestUtil.testDbId1, transactionId, "User Cancelled");
    }

    @Test
    public void testAbortTransactionByLabelConflict() throws UserException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            int times = 1;
            @Mock
            public Cloud.AbortTxnResponse abortTxn(Cloud.AbortTxnRequest request) {
                AbortTxnResponse.Builder abortTxnResponseBuilder = AbortTxnResponse.newBuilder();
                if (times > 5) {
                    abortTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.OK).setMsg("OK"));
                    return abortTxnResponseBuilder.build();
                } else {
                    abortTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(MetaServiceCode.KV_TXN_CONFLICT).setMsg("kv txn conflict"));
                }
                times++;
                return abortTxnResponseBuilder.build();
            }
        };
        masterTransMgr.abortTransaction(CatalogTestUtil.testDbId1, CatalogTestUtil.testTxnLabel1, "User Cancelled");
    }

    @Test
    public void testIsPreviousTransactionsFinished() throws UserException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.CheckTxnConflictResponse checkTxnConflict(Cloud.CheckTxnConflictRequest request) {
                CheckTxnConflictResponse.Builder checkTxnConflictResponseBuilder = CheckTxnConflictResponse.newBuilder();
                checkTxnConflictResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setFinished(true);
                return checkTxnConflictResponseBuilder.build();
            }
        };
        boolean result = masterTransMgr.isPreviousTransactionsFinished(12131231,
                CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1));
        Assert.assertEquals(result, true);
    }

    @Test
    public void testIsPreviousTransactionsFinishedException() throws UserException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.CheckTxnConflictResponse checkTxnConflict(Cloud.CheckTxnConflictRequest request) {
                CheckTxnConflictResponse.Builder checkTxnConflictResponseBuilder = CheckTxnConflictResponse.newBuilder();
                checkTxnConflictResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setFinished(false);
                return checkTxnConflictResponseBuilder.build();
            }
        };
        boolean result = masterTransMgr.isPreviousTransactionsFinished(12131231,
                CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1));
        Assert.assertEquals(result, false);
    }

    @Test
    public void testGetNextTransactionId() throws UserException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.GetCurrentMaxTxnResponse getCurrentMaxTxnId(Cloud.GetCurrentMaxTxnRequest request) {
                GetCurrentMaxTxnResponse.Builder getCurrentMaxTxnResponseBuilder = GetCurrentMaxTxnResponse.newBuilder();
                getCurrentMaxTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"))
                        .setCurrentMaxTxnId(1000);
                return getCurrentMaxTxnResponseBuilder.build();
            }
        };
        long result = masterTransMgr.getNextTransactionId();
        Assert.assertEquals(1000, result);
    }

    @Test
    public void testVisibleRetryInvalidatesTableAndPartitionVersions() throws Exception {
        useVersionCaches();
        CloudPartition first = addCloudPartition(1000);
        CloudPartition second = addCloudPartition(2000);
        OlapTable firstTable = getCloudTable(first);
        OlapTable secondTable = getCloudTable(second);
        CommitTxnResponse response = visibleRetry(Lists.newArrayList(first.getTableId(), second.getTableId()));
        try (MockedStatic<VersionHelper> versions = mockVersionHelper()) {
            versions.when(() -> VersionHelper.getVersionFromMeta(Mockito.any())).thenReturn(partitionVersion(4));
            masterTransMgr.afterCommitTxnResp(response, null, Lists.newArrayList());
            versions.verifyNoInteractions();
            Assertions.assertEquals(2, first.getCachedVisibleVersion());
            Assertions.assertEquals(2, firstTable.getCachedTableVersion());
            Assertions.assertEquals(4, first.getVisibleVersion());
            Assertions.assertEquals(Lists.newArrayList(4L), CloudPartition.getSnapshotVisibleVersion(Lists.newArrayList(second)));
            Assertions.assertEquals(4, firstTable.getVisibleVersion());
            Assertions.assertEquals(Lists.newArrayList(4L), OlapTable.getVisibleVersionInBatch(Lists.newArrayList(secondTable)));
            Assertions.assertEquals(4, first.getCachedVisibleVersion());
            Assertions.assertEquals(4, second.getCachedVisibleVersion());
            Assertions.assertEquals(40, first.getVisibleVersionTime());
            // Current versions must not become the original transaction's BE promotion outcome.
            Assertions.assertEquals(0, response.getVersionsCount());
            Assertions.assertEquals(4, first.getVisibleVersion());
            Assertions.assertEquals(Lists.newArrayList(4L), CloudPartition.getSnapshotVisibleVersion(Lists.newArrayList(second)));
            Assertions.assertEquals(4, firstTable.getVisibleVersion());
            Assertions.assertEquals(Lists.newArrayList(4L), OlapTable.getVisibleVersionInBatch(Lists.newArrayList(secondTable)));
            versions.verify(() -> VersionHelper.getVersionFromMeta(Mockito.any()), Mockito.times(4));
        }
    }

    @Test
    public void testIncompleteCommitDoesNotRefreshPartitionVersions() throws Exception {
        CloudPartition partition = addCloudPartition(1000);
        CommitTxnResponse response = visibleRetry(Lists.newArrayList(partition.getTableId()));
        try (MockedStatic<VersionHelper> versions = mockVersionHelper()) {
            masterTransMgr.afterCommitTxnResp(response.toBuilder().setTxnInfo(response.getTxnInfo().toBuilder()
                    .setStatus(Cloud.TxnStatusPB.TXN_STATUS_COMMITTED)).build(), null, Lists.newArrayList());
            masterTransMgr.afterCommitTxnResp(response.toBuilder().setIsLazyCommit(true)
                    .setIsLazyCommitIncomplete(true).build(), null, Lists.newArrayList());
            Assertions.assertEquals(2, partition.getCachedVisibleVersion());
            versions.verifyNoInteractions();
        }
    }

    @Test
    public void testVisibleRetrySkipsDroppedTable() throws Exception {
        try (MockedStatic<VersionHelper> versions = mockVersionHelper()) {
            masterTransMgr.afterCommitTxnResp(visibleRetry(Lists.newArrayList(1000L)), null, Lists.newArrayList());
            versions.verifyNoInteractions();
        }
    }

    @Test
    public void testMowVisiblePrecheckInvalidatesVersions() throws Exception {
        useVersionCaches();
        CloudPartition partition = addCloudPartition(1000);
        TxnInfoPB txnInfo = visibleRetry(Lists.newArrayList(partition.getTableId())).getTxnInfo();
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        Mockito.when(proxy.getTxn(Mockito.any())).thenReturn(Cloud.GetTxnResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK))
                .setTxnInfo(txnInfo).build());
        try (MockedStatic<MetaServiceProxy> proxyMock = Mockito.mockStatic(MetaServiceProxy.class);
                MockedStatic<VersionHelper> versions = mockVersionHelper()) {
            proxyMock.when(MetaServiceProxy::getInstance).thenReturn(proxy);
            versions.when(() -> VersionHelper.getVersionFromMeta(Mockito.any())).thenReturn(partitionVersion(4));
            Method check = CloudGlobalTransactionMgr.class.getDeclaredMethod("checkTransactionStateBeforeCommit",
                    long.class, long.class);
            check.setAccessible(true);
            Assertions.assertEquals(false, check.invoke(masterTransMgr, txnInfo.getDbId(), txnInfo.getTxnId()));
            versions.verifyNoInteractions();
            Assertions.assertEquals(4, partition.getVisibleVersion());
            Assertions.assertEquals(4, getCloudTable(partition).getVisibleVersion());
            Assertions.assertEquals(4, partition.getCachedVisibleVersion());
        }
    }

    @Test
    public void testRefreshFailurePreservesCommitCallbacks() throws Exception {
        useVersionCaches();
        CloudPartition partition = addCloudPartition(1000);
        CommitTxnResponse retry = visibleRetry(Lists.newArrayList(partition.getTableId()));
        CommitTxnResponse response = retry.toBuilder()
                .setTxnInfo(retry.getTxnInfo().toBuilder().setListenerId(42)).build();
        TxnStateChangeCallback callback = Mockito.mock(TxnStateChangeCallback.class);
        Mockito.when(callback.getId()).thenReturn(42L);
        masterTransMgr.getCallbackFactory().addCallback(callback);
        Mockito.clearInvocations(callback);
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        Mockito.when(proxy.commitTxn(Mockito.any())).thenReturn(response);
        Table table = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(partition.getTableId());
        try (MockedStatic<MetaServiceProxy> proxyMock = Mockito.mockStatic(MetaServiceProxy.class);
                MockedStatic<VersionHelper> versions = mockVersionHelper()) {
            proxyMock.when(MetaServiceProxy::getInstance).thenReturn(proxy);
            versions.when(() -> VersionHelper.getVersionFromMeta(Mockito.any()))
                    .thenThrow(new RpcException("MS", "unavailable"));
            Assertions.assertDoesNotThrow(() -> masterTransMgr.commitTransactionWithoutLock(CatalogTestUtil.testDbId1,
                    Lists.newArrayList(table), response.getTxnInfo().getTxnId(), null, null));
            Assertions.assertEquals(2, partition.getCachedVisibleVersion());
            versions.verifyNoInteractions();
            Mockito.verify(callback).afterCommitted(Mockito.argThat(state ->
                    state.getTransactionStatus() == TransactionStatus.VISIBLE), Mockito.eq(true));
            Mockito.verify(callback).afterVisible(Mockito.argThat(state ->
                    state.getTransactionStatus() == TransactionStatus.VISIBLE), Mockito.eq(true));
            // A version outage fails the following read, not the already committed transaction.
            Assertions.assertThrows(RuntimeException.class, partition::getVisibleVersion);
            Assertions.assertThrows(RpcException.class, ((OlapTable) table)::getVisibleVersion);
            versions.when(() -> VersionHelper.getVersionFromMeta(Mockito.any())).thenReturn(partitionVersion(4));
            Assertions.assertEquals(4, partition.getVisibleVersion());
            Assertions.assertEquals(4, ((OlapTable) table).getVisibleVersion());
            Mockito.verifyNoMoreInteractions(callback);
        }
    }

    @Test
    public void testSinglePartitionReadCannotClearConcurrentInvalidation() throws Exception {
        checkReadCannotClearConcurrentInvalidation(false, false);
    }

    @Test
    public void testBatchPartitionReadCannotClearConcurrentInvalidation() throws Exception {
        checkReadCannotClearConcurrentInvalidation(false, true);
    }

    @Test
    public void testSingleTableReadCannotClearConcurrentInvalidation() throws Exception {
        checkReadCannotClearConcurrentInvalidation(true, false);
    }

    @Test
    public void testBatchTableReadCannotClearConcurrentInvalidation() throws Exception {
        checkReadCannotClearConcurrentInvalidation(true, true);
    }

    private void checkReadCannotClearConcurrentInvalidation(boolean tableVersion, boolean batch) throws Exception {
        useVersionCaches();
        CloudPartition partition = addCloudPartition(1000);
        OlapTable table = getCloudTable(partition);
        ConnectContext.get().getSessionVariable().cloudPartitionVersionCacheTtlMs = 0;
        ConnectContext.get().getSessionVariable().cloudTableVersionCacheTtlMs = 0;
        try (MockedStatic<VersionHelper> versions = mockVersionHelper()) {
            versions.when(() -> VersionHelper.getVersionFromMeta(Mockito.any())).thenAnswer(invocation -> {
                // The MS snapshot predates the commit, but its reply arrives after cache invalidation.
                masterTransMgr.afterCommitTxnResp(visibleRetry(Lists.newArrayList(table.getId())), null, Lists.newArrayList());
                useVersionCaches();
                return partitionVersion(2);
            });
            if (tableVersion) {
                Assertions.assertEquals(2L, batch
                        ? OlapTable.getVisibleVersionInBatch(Lists.newArrayList(table)).get(0) : table.getVisibleVersion());
            } else {
                Assertions.assertEquals(2L, batch
                        ? CloudPartition.getSnapshotVisibleVersion(Lists.newArrayList(partition)).get(0)
                        : partition.getVisibleVersion());
            }
            // A delayed ordinary commit notification cannot repair an unknown version either.
            table.setCachedTableVersion(3);
            partition.setCachedVisibleVersion(3, 30);
            versions.when(() -> VersionHelper.getVersionFromMeta(Mockito.any())).thenReturn(partitionVersion(4));
            if (tableVersion) {
                Assertions.assertEquals(4, table.getVisibleVersion());
                Assertions.assertEquals(4, table.getVisibleVersion());
            } else {
                Assertions.assertEquals(4, partition.getVisibleVersion());
                Assertions.assertEquals(4, partition.getVisibleVersion());
            }
            versions.verify(() -> VersionHelper.getVersionFromMeta(Mockito.any()), Mockito.times(2));
        }
    }

    @Test
    public void testInvalidationWaitsForVersionSnapshotReaders() throws Exception {
        CloudPartition partition = addCloudPartition(1000);
        OlapTable table = getCloudTable(partition);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        table.versionReadLock();
        CompletableFuture<Void> invalidation;
        try {
            invalidation = CompletableFuture.runAsync(() -> {
                started.countDown();
                ((CloudEnv) masterEnv).getCloudFEVersionSynchronizer()
                        .invalidateVersionCaches(CatalogTestUtil.testDbId1, Lists.newArrayList(table.getId()));
            }, executor);
            Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
            Assertions.assertThrows(TimeoutException.class, () -> invalidation.get(200, TimeUnit.MILLISECONDS));
        } finally {
            table.versionReadUnlock();
            executor.shutdown();
        }
        invalidation.get(5, TimeUnit.SECONDS);
        Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    public void testBatchRefreshInstallsVersionsUnderWriteLock() throws Exception {
        CloudPartition first = addCloudPartition(1000);
        OlapTable table = getCloudTable(first);
        CloudPartition second = Mockito.spy(new CloudPartition(1002, "p2", new MaterializedIndex(),
                new RandomDistributionInfo(1), CatalogTestUtil.testDbId1, table.getId()));
        table.addPartition(second);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<CompletableFuture<Void>> snapshotReader = new AtomicReference<>();
        try (MockedStatic<VersionHelper> versions = mockVersionHelper()) {
            versions.when(() -> VersionHelper.getVersionFromMeta(Mockito.any())).thenReturn(
                    partitionVersion(4).toBuilder().addVersions(4).addVersionUpdateTimeMs(40).build());
            Mockito.doAnswer(invocation -> {
                CountDownLatch started = new CountDownLatch(1);
                CompletableFuture<Void> reader = CompletableFuture.runAsync(() -> {
                    started.countDown();
                    table.versionReadLock();
                    try {
                        Assertions.assertEquals(4, first.getCachedVisibleVersion());
                        Assertions.assertEquals(4, second.getCachedVisibleVersion());
                    } finally {
                        table.versionReadUnlock();
                    }
                }, executor);
                snapshotReader.set(reader);
                Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
                Assertions.assertThrows(TimeoutException.class, () -> reader.get(200, TimeUnit.MILLISECONDS));
                return invocation.callRealMethod();
            }).when(second).setCachedVisibleVersion(4, 40);
            CloudPartition.getSnapshotVisibleVersionFromMs(Lists.newArrayList(first, second), false);
            snapshotReader.get().get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdown();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testFollowerInvalidatesTableAndAllPartitions() throws Exception {
        useVersionCaches();
        CloudPartition partition = addCloudPartition(1000);
        OlapTable table = getCloudTable(partition);
        CloudPartition second = new CloudPartition(1002, "p2", new MaterializedIndex(),
                new RandomDistributionInfo(1), CatalogTestUtil.testDbId1, table.getId());
        second.setCachedVisibleVersion(2, 20);
        table.addPartition(second);
        TFrontendSyncCloudVersionRequest request = new TFrontendSyncCloudVersionRequest()
                .setDbId(CatalogTestUtil.testDbId1).setPartitionVersionInfos(Lists.newArrayList())
                .setTableVersionInfos(Lists.newArrayList(new TCloudVersionInfo().setTableId(table.getId()).setVersion(-1)));
        Mockito.doReturn(false).when(masterEnv).isMaster();
        FrontendServiceImpl service = new FrontendServiceImpl(null);
        try (MockedStatic<VersionHelper> versions = mockVersionHelper()) {
            Assertions.assertEquals(TStatusCode.OK, service.syncCloudVersion(request).getStatusCode());
            versions.verifyNoInteractions();
            // Even a delayed regular version push must leave these caches invalid.
            service.syncCloudVersion(new TFrontendSyncCloudVersionRequest()
                    .setDbId(CatalogTestUtil.testDbId1).setPartitionVersionInfos(Lists.newArrayList())
                    .setTableVersionInfos(Lists.newArrayList(new TCloudVersionInfo().setTableId(table.getId()).setVersion(3))));
            versions.when(() -> VersionHelper.getVersionFromMeta(Mockito.any())).thenReturn(partitionVersion(4));
            Assertions.assertEquals(4, table.getVisibleVersion());
            Assertions.assertEquals(4, partition.getVisibleVersion());
            Assertions.assertEquals(4, second.getVisibleVersion());
            versions.verify(() -> VersionHelper.getVersionFromMeta(Mockito.any()), Mockito.times(3));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInvalidationIsPushedThroughVersionRpc() throws Exception {
        CloudPartition partition = addCloudPartition(1000);
        boolean syncEnabled = Config.cloud_enable_version_syncer;
        GenericPool<FrontendService.Client> originalPool = ClientPool.frontendVersionPool;
        GenericPool<FrontendService.Client> pool = Mockito.mock(GenericPool.class);
        FrontendService.Client client = Mockito.mock(FrontendService.Client.class);
        Mockito.when(pool.borrowObject(Mockito.any())).thenReturn(client);
        CompletableFuture<TFrontendSyncCloudVersionRequest> sent = new CompletableFuture<>();
        CountDownLatch returned = new CountDownLatch(1);
        Mockito.when(client.syncCloudVersion(Mockito.any())).thenAnswer(invocation -> {
            sent.complete(invocation.getArgument(0));
            return new TStatus(TStatusCode.OK);
        });
        Mockito.doAnswer(invocation -> {
            returned.countDown();
            return null;
        }).when(pool).returnObject(Mockito.any(), Mockito.eq(client));
        CloudEnv sender = (CloudEnv) masterEnv;
        Frontend follower = Mockito.mock(Frontend.class);
        Mockito.when(follower.isAlive()).thenReturn(true);
        Mockito.when(follower.getHost()).thenReturn("127.0.0.2");
        Mockito.when(follower.getRpcPort()).thenReturn(9020);
        Mockito.doReturn(Lists.newArrayList(follower)).when(sender).getFrontends(null);
        Mockito.doReturn(new HostInfo("127.0.0.1", 9010)).when(sender).getSelfNode();
        try {
            FakeEnv.setEnv(sender);
            Config.cloud_enable_version_syncer = true;
            ClientPool.frontendVersionPool = pool;
            masterTransMgr.afterCommitTxnResp(visibleRetry(Lists.newArrayList(partition.getTableId())), null, Lists.newArrayList());
            TFrontendSyncCloudVersionRequest request = sent.get(5, TimeUnit.SECONDS);
            Assertions.assertTrue(returned.await(5, TimeUnit.SECONDS));
            // Check the actual Thrift payload as well as the sender's Java objects.
            TFrontendSyncCloudVersionRequest decoded = new TFrontendSyncCloudVersionRequest();
            new org.apache.thrift.TDeserializer().deserialize(decoded,
                    new org.apache.thrift.TSerializer().serialize(request));
            Assertions.assertEquals(CatalogTestUtil.testDbId1, decoded.getDbId());
            Assertions.assertTrue(decoded.getPartitionVersionInfos().isEmpty());
            Assertions.assertEquals(1, decoded.getTableVersionInfosSize());
            Assertions.assertEquals(partition.getTableId(), decoded.getTableVersionInfos().get(0).getTableId());
            Assertions.assertEquals(-1, decoded.getTableVersionInfos().get(0).getVersion());
        } finally {
            FakeEnv.setEnv(masterEnv);
            ClientPool.frontendVersionPool = originalPool;
            Config.cloud_enable_version_syncer = syncEnabled;
        }
    }

    private CloudPartition addCloudPartition(long tableId) throws Exception {
        OlapTable table = new OlapTable(tableId, "version_cache_" + tableId, Lists.newArrayList(), KeysType.DUP_KEYS,
                new PartitionInfo(), new RandomDistributionInfo(1));
        CloudPartition partition = new CloudPartition(tableId + 1, "p1", new MaterializedIndex(),
                new RandomDistributionInfo(1), CatalogTestUtil.testDbId1, tableId);
        partition.setCachedVisibleVersion(2, 20);
        table.addPartition(partition);
        table.setCachedTableVersion(2);
        masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1).registerTable(table);
        return partition;
    }

    private MockedStatic<VersionHelper> mockVersionHelper() {
        MockedStatic<VersionHelper> versions = Mockito.mockStatic(VersionHelper.class);
        // Batch reads pass an explicit retry limit; share the response stub with single-version reads.
        versions.when(() -> VersionHelper.getVersionFromMeta(Mockito.any(), Mockito.anyInt()))
                .thenAnswer(invocation -> VersionHelper.getVersionFromMeta(invocation.getArgument(0)));
        return versions;
    }

    private CommitTxnResponse visibleRetry(List<Long> tableIds) {
        return CommitTxnResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK))
                .setTxnInfo(buildTxnInfo(100).toBuilder().clearTableIds().addAllTableIds(tableIds)
                        .setStatus(Cloud.TxnStatusPB.TXN_STATUS_VISIBLE)).build();
    }

    private Cloud.GetVersionResponse partitionVersion(long version) {
        return Cloud.GetVersionResponse.newBuilder().setVersion(version).addVersions(version)
                .addVersionUpdateTimeMs(version * 10).build();
    }

    private OlapTable getCloudTable(CloudPartition partition) throws Exception {
        return (OlapTable) masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(partition.getTableId());
    }

    private void useVersionCaches() {
        ConnectContext context = new ConnectContext();
        context.setSessionVariable(new SessionVariable());
        context.getSessionVariable().cloudPartitionVersionCacheTtlMs = Long.MAX_VALUE;
        context.getSessionVariable().cloudTableVersionCacheTtlMs = Long.MAX_VALUE;
        context.setThreadLocalInfo();
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

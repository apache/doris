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
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicLong;

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
        masterEnv = CatalogTestUtil.createTestCatalog();
        FakeEnv.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        masterTransMgr = masterEnv.getGlobalTransactionMgr();
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
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1),
                transactionId, null);
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
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1),
                transactionId, null);
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
                        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1),
                                transactionId, null);
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
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1),
                transactionId, null);
    }

    @Test
    public void testAbortTransaction() throws UserException {
        new MockUp<MetaServiceProxy>(MetaServiceProxy.class) {
            @Mock
            public Cloud.AbortTxnResponse abortTxn(Cloud.AbortTxnRequest request) {
                AbortTxnResponse.Builder abortTxnResponseBuilder = AbortTxnResponse.newBuilder();
                abortTxnResponseBuilder.setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(MetaServiceCode.OK).setMsg("OK"));
                return abortTxnResponseBuilder.build();
            }
        };
        long transactionId = 123533;
        masterTransMgr.abortTransaction(CatalogTestUtil.testDbId1, transactionId, "User Cancelled");
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
        long transactionId = 123533;
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
}

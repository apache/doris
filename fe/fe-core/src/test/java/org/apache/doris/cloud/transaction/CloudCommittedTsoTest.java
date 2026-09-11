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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.TxnInfoPB;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.transaction.TransactionUtil;
import org.apache.doris.tso.TSOService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CloudCommittedTsoTest {
    @Test
    public void testLazyCommitResponseDoesNotReleaseTsoUntilReallyVisible() {
        Env env = Mockito.mock(Env.class);
        TSOService tsoService = Mockito.mock(TSOService.class);
        Mockito.when(env.getTSOService()).thenReturn(tsoService);
        CommitTxnResponse.Builder response = CommitTxnResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK))
                .setTxnInfo(TxnInfoPB.newBuilder().setStatus(Cloud.TxnStatusPB.TXN_STATUS_VISIBLE))
                .setIsLazyCommit(true).setIsLazyCommitIncomplete(true);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            CloudGlobalTransactionMgr.releaseFinishedTso(1, 10, response.build());
            Mockito.verifyNoInteractions(tsoService);
            response.setIsLazyCommitIncomplete(false);
            CloudGlobalTransactionMgr.releaseFinishedTso(1, 10, response.build());
            Mockito.verify(tsoService).transactionFinished(1, 10);
            response.setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.TXN_ALREADY_ABORTED));
            CloudGlobalTransactionMgr.releaseFinishedTso(2, 20, response.build());
            Mockito.verify(tsoService).transactionFinished(2, 20);
        }
    }

    @Test
    public void testOrdinarySubTransactionAndTwoPhaseCommitReuseTsoAcrossRpcRetries() throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getId()).thenReturn(1L);
        Mockito.when(db.getTablesOnIdOrderOrThrowException(Mockito.anyList())).thenReturn(Collections.emptyList());
        Mockito.when(catalog.getDbOrMetaException(1L)).thenReturn(db);
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class);
                MockedStatic<TransactionUtil> allocation = Mockito.mockStatic(TransactionUtil.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
            allocation.when(() -> TransactionUtil.getCommitTSO(10L, db, Collections.emptySet())).thenReturn(500L);
            for (int path = 0; path < 3; path++) {
                List<Cloud.CommitTxnRequest> requests = new ArrayList<>();
                Mockito.when(proxy.commitTxn(Mockito.any())).thenAnswer(invocation -> {
                    requests.add(invocation.getArgument(0));
                    return CommitTxnResponse.newBuilder().setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                            .setCode(requests.size() == 1 ? MetaServiceCode.KV_TXN_CONFLICT : MetaServiceCode.UNDEFINED_ERR)
                            .setMsg("injected commit failure")).build();
                });
                CloudGlobalTransactionMgr mgr = new CloudGlobalTransactionMgr();
                int selectedPath = path;
                Assertions.assertThrows(UserException.class, () -> {
                    if (selectedPath == 0) {
                        mgr.commitTransactionWithoutLock(1, Collections.emptyList(), 10, null, null);
                    } else if (selectedPath == 1) {
                        mgr.commitAndPublishTransaction(db, 10, Collections.emptyList(), 1000);
                    } else {
                        mgr.commitTransaction2PC(db, Collections.emptyList(), 10, 1000);
                    }
                });
                Assertions.assertEquals(2, requests.size());
                Assertions.assertSame(requests.get(0), requests.get(1));
                Assertions.assertEquals(500L, requests.get(0).getCommitTso());
                Assertions.assertEquals(path == 1, requests.get(0).getIsTxnLoad());
                Assertions.assertEquals(path == 2, requests.get(0).getIs2Pc());
                allocation.verify(() -> TransactionUtil.getCommitTSO(10L, db, Collections.emptySet()));
                allocation.clearInvocations();
            }
        }
    }

    @Test
    public void testValidationFailureDoesNotAllocateTsoOrSendCommit() throws Exception {
        Env env = Mockito.mock(Env.class);
        TabletInvertedIndex index = Mockito.mock(TabletInvertedIndex.class);
        Mockito.when(env.getTabletInvertedIndex()).thenReturn(index);
        Mockito.when(index.getTabletMetaList(Mockito.anyList())).thenThrow(new IllegalStateException("validation failed"));
        Method commit = CloudGlobalTransactionMgr.class.getDeclaredMethod("commitTxn", Cloud.CommitTxnRequest.Builder.class,
                List.class, long.class, boolean.class, List.class, List.class);
        commit.setAccessible(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<TransactionUtil> allocation = Mockito.mockStatic(TransactionUtil.class);
                MockedStatic<MetaServiceProxy> proxy = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Cloud.CommitTxnRequest.Builder builder = Cloud.CommitTxnRequest.newBuilder()
                    .setDbId(1).setTxnId(10).addBaseTabletIds(20);
            InvocationTargetException error = Assertions.assertThrows(InvocationTargetException.class,
                    () -> commit.invoke(new CloudGlobalTransactionMgr(), builder, Collections.emptyList(),
                            10L, false, Collections.emptyList(), Collections.emptyList()));
            Assertions.assertEquals("validation failed", error.getCause().getMessage());
            allocation.verifyNoInteractions();
            proxy.verifyNoInteractions();
            Assertions.assertFalse(builder.hasCommitTso());
        }
    }
}

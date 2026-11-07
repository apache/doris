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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.GenericPool;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TNetworkAddress;

import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class MasterOpExecutorMetadataTest {
    private static final TNetworkAddress MASTER = new TNetworkAddress("master", 9020);
    private GenericPool<FrontendService.Client> previousPool;
    private GenericPool<FrontendService.Client> pool;
    private FrontendService.Client client;
    private Env env;
    private Auth auth;
    private JournalObservable journal;
    private ConnectContext caller;

    @BeforeEach
    void setUp() throws Exception {
        env = Mockito.mock(Env.class);
        auth = Mockito.mock(Auth.class);
        journal = Mockito.mock(JournalObservable.class);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(env.getMasterHost()).thenReturn(MASTER.hostname);
        Mockito.when(env.getMasterRpcPort()).thenReturn(MASTER.port);
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("follower", 9030));
        Mockito.when(env.getJournalObservable()).thenReturn(journal);
        Mockito.when(auth.getQueryTimeoutForRegularUser(Mockito.anyString())).thenReturn(7);
        client = Mockito.mock(FrontendService.Client.class);
        Mockito.when(client.forward(Mockito.any())).thenReturn(new TMasterOpResult().setMaxJournalId(37L));
        pool = Mockito.mock(GenericPool.class);
        Mockito.when(pool.borrowObject(Mockito.eq(MASTER), Mockito.anyInt())).thenReturn(client);
        previousPool = ClientPool.frontendPool;
        ClientPool.frontendPool = pool;
        caller = Mockito.mock(ConnectContext.class);
        Mockito.doCallRealMethod().when(caller).setThreadLocalInfo();
        caller.setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        ClientPool.frontendPool = previousPool;
        ConnectContext.remove();
    }

    @ParameterizedTest
    @CsvSource({"false,0", "false,7", "true,0", "true,7"})
    void metadataRequestsKeepFieldsTimeoutAndCallerBinding(boolean sync, int userTimeoutS) throws Exception {
        Mockito.when(auth.getQueryTimeoutForRegularUser(Mockito.anyString())).thenReturn(userTimeoutS);
        try (MockedStatic<VariableMgr> variables = Mockito.mockStatic(VariableMgr.class);
                MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            variables.when(VariableMgr::getDefaultQueryTimeoutS).thenReturn(13);
            invoke(sync);
            Assertions.assertTrue(contexts.constructed().isEmpty());
        }
        ArgumentCaptor<TMasterOpRequest> request = ArgumentCaptor.forClass(TMasterOpRequest.class);
        Mockito.verify(client).forward(request.capture());
        TMasterOpRequest sent = request.getValue();
        Assertions.assertEquals("follower", sent.getClientNodeHost());
        Assertions.assertEquals(9030, sent.getClientNodePort());
        Assertions.assertEquals("", sent.getSql());
        Assertions.assertFalse(sent.isSetQueryId());
        String user = sync ? "binlog_user" : UserIdentity.ADMIN.getQualifiedUser();
        Assertions.assertEquals(user, sent.getUser());
        Assertions.assertEquals(sync ? "binlog_db" : "", sent.getDb());
        Mockito.verify(auth).getQueryTimeoutForRegularUser(user);
        Mockito.verify(auth, Mockito.never()).getQueryTimeout(Mockito.anyString());
        int timeoutMs = (userTimeoutS > 0 ? userTimeoutS : 13) * 1200;
        Mockito.verify(pool).borrowObject(MASTER, timeoutMs);
        Mockito.verify(pool).returnObject(MASTER, client);
        Mockito.verify(pool, Mockito.never()).invalidateObject(Mockito.any(), Mockito.any());
        if (sync) {
            Assertions.assertTrue(sent.isSyncJournalOnly());
            Mockito.verify(journal).waitOn(37L, timeoutMs);
        } else {
            Assertions.assertTrue(sent.getGroupCommitInfo().isUpdateLoadData());
            Assertions.assertEquals(41L, sent.getGroupCommitInfo().getTableId());
            Assertions.assertEquals(43L, sent.getGroupCommitInfo().getReceiveData());
            Mockito.verifyNoInteractions(journal);
        }
        Assertions.assertSame(caller, ConnectContext.get());
        Mockito.verify(caller, Mockito.never()).cleanup();
        Mockito.verify(caller, Mockito.never()).requestClose(Mockito.any());
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void metadataTransportRetainsRetryAndPoolRules(boolean sync, boolean timedOut) throws Exception {
        TTransportException failure = new TTransportException(
                timedOut ? TTransportException.TIMED_OUT : TTransportException.END_OF_FILE, "transport failed");
        Mockito.when(client.forward(Mockito.any())).thenThrow(failure)
                .thenReturn(new TMasterOpResult().setMaxJournalId(37L));
        Mockito.when(pool.reopenOrClear(MASTER, client, 8400)).thenReturn(true);
        if (timedOut) {
            FEOpExecutor.ForwardToMasterException thrown = Assertions.assertThrows(
                    FEOpExecutor.ForwardToMasterException.class, () -> invoke(sync));
            Assertions.assertTrue(thrown.getMessage().contains("Connection timeout"));
            Assertions.assertTrue(thrown.getMessage().contains(failure.getMessage()));
            Mockito.verify(client).forward(Mockito.any());
            Mockito.verify(pool).invalidateObject(MASTER, client);
            Mockito.verify(pool, Mockito.never()).returnObject(Mockito.any(), Mockito.any());
            Mockito.verifyNoInteractions(journal);
        } else {
            invoke(sync);
            Mockito.verify(client, Mockito.times(2)).forward(Mockito.any());
            Mockito.verify(pool).returnObject(MASTER, client);
            Mockito.verify(pool, Mockito.never()).invalidateObject(Mockito.any(), Mockito.any());
        }
        Assertions.assertSame(caller, ConnectContext.get());
    }

    @Test
    void emptyBinlogUserUsesGlobalTimeoutBeforeRequestValidation() throws Exception {
        try (MockedStatic<VariableMgr> variables = Mockito.mockStatic(VariableMgr.class)) {
            variables.when(VariableMgr::getDefaultQueryTimeoutS).thenReturn(13);
            MasterOpExecutor.syncJournal(env, "binlog_db", "");
        }
        ArgumentCaptor<TMasterOpRequest> request = ArgumentCaptor.forClass(TMasterOpRequest.class);
        Mockito.verify(client).forward(request.capture());
        Assertions.assertEquals("", request.getValue().getUser());
        Mockito.verifyNoInteractions(auth);
        Mockito.verify(pool).borrowObject(MASTER, 15600);
        Mockito.verify(journal).waitOn(37L, 15600);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rejectedBorrowPreservesCauseAndDoesNotReturnAnUnborrowedClient(boolean sync) throws Exception {
        Exception failure = new Exception("pool unavailable");
        Mockito.when(pool.borrowObject(Mockito.eq(MASTER), Mockito.anyInt())).thenThrow(failure);
        Exception thrown = Assertions.assertThrows(Exception.class, () -> invoke(sync));
        Assertions.assertSame(failure, thrown.getCause());
        Mockito.verify(pool, Mockito.never()).returnObject(Mockito.any(), Mockito.any());
        Mockito.verify(pool, Mockito.never()).invalidateObject(Mockito.any(), Mockito.any());
        Mockito.verifyNoInteractions(journal);
        Assertions.assertSame(caller, ConnectContext.get());
    }

    @Test
    void journalFailurePropagatesAfterReturningTransport() throws Exception {
        DdlException failure = new DdlException("journal replay timeout");
        Mockito.doThrow(failure).when(journal).waitOn(37L, 8400);
        Assertions.assertSame(failure, Assertions.assertThrows(DdlException.class, () -> invoke(true)));
        Mockito.verify(pool).returnObject(MASTER, client);
        Mockito.verify(pool, Mockito.never()).invalidateObject(Mockito.any(), Mockito.any());
        Assertions.assertSame(caller, ConnectContext.get());
    }

    private void invoke(boolean sync) throws Exception {
        if (sync) {
            MasterOpExecutor.syncJournal(env, "binlog_db", "binlog_user");
        } else {
            MasterOpExecutor.updateLoadData(env, 41L, 43L);
        }
    }
}

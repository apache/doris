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

package org.apache.doris.service;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.auth.certificate.CertificateAuthDecision;
import org.apache.doris.auth.certificate.StreamLoadCertificateAuthHelper;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TLoadTxn2PCRequest;
import org.apache.doris.thrift.TLoadTxn2PCResult;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class FrontendServiceImplStreamLoad2PCTest {
    private final UserIdentity txnUser = UserIdentity.createAnalyzedUserIdentWithIp("loader", "%");
    private UserIdentity caller;
    private MockedStatic<Env> envMock;
    private MockedStatic<StreamLoadCertificateAuthHelper> certificateMock;
    private GlobalTransactionMgrIface transactions;
    private AccessControllerManager access;
    private Database database;
    private TransactionState transaction;
    private FrontendServiceImpl service;
    private List<Table> tables;

    @BeforeEach
    public void setUp() throws Exception {
        caller = txnUser;
        Env env = Mockito.mock(Env.class);
        Auth auth = Mockito.mock(Auth.class);
        access = Mockito.mock(AccessControllerManager.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        transactions = Mockito.mock(GlobalTransactionMgrIface.class);
        tables = Collections.singletonList(table);
        transaction = new TransactionState(1, Collections.singletonList(2L), 3, "load_label", null,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.BE, 1, "be", 0), -1, 60000);
        transaction.setTxnUser(txnUser);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(database);
        Mockito.when(database.getId()).thenReturn(1L);
        Mockito.when(table.getId()).thenReturn(2L);
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(database.getTableOrMetaException("tbl", Table.TableType.OLAP)).thenReturn(table);
        Mockito.when(database.getTablesOnIdOrderOrThrowException(Mockito.anyList())).thenReturn(tables);
        Mockito.when(database.getTablesOnIdOrderIfExist(Mockito.anyList())).thenReturn(tables);
        Mockito.when(transactions.getTransactionState(1L, 3L)).thenReturn(transaction);
        Mockito.when(transactions.getTransactionIdByLabel(Mockito.eq(1L), Mockito.eq("load_label"),
                Mockito.anyList())).thenReturn(3L);
        Mockito.when(access.checkTblPriv(Mockito.any(UserIdentity.class), Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.eq(PrivPredicate.LOAD))).thenReturn(true);
        Mockito.doAnswer(invocation -> {
            if (!"password".equals(invocation.getArgument(2))) {
                throw new AuthenticationException("Access denied");
            }
            List<UserIdentity> identities = invocation.getArgument(3);
            identities.add(caller);
            return null;
        }).when(auth).checkPlainPassword(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.anyList());
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        envMock.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactions);
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        envMock.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
        certificateMock = Mockito.mockStatic(StreamLoadCertificateAuthHelper.class);
        certificateMock.when(() -> StreamLoadCertificateAuthHelper.authenticateForwarded(Mockito.any(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenReturn(CertificateAuthDecision.notApplicable());
        service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));
    }

    @AfterEach
    public void tearDown() {
        certificateMock.close();
        envMock.close();
    }

    private TLoadTxn2PCRequest request(String operation, boolean byLabel) {
        TLoadTxn2PCRequest request = new TLoadTxn2PCRequest().setUser(caller.getQualifiedUser())
                .setPasswd("password").setUserIp("127.0.0.1").setDb("db").setOperation(operation);
        if (byLabel) {
            request.setLabel("load_label");
        } else {
            request.setTxnId(3);
        }
        return request;
    }

    @ParameterizedTest
    @CsvSource({"commit,false", "commit,true", "abort,false", "abort,true"})
    public void testOtherLoaderCannotFinishTransaction(String operation, boolean byLabel) throws Exception {
        caller = UserIdentity.createAnalyzedUserIdentWithIp("other_loader", "%");
        TLoadTxn2PCResult result = service.loadTxn2PC(request(operation, byLabel));
        Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, result.getStatus().getStatusCode());
        Assertions.assertTrue(result.getStatus().getErrorMsgs().get(0).contains("not the owner"));
        verifyNoCompletion();
        caller = txnUser;
        Assertions.assertEquals(TStatusCode.OK,
                service.loadTxn2PC(request(operation, byLabel)).getStatus().getStatusCode());
        verifyCompletion(operation);
    }

    @Test
    public void testSameNameWithDifferentHostIsNotTxnUser() throws Exception {
        caller = UserIdentity.createAnalyzedUserIdentWithIp("loader", "127.0.0.1");
        Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR,
                service.loadTxn2PC(request("commit", false)).getStatus().getStatusCode());
        verifyNoCompletion();
    }

    @Test
    public void testAbortAfterTableDroppedStillAuthenticatesAndChecksTxnUser() throws Exception {
        Mockito.when(database.getTablesOnIdOrderIfExist(Mockito.anyList())).thenReturn(Collections.emptyList());
        caller = UserIdentity.createAnalyzedUserIdentWithIp("other_loader", "%");
        Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR,
                service.loadTxn2PC(request("abort", false)).getStatus().getStatusCode());
        caller = txnUser;
        Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR,
                service.loadTxn2PC(request("abort", false).setPasswd("wrong")).getStatus().getStatusCode());
        verifyNoCompletion();
        Assertions.assertEquals(TStatusCode.OK,
                service.loadTxn2PC(request("abort", false)).getStatus().getStatusCode());
        Mockito.verify(transactions).abortTransaction2PC(1L, 3L, Collections.emptyList());
    }

    @Test
    public void testTxnUserStillNeedsLoadPrivilege() throws Exception {
        Mockito.when(access.checkTblPriv(txnUser, InternalCatalog.INTERNAL_CATALOG_NAME, "db", "tbl",
                PrivPredicate.LOAD)).thenReturn(false);
        Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR,
                service.loadTxn2PC(request("commit", false)).getStatus().getStatusCode());
        verifyNoCompletion();
    }

    @Test
    public void testAdminCanFinishAnotherUsersTransaction() throws Exception {
        caller = UserIdentity.ROOT;
        Mockito.when(access.checkGlobalPriv(caller, PrivPredicate.ADMIN)).thenReturn(true);
        Assertions.assertEquals(TStatusCode.OK,
                service.loadTxn2PC(request("commit", false)).getStatus().getStatusCode());
        verifyCompletion("commit");
    }

    @Test
    public void testLegacyTxnUserlessTransactionKeepsLoadAuthorization() throws Exception {
        transaction.setTxnUser(null);
        caller = UserIdentity.createAnalyzedUserIdentWithIp("other_loader", "%");
        Assertions.assertEquals(TStatusCode.OK,
                service.loadTxn2PC(request("commit", false)).getStatus().getStatusCode());
        verifyCompletion("commit");
    }

    @Test
    public void testBeginPassesAuthenticatedIdentityToTransactionManager() throws Exception {
        Mockito.when(transactions.beginTransaction(Mockito.eq(1L), Mockito.eq(Collections.singletonList(2L)),
                Mockito.eq("load_label"), Mockito.any(), Mockito.any(),
                Mockito.eq(TransactionState.LoadJobSourceType.BACKEND_STREAMING), Mockito.eq(-1L),
                Mockito.anyLong(), Mockito.eq(txnUser))).thenReturn(3L);
        TLoadTxnBeginRequest request = new TLoadTxnBeginRequest().setUser("loader").setPasswd("password")
                .setUserIp("127.0.0.1").setDb("db").setTbl("tbl").setLabel("load_label").setBackendId(1);
        TLoadTxnBeginResult result = service.loadTxnBegin(request);
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        Assertions.assertEquals(3L, result.getTxnId());
    }

    @Test
    public void testDuplicateBeginDoesNotReturnAnotherUsersTransaction() throws Exception {
        caller = UserIdentity.createAnalyzedUserIdentWithIp("other_loader", "%");
        Mockito.when(transactions.beginTransaction(Mockito.eq(1L), Mockito.eq(Collections.singletonList(2L)),
                Mockito.eq("load_label"), Mockito.any(), Mockito.any(),
                Mockito.eq(TransactionState.LoadJobSourceType.BACKEND_STREAMING), Mockito.eq(-1L),
                Mockito.anyLong(), Mockito.eq(caller)))
                .thenThrow(new DuplicatedRequestException("request", 3L, ""));
        TLoadTxnBeginRequest request = new TLoadTxnBeginRequest().setUser("other_loader").setPasswd("password")
                .setUserIp("127.0.0.1").setDb("db").setTbl("tbl").setLabel("load_label").setBackendId(1);
        TLoadTxnBeginResult result = service.loadTxnBegin(request);
        Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, result.getStatus().getStatusCode());
        Assertions.assertFalse(result.isSetTxnId());
        Assertions.assertTrue(result.getStatus().getErrorMsgs().get(0).contains("not the owner"));
        Assertions.assertEquals(txnUser, transaction.getTxnUser());
    }

    private void verifyNoCompletion() throws Exception {
        Mockito.verify(transactions, Mockito.never()).commitTransaction2PC(Mockito.any(), Mockito.anyList(),
                Mockito.anyLong(), Mockito.anyLong());
        Mockito.verify(transactions, Mockito.never()).abortTransaction2PC(Mockito.anyLong(), Mockito.anyLong(),
                Mockito.anyList());
    }

    private void verifyCompletion(String operation) throws Exception {
        if (operation.equals("commit")) {
            Mockito.verify(transactions).commitTransaction2PC(Mockito.eq(database), Mockito.eq(tables),
                    Mockito.eq(3L), Mockito.anyLong());
        } else {
            Mockito.verify(transactions).abortTransaction2PC(1L, 3L, tables);
        }
    }
}

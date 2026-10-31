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

package org.apache.doris.httpv2.rest;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;

public class CancelLoadActionTest {
    private final UserIdentity owner = UserIdentity.createAnalyzedUserIdentWithIp("loader", "%");
    private final UserIdentity other = UserIdentity.createAnalyzedUserIdentWithIp("other_loader", "%");
    private final CancelLoadAction action = Mockito.spy(new CancelLoadAction());
    private final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    private final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    private GlobalTransactionMgrIface transactions;
    private AccessControllerManager access;
    private ConnectContext context;
    private TransactionState transaction;
    private MockedStatic<Env> envMock;
    private MockedStatic<ConnectContext> contextMock;

    @BeforeEach
    public void setUp() throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        transactions = Mockito.mock(GlobalTransactionMgrIface.class);
        access = Mockito.mock(AccessControllerManager.class);
        context = Mockito.mock(ConnectContext.class);
        transaction = Mockito.mock(TransactionState.class);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(catalog.getDbOrMetaException("db")).thenReturn(database);
        Mockito.when(database.getId()).thenReturn(1L);
        Mockito.when(context.getCurrentUserIdentity()).thenReturn(owner);
        Mockito.when(context.getQualifiedUser()).thenReturn("loader");
        Mockito.when(access.checkDbPriv(context, InternalCatalog.INTERNAL_CATALOG_NAME, "db", PrivPredicate.LOAD))
                .thenReturn(true);
        Mockito.when(transaction.getTxnUser()).thenReturn(owner);
        Mockito.when(transaction.getTransactionId()).thenReturn(3L);
        Mockito.when(transactions.getTransactionId(1L, "load_label")).thenReturn(3L);
        Mockito.when(transactions.getTransactionState(1L, 3L)).thenReturn(transaction);
        Mockito.when(request.getParameter("label")).thenReturn("load_label");
        Mockito.doReturn(false).when(action).needRedirect(Mockito.any());
        Mockito.doReturn(false).when(action).checkForwardToMaster(request);
        Mockito.doReturn(null).when(action).executeCheckPassword(request, response);
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        envMock.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        envMock.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactions);
        contextMock = Mockito.mockStatic(ConnectContext.class);
        contextMock.when(ConnectContext::get).thenReturn(context);
    }

    @AfterEach
    public void tearDown() {
        contextMock.close();
        envMock.close();
    }

    @Test
    public void testOtherDatabaseLoadUserIsDeniedAndOwnerCanRetry() throws Exception {
        Mockito.when(context.getCurrentUserIdentity()).thenReturn(other);
        ResponseBody<?> denied = cancel();
        Assertions.assertEquals(RestApiStatusCode.COMMON_ERROR.code, denied.getCode());
        Assertions.assertTrue(denied.getData().toString().contains("not the owner"));
        verifyNoAbort();

        Mockito.when(context.getCurrentUserIdentity()).thenReturn(owner);
        Assertions.assertEquals(RestApiStatusCode.OK.code, cancel().getCode());
        Mockito.verify(transactions).abortTransaction(1L, 3L, "user cancel");
    }

    @Test
    public void testAdminCanCancelAnotherUsersTransaction() throws Exception {
        Mockito.when(context.getCurrentUserIdentity()).thenReturn(other);
        Mockito.when(access.checkGlobalPriv(other, PrivPredicate.ADMIN)).thenReturn(true);
        Assertions.assertEquals(RestApiStatusCode.OK.code, cancel().getCode());
        Mockito.verify(transactions).abortTransaction(1L, 3L, "user cancel");
    }

    @Test
    public void testLegacyOwnerlessTransactionRetainsDatabaseLoadPolicy() throws Exception {
        Mockito.when(transaction.getTxnUser()).thenReturn(null);
        Mockito.when(context.getCurrentUserIdentity()).thenReturn(other);
        Assertions.assertEquals(RestApiStatusCode.OK.code, cancel().getCode());
        Mockito.verify(transactions).abortTransaction(1L, 3L, "user cancel");
    }

    @Test
    public void testOwnerStillNeedsDatabaseLoad() throws Exception {
        Mockito.when(access.checkDbPriv(context, InternalCatalog.INTERNAL_CATALOG_NAME, "db", PrivPredicate.LOAD))
                .thenReturn(false);
        Assertions.assertThrows(UnauthorizedException.class, this::cancel);
        Mockito.verifyNoInteractions(transactions);
    }

    @Test
    public void testAuthenticationFailureDoesNotAccessTransactions() throws Exception {
        Mockito.doThrow(new UnauthorizedException("Access denied")).when(action)
                .executeCheckPassword(request, response);
        Assertions.assertThrows(UnauthorizedException.class, this::cancel);
        Mockito.verifyNoInteractions(transactions);
    }

    @Test
    public void testLabelReuseDoesNotChangeTheCheckedTransaction() throws Exception {
        // A later lookup would select another transaction. Cancellation must keep the checked ID.
        Mockito.when(transactions.getTransactionId(1L, "load_label")).thenReturn(3L, 4L);
        Assertions.assertEquals(RestApiStatusCode.OK.code, cancel().getCode());
        Mockito.verify(transactions).getTransactionId(1L, "load_label");
        Mockito.verify(transactions).abortTransaction(1L, 3L, "user cancel");
        Mockito.verify(transactions, Mockito.never()).abortTransaction(1L, "load_label", "user cancel");
        Mockito.verify(transactions, Mockito.never()).abortTransaction(1L, 4L, "user cancel");
    }

    @Test
    public void testMissingLabelDoesNotAbort() throws Exception {
        Mockito.when(transactions.getTransactionId(1L, "load_label")).thenReturn(null);
        ResponseBody<?> result = cancel();
        Assertions.assertEquals(RestApiStatusCode.COMMON_ERROR.code, result.getCode());
        Assertions.assertTrue(result.getData().toString().contains("does not exist"));
        verifyNoAbort();
    }

    @Test
    public void testDisappearedTransactionDoesNotAbort() throws Exception {
        Mockito.when(transactions.getTransactionState(1L, 3L)).thenReturn(null);
        ResponseBody<?> result = cancel();
        Assertions.assertEquals(RestApiStatusCode.COMMON_ERROR.code, result.getCode());
        Assertions.assertTrue(result.getData().toString().contains("not found"));
        verifyNoAbort();
    }

    private ResponseBody<?> cancel() {
        ResponseEntity<?> result = (ResponseEntity<?>) action.execute("db", request, response);
        return (ResponseBody<?>) result.getBody();
    }

    private void verifyNoAbort() throws Exception {
        Mockito.verify(transactions, Mockito.never()).abortTransaction(Mockito.anyLong(), Mockito.anyLong(),
                Mockito.anyString());
        Mockito.verify(transactions, Mockito.never()).abortTransaction(Mockito.anyLong(), Mockito.anyString(),
                Mockito.anyString());
    }
}

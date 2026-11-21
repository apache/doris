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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.thrift.TBeginRemoteTxnRequest;
import org.apache.doris.thrift.TCommitRemoteTxnRequest;
import org.apache.doris.transaction.GlobalTransactionMgrIface;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

class RemoteTxnWriteTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void destinationRejectsChangelogBeforeBeginAndCommit(boolean begin) throws Exception {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.hasChangelog()).thenReturn(true);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(db);
        Mockito.doReturn(table).when(db).getTableOrMetaException("tbl", TableType.OLAP);
        GlobalTransactionMgrIface transactions = Mockito.mock(GlobalTransactionMgrIface.class);
        TBeginRemoteTxnRequest beginRequest = new TBeginRemoteTxnRequest().setDb("db").setTbl("tbl");
        TCommitRemoteTxnRequest commitRequest = new TCommitRemoteTxnRequest().setDb("db").setTbl("tbl").setTxnId(7L);
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            env.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactions);
            InvocationTargetException error = Assertions.assertThrows(InvocationTargetException.class,
                    () -> invoke(begin ? "beginRemoteTxnImpl" : "commitRemoteTxnImpl",
                            begin ? beginRequest : commitRequest));
            Assertions.assertInstanceOf(UserException.class, error.getCause());
            Assertions.assertTrue(error.getCause().getMessage().contains(RemoteOlapTable.CHANGELOG_WRITE_ERROR),
                    error.getCause().getMessage());
            Mockito.verifyNoInteractions(transactions);
        }
    }

    @Test
    void ordinaryCommitLeavesQuorumSelectionToDestination() throws Exception {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(db);
        Mockito.doReturn(table).when(db).getTableOrMetaException("tbl", TableType.OLAP);
        GlobalTransactionMgrIface transactions = Mockito.mock(GlobalTransactionMgrIface.class);
        TCommitRemoteTxnRequest request = new TCommitRemoteTxnRequest().setDb("db").setTbl("tbl").setTxnId(7L)
                .setCommitInfos(Collections.emptyList()).setInsertVisibleTimeoutMs(1000L);
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            env.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactions);
            Mockito.when(transactions.commitAndPublishTransaction(db, Collections.singletonList(table), 7L,
                    Collections.emptyList(), 1000L, null)).thenReturn(true);
            Assertions.assertEquals(Boolean.TRUE, invoke("commitRemoteTxnImpl", request));
            Mockito.verify(transactions).commitAndPublishTransaction(db, Collections.singletonList(table), 7L,
                    Collections.emptyList(), 1000L, null);
            Mockito.verifyNoMoreInteractions(transactions);
        }
    }

    private Object invoke(String methodName, Object request) throws ReflectiveOperationException {
        Method method = FrontendServiceImpl.class.getDeclaredMethod(methodName, request.getClass());
        method.setAccessible(true);
        return method.invoke(new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class)), request);
    }
}

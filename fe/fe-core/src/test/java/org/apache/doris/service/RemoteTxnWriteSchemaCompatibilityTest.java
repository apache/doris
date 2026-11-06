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
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.thrift.TCommitRemoteTxnRequest;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnWriteSchemaSnapshot;
import org.apache.doris.transaction.TxnWriteSchemaSnapshotCodec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

class RemoteTxnWriteSchemaCompatibilityTest {
    @ParameterizedTest
    @CsvSource({
            "false, false, false", "true, false, false",
            "false, true, false", "true, true, false",
            "false, false, true", "true, false, true",
            "false, true, true", "true, true, true"
    })
    void validateRemoteSchemaBoundary(boolean cloud, boolean changelog, boolean explicitEmpty) throws Exception {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        GlobalTransactionMgrIface transactions = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(db);
        Mockito.doReturn(table).when(db).getTableOrMetaException("tbl", TableType.OLAP);
        Mockito.when(table.hasChangelog()).thenReturn(changelog);
        Mockito.when(transactions.commitAndPublishTransaction(db, Collections.singletonList(table),
                7L, Collections.emptyList(), 1000L, null)).thenReturn(true);

        TCommitRemoteTxnRequest request = new TCommitRemoteTxnRequest()
                .setDb("db").setTbl("tbl").setTxnId(7L)
                .setCommitInfos(Collections.emptyList()).setInsertVisibleTimeoutMs(1000L);
        if (explicitEmpty) {
            request.setWriteSchemas(Collections.emptyList());
        }
        Assertions.assertEquals(explicitEmpty, request.isSetWriteSchemas());
        Method method = FrontendServiceImpl.class.getDeclaredMethod(
                "commitRemoteTxnImpl", TCommitRemoteTxnRequest.class);
        method.setAccessible(true);
        FrontendServiceImpl service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<Config> config = Mockito.mockStatic(Config.class)) {
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            env.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactions);
            config.when(Config::isCloudMode).thenReturn(cloud);
            if (!changelog && !explicitEmpty) {
                Assertions.assertEquals(Boolean.TRUE, method.invoke(service, request));
                Mockito.verify(transactions).commitAndPublishTransaction(db, Collections.singletonList(table),
                        7L, Collections.emptyList(), 1000L, null);
                Mockito.verifyNoMoreInteractions(transactions);
            } else {
                InvocationTargetException error = Assertions.assertThrows(InvocationTargetException.class,
                        () -> method.invoke(service, request));
                Assertions.assertInstanceOf(UserException.class, error.getCause());
                Assertions.assertTrue(error.getCause().getMessage()
                        .contains("Remote commit requires exactly one prepared table schema"));
                Mockito.verifyNoInteractions(transactions);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false, true", "true, true", "false, false", "true, false"})
    void suppliedSnapshotStillValidatesAndInstalls(boolean cloud, boolean valid) throws Exception {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        GlobalTransactionMgrIface transactions = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(db);
        Mockito.when(db.getId()).thenReturn(1L);
        Mockito.doReturn(table).when(db).getTableOrMetaException("tbl", TableType.OLAP);
        Mockito.when(table.getId()).thenReturn(2L);
        Mockito.when(table.getBaseIndexId()).thenReturn(3L);
        TransactionState state = new TransactionState();
        state.setTransactionId(7L);
        state.addTableId(2L);
        Mockito.when(transactions.getTransactionState(1L, 7L)).thenReturn(state);
        Mockito.when(transactions.commitAndPublishTransaction(db, Collections.singletonList(table),
                7L, Collections.emptyList(), 1000L, null)).thenReturn(true);
        TxnWriteSchemaSnapshot snapshot = new TxnWriteSchemaSnapshot(2L,
                Collections.singletonMap(valid ? 3L : 99L, 1), Collections.emptyMap(), false);
        TCommitRemoteTxnRequest request = new TCommitRemoteTxnRequest().setDb("db").setTbl("tbl").setTxnId(7L)
                .setCommitInfos(Collections.emptyList()).setInsertVisibleTimeoutMs(1000L)
                .setWriteSchemas(Collections.singletonList(TxnWriteSchemaSnapshotCodec.toThrift(snapshot)));
        Method method = FrontendServiceImpl.class.getDeclaredMethod(
                "commitRemoteTxnImpl", TCommitRemoteTxnRequest.class);
        method.setAccessible(true);
        FrontendServiceImpl service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<Config> config = Mockito.mockStatic(Config.class)) {
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            env.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactions);
            config.when(Config::isCloudMode).thenReturn(cloud);
            if (valid) {
                Assertions.assertEquals(Boolean.TRUE, method.invoke(service, request));
                Mockito.verify(transactions).commitAndPublishTransaction(db, Collections.singletonList(table),
                        7L, Collections.emptyList(), 1000L, null);
                if (cloud) {
                    Mockito.verify(transactions, Mockito.never()).getTransactionState(1L, 7L);
                    Assertions.assertNull(state.getWriteSchema(7L, 2L));
                } else {
                    Mockito.verify(transactions).getTransactionState(1L, 7L);
                    Assertions.assertEquals(snapshot, state.getWriteSchema(7L, 2L));
                }
            } else {
                InvocationTargetException error = Assertions.assertThrows(InvocationTargetException.class,
                        () -> method.invoke(service, request));
                Assertions.assertInstanceOf(UserException.class, error.getCause());
                Assertions.assertTrue(error.getCause().getMessage().contains("Invalid remote write schema"));
                Mockito.verifyNoInteractions(transactions);
            }
        }
    }

}

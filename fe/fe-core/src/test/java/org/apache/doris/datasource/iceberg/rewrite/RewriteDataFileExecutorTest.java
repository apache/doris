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

package org.apache.doris.datasource.iceberg.rewrite;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.transaction.TransactionManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class RewriteDataFileExecutorTest {

    @Test
    void testInvalidateTableCacheAfterCommit() throws Exception {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        IcebergTransaction transaction = Mockito.mock(IcebergTransaction.class);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        TransactionManager transactionManager = Mockito.mock(TransactionManager.class);
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getTransactionManager()).thenReturn(transactionManager);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

            transactionManager.commit(7L);
            new RewriteDataFileExecutor(table, null).invalidateTableCacheAfterCommit();

            InOrder inOrder = Mockito.inOrder(transactionManager, cacheMgr);
            inOrder.verify(transactionManager).commit(7L);
            inOrder.verify(cacheMgr).invalidateTableCache(table);
            Mockito.verify(transaction, Mockito.never()).commit();
        }
    }

    @Test
    void testCacheInvalidationFailureDoesNotTurnDurableCommitIntoFailure() throws Exception {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        TransactionManager transactionManager = Mockito.mock(TransactionManager.class);
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
            Mockito.doThrow(new IllegalStateException("injected cache failure"))
                    .when(cacheMgr).invalidateTableCache(table);

            transactionManager.commit(7L);
            Assertions.assertDoesNotThrow(() -> new RewriteDataFileExecutor(table, null)
                    .invalidateTableCacheAfterCommit());

            Mockito.verify(transactionManager).commit(7L);
            Mockito.verify(transactionManager, Mockito.never()).rollback(7L);
        }
    }

    @Test
    void testRollbackThroughManagerWhenSnapshotLoadFails() throws Exception {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        IcebergTransaction transaction = Mockito.mock(IcebergTransaction.class);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        TransactionManager transactionManager = Mockito.mock(TransactionManager.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getTransactionManager()).thenReturn(transactionManager);
        Mockito.when(transactionManager.begin()).thenReturn(7L);
        Mockito.when(transactionManager.getTransaction(7L)).thenReturn(transaction);
        Mockito.when(table.loadSnapshot(Mockito.any(), Mockito.any()))
                .thenThrow(new IllegalStateException("injected snapshot failure"));

        Assertions.assertThrows(IllegalStateException.class,
                () -> new RewriteDataFileExecutor(table, null)
                        .executeGroupsConcurrently(java.util.Collections.emptyList(), 1024));

        InOrder inOrder = Mockito.inOrder(transaction, transactionManager);
        inOrder.verify(transaction).stopAcceptingCommitData();
        inOrder.verify(transactionManager).rollback(7L);
        Mockito.verify(transactionManager, Mockito.never()).commit(7L);
    }
}

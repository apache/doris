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
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.qe.HttpStreamParams;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;
import org.apache.doris.transaction.GlobalTransactionMgr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class HttpStreamLoadTablePropertiesTest {
    @Test
    public void testLockTimeoutAbortsBegunTransaction() throws Exception {
        verifyLockTimeout(23, false);
    }

    @Test
    public void testGroupCommitLockTimeoutDoesNotAbortTransaction() throws Exception {
        verifyLockTimeout(-1, false);
    }

    @Test
    public void testAbortFailurePreservesLockTimeout() throws Exception {
        verifyLockTimeout(23, true);
    }

    private void verifyLockTimeout(long txnId, boolean abortFails) throws Exception {
        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doReturn("stream_load_lock_test").when(table).getName();
        Database db = new Database(17, "stream_load_lock_db");
        HttpStreamParams params = new HttpStreamParams();
        params.setDb(db);
        params.setTable(table);
        params.setTxnId(txnId);
        GlobalTransactionMgr transactions = Mockito.mock(GlobalTransactionMgr.class);
        if (abortFails) {
            Mockito.doThrow(new UserException("abort failed")).when(transactions)
                    .abortTransaction(Mockito.eq(17L), Mockito.eq(txnId), Mockito.anyString());
        }
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);
        ExecutorService writer = Executors.newSingleThreadExecutor();
        Future<?> writeTask = writer.submit(() -> {
            table.writeLock();
            try {
                locked.countDown();
                Assertions.assertTrue(releaseWriter.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            } finally {
                table.writeUnlock();
            }
        });
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            Assertions.assertTrue(locked.await(5, TimeUnit.SECONDS));
            env.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactions);
            UserException failure = Assertions.assertThrows(UserException.class,
                    () -> Deencapsulation.invoke(service, "setHttpStreamLoadTableProperties",
                            new TStreamLoadPutRequest().setThriftRpcTimeoutMs(1), params,
                            new TStreamLoadPutResult()));
            Assertions.assertTrue(failure.getMessage().contains("get table read lock timeout"));
            Assertions.assertEquals(abortFails ? 1 : 0, failure.getSuppressed().length);
            if (txnId > 0) {
                Mockito.verify(transactions).abortTransaction(17L, txnId, failure.getMessage());
            } else {
                Mockito.verifyNoInteractions(transactions);
            }
            Mockito.verify(table, Mockito.never()).readUnlock();
            Mockito.verify(table, Mockito.never()).getBaseSchemaVersion();
        } finally {
            releaseWriter.countDown();
            try {
                writeTask.get(5, TimeUnit.SECONDS);
            } finally {
                writer.shutdownNow();
            }
        }
    }

    @Test
    public void testSchemaVersionIsReadUnderTableLock() {
        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doReturn(false).when(table).getEnableUniqueKeyMergeOnWrite();
        Mockito.doReturn(false).when(table).enableTso();
        Mockito.doReturn(1000).when(table).getGroupCommitIntervalMs();
        Mockito.doReturn(1024).when(table).getGroupCommitDataBytes();
        Mockito.doAnswer(invocation -> {
            Assertions.assertEquals(1, readHoldCount(table));
            return 7;
        }).when(table).getBaseSchemaVersion();
        HttpStreamParams params = new HttpStreamParams();
        params.setTable(table);
        TStreamLoadPutResult result = new TStreamLoadPutResult();
        result.setPipelineParams(new TPipelineFragmentParams());
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class);

        Deencapsulation.invoke(service, "setHttpStreamLoadTableProperties",
                new TStreamLoadPutRequest(), params, result);

        Assertions.assertEquals(7, result.getBaseSchemaVersion());
        Assertions.assertEquals(0, readHoldCount(table));
    }

    @Test
    public void testSchemaReadFailureReleasesTableLock() {
        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doReturn(false).when(table).getEnableUniqueKeyMergeOnWrite();
        Mockito.doReturn(false).when(table).enableTso();
        Mockito.doThrow(new IllegalStateException("schema read failed")).when(table).getBaseSchemaVersion();
        HttpStreamParams params = new HttpStreamParams();
        params.setTable(table);
        TStreamLoadPutResult result = new TStreamLoadPutResult();
        result.setPipelineParams(new TPipelineFragmentParams());
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class);

        Assertions.assertThrows(IllegalStateException.class,
                () -> Deencapsulation.invoke(service, "setHttpStreamLoadTableProperties",
                        new TStreamLoadPutRequest(), params, result));
        Assertions.assertEquals(0, readHoldCount(table));
    }

    private int readHoldCount(OlapTable table) {
        MonitoredReentrantReadWriteLock lock = Deencapsulation.getField(table, "rwLock");
        return lock.getReadHoldCount();
    }
}

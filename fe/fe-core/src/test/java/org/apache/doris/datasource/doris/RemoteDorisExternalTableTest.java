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

package org.apache.doris.datasource.doris;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TempPartitions;
import org.apache.doris.nereids.exceptions.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteDorisExternalTableTest {
    private static final String DB_NAME = "test_db";
    private static final String REMOTE_TABLE_NAME = "remote_test_table";

    private RemoteDorisExternalTable table;
    private FeServiceClient client;
    private RemoteOlapTable remoteOlapTable;

    @BeforeEach
    public void setUp() throws Exception {
        RemoteDorisExternalCatalog catalog = Mockito.mock(RemoteDorisExternalCatalog.class);
        RemoteDorisExternalDatabase db = Mockito.mock(RemoteDorisExternalDatabase.class);
        client = Mockito.mock(FeServiceClient.class);
        remoteOlapTable = Mockito.mock(RemoteOlapTable.class);
        TempPartitions tempPartitions = Mockito.mock(TempPartitions.class);

        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.doReturn(db).when(catalog).getDbOrAnalysisException(DB_NAME);
        Mockito.when(catalog.getFeServiceClient()).thenReturn(client);
        Mockito.when(db.getId()).thenReturn(2L);
        Mockito.when(db.getFullName()).thenReturn(DB_NAME);
        Mockito.when(db.getRemoteName()).thenReturn(DB_NAME);
        Mockito.when(remoteOlapTable.getId()).thenReturn(3L);
        Mockito.when(remoteOlapTable.getPartitions()).thenReturn(Collections.emptyList());
        Mockito.when(remoteOlapTable.getTempPartitions()).thenReturn(tempPartitions);
        Mockito.when(tempPartitions.getPartitions()).thenReturn(Collections.emptyList());

        table = new RemoteDorisExternalTable(
                4L, "test_table", REMOTE_TABLE_NAME, catalog, db);
    }

    @Test
    public void testConcurrentRefreshSharesInFlightTask() throws Exception {
        CountDownLatch rpcStarted = new CountDownLatch(1);
        CountDownLatch releaseRpc = new CountDownLatch(1);
        whenRefreshCalled()
                .thenAnswer(invocation -> {
                    rpcStarted.countDown();
                    await(releaseRpc);
                    return remoteOlapTable;
                })
                .thenReturn(remoteOlapTable);

        AtomicReference<OlapTable> ownerResult = new AtomicReference<>();
        AtomicReference<OlapTable> waiterResult = new AtomicReference<>();
        AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
        AtomicReference<Throwable> waiterFailure = new AtomicReference<>();

        Thread owner = startRefresh(ownerResult, ownerFailure);
        Assertions.assertTrue(rpcStarted.await(5, TimeUnit.SECONDS));
        Thread waiter = startRefresh(waiterResult, waiterFailure);
        waitUntilWaiting(waiter);

        releaseRpc.countDown();
        join(owner);
        join(waiter);

        Assertions.assertNull(ownerFailure.get());
        Assertions.assertNull(waiterFailure.get());
        Assertions.assertSame(remoteOlapTable, ownerResult.get());
        Assertions.assertSame(remoteOlapTable, waiterResult.get());
        verifyRefreshCount(1);

        Assertions.assertSame(remoteOlapTable, table.getOlapTable());
        verifyRefreshCount(2);
    }

    @Test
    public void testFailedRefreshCanRetry() {
        RuntimeException failure = new RuntimeException("refresh failed");
        whenRefreshCalled().thenThrow(failure).thenReturn(remoteOlapTable);

        AnalysisException exception =
                Assertions.assertThrows(AnalysisException.class, table::getOlapTable);
        Assertions.assertSame(failure, exception.getCause());

        Assertions.assertSame(remoteOlapTable, table.getOlapTable());
        verifyRefreshCount(2);
    }

    private OngoingStubbing<RemoteOlapTable> whenRefreshCalled() {
        return Mockito.when(client.getOlapTable(
                ArgumentMatchers.eq(DB_NAME), ArgumentMatchers.eq(REMOTE_TABLE_NAME),
                ArgumentMatchers.anyLong(), ArgumentMatchers.anyList(), ArgumentMatchers.anyList()));
    }

    private void verifyRefreshCount(int count) {
        Mockito.verify(client, Mockito.times(count)).getOlapTable(
                ArgumentMatchers.eq(DB_NAME), ArgumentMatchers.eq(REMOTE_TABLE_NAME),
                ArgumentMatchers.anyLong(), ArgumentMatchers.anyList(), ArgumentMatchers.anyList());
    }

    private Thread startRefresh(AtomicReference<OlapTable> result,
            AtomicReference<Throwable> failure) {
        Thread thread = new Thread(() -> {
            try {
                result.set(table.getOlapTable());
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        thread.start();
        return thread;
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(5, TimeUnit.SECONDS)) {
            throw new AssertionError("timed out waiting for test latch");
        }
    }

    private static void waitUntilWaiting(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            if (!thread.isAlive()) {
                throw new AssertionError("thread exited before waiting");
            }
            Thread.sleep(10);
        }
        throw new AssertionError("thread did not enter waiting state");
    }

    private static void join(Thread thread) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(5));
        Assertions.assertFalse(thread.isAlive(), "test thread did not finish");
    }
}

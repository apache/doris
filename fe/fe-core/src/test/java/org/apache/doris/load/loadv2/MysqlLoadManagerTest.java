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

package org.apache.doris.load.loadv2;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.StreamLoadHandler;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.BackendSelectionManager;
import org.apache.doris.resource.spi.BackendSelectionProvider;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MysqlLoadManagerTest {

    @AfterEach
    void resetBackendSelectionProvider() {
        BackendSelectionManager.resetProviderForTest();
    }

    private final String originalCloudUniqueId = Config.cloud_unique_id;

    @AfterEach
    public void tearDown() {
        Config.cloud_unique_id = originalCloudUniqueId;
        ConnectContext.remove();
    }

    @Test
    public void testSelectBackendForCloudMySqlLoadIgnoresLoadSelection() throws Exception {
        Config.cloud_unique_id = "cloud";
        ConnectContext context = new ConnectContext();
        context.setCloudCluster("cg1");
        context.setThreadLocalInfo();
        MysqlLoadManager manager = new MysqlLoadManager();
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn("be-host");
        Mockito.when(backend.getHttpPort()).thenReturn(8040);
        BackendSelectionProvider provider = Mockito.mock(BackendSelectionProvider.class);

        BackendSelectionManager.setProviderForTest(provider);
        try (MockedStatic<StreamLoadHandler> mockedStreamLoad = Mockito.mockStatic(StreamLoadHandler.class)) {
            mockedStreamLoad.when(() -> StreamLoadHandler.selectBackend("cg1")).thenReturn(backend);

            String url = invokeSelectBackendForMySqlLoad(manager, context, "db1", "tbl1");

            Assertions.assertEquals("http://be-host:8040/api/db1/tbl1/_stream_load", url);
            mockedStreamLoad.verify(() -> StreamLoadHandler.selectBackend("cg1"));
            Mockito.verifyNoInteractions(provider);
        }
    }

    @Test
    public void testSelectBackendForMySqlLoadUsesExplicitContext() throws Exception {
        Config.cloud_unique_id = "";
        ConnectContext context = new ConnectContext();
        MysqlLoadManager manager = new MysqlLoadManager();
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn("be-host");
        Mockito.when(backend.getHttpPort()).thenReturn(8040);
        Mockito.when(systemInfoService.selectBackendIdsByPolicy(Mockito.any(), Mockito.eq(-1)))
                .thenReturn(Collections.singletonList(1L));
        Mockito.when(systemInfoService.getBackend(1L)).thenReturn(backend);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<BackendSelectionManager> mockedSelection =
                        Mockito.mockStatic(BackendSelectionManager.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            mockedSelection.when(() -> BackendSelectionManager.chooseLoadBackend(
                    Mockito.same(context), Mockito.anyList())).thenReturn(backend);

            String url = invokeSelectBackendForMySqlLoad(manager, context, "db1", "tbl1");

            Assertions.assertEquals("http://be-host:8040/api/db1/tbl1/_stream_load", url);
            mockedSelection.verify(() -> BackendSelectionManager.chooseLoadBackend(
                    Mockito.same(context), Mockito.anyList()));
        }
    }

    @Test
    void uploadsOneFileAtATimeAndLeavesTheNextCommandUnread() throws Exception {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        Mockito.when(channel.getSerializer()).thenReturn(MysqlSerializer.newInstance());
        ByteBuffer nextCommand = ByteBuffer.wrap(new byte[] {3});
        Mockito.when(channel.fetchOnePacket()).thenReturn(
                ByteBuffer.wrap(new byte[] {5}), ByteBuffer.wrap(new byte[] {6}), ByteBuffer.allocate(0),
                ByteBuffer.wrap(new byte[] {7}), ByteBuffer.allocate(0), nextCommand);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        Mockito.when(response.getEntity()).thenAnswer(invocation -> new StringEntity("ok"));
        Mockito.when(client.execute(Mockito.any(HttpPut.class))).thenAnswer(invocation -> {
            HttpPut put = invocation.getArgument(0);
            put.getEntity().writeTo(bytes);
            return response;
        });
        StmtExecutor executor = executorFor(channel);
        MysqlLoadManager manager = managerWithPool(pool);
        HttpPut firstFile = new HttpPut();
        HttpPut secondFile = new HttpPut();
        try {
            Assertions.assertEquals("ok", manager.uploadFile(executor, client, firstFile, true, "f1"));
            Assertions.assertArrayEquals(new byte[] {5, 6}, bytes.toByteArray());
            bytes.reset();
            Assertions.assertEquals("ok", manager.uploadFile(executor, client, secondFile, true, "f2"));
            Assertions.assertArrayEquals(new byte[] {7}, bytes.toByteArray());
            Assertions.assertSame(nextCommand, channel.fetchOnePacket());
            executor.cancel(Status.CANCELLED, false);
            Assertions.assertFalse(firstFile.isAborted());
            Assertions.assertFalse(secondFile.isAborted());
            Mockito.verify(response, Mockito.times(2)).close();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void cancelledUploadWaitsForDrainEvenIfCommandThreadWasInterrupted() throws Exception {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        Mockito.when(channel.getSerializer()).thenReturn(MysqlSerializer.newInstance());
        StmtExecutor executor = executorFor(channel);
        MysqlLoadManager manager = managerWithPool(pool);
        HttpPut request = new HttpPut();
        CountDownLatch uploading = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        CountDownLatch draining = new CountDownLatch(1);
        CountDownLatch clientEof = new CountDownLatch(1);
        AtomicReference<Thread> reader = new AtomicReference<>();
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        Mockito.when(client.execute(request)).thenAnswer(invocation -> {
            reader.set(Thread.currentThread());
            Assertions.assertEquals(5, request.getEntity().getContent().read());
            uploading.countDown();
            Assertions.assertTrue(cancelled.await(10, TimeUnit.SECONDS));
            Assertions.assertTrue(request.isAborted());
            throw new IOException("HTTP request aborted");
        });
        Mockito.when(channel.fetchOnePacket()).thenAnswer(invocation -> {
            Assertions.assertSame(reader.get(), Thread.currentThread());
            return ByteBuffer.wrap(new byte[] {5, 6});
        }).thenAnswer(invocation -> {
            Assertions.assertSame(reader.get(), Thread.currentThread());
            draining.countDown();
            Assertions.assertTrue(clientEof.await(10, TimeUnit.SECONDS));
            return ByteBuffer.allocate(0);
        });
        FutureTask<Boolean> command = new FutureTask<>(() -> {
            Thread.currentThread().interrupt();
            IOException error = Assertions.assertThrows(IOException.class,
                    () -> manager.uploadFile(executor, client, request, true, "f"));
            Assertions.assertEquals("HTTP request aborted", error.getMessage());
            return Thread.currentThread().isInterrupted();
        });
        try {
            new Thread(command).start();
            Assertions.assertTrue(uploading.await(10, TimeUnit.SECONDS));
            executor.cancel(Status.CANCELLED, false);
            cancelled.countDown();
            Assertions.assertTrue(draining.await(10, TimeUnit.SECONDS));
            Assertions.assertFalse(command.isDone());
            clientEof.countDown();
            Assertions.assertTrue(command.get(10, TimeUnit.SECONDS));
            Mockito.verify(channel, Mockito.times(2)).fetchOnePacket();
        } finally {
            cancelled.countDown();
            clientEof.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void rejectionAndCancellationBeforeAdmissionDoNotRequestAFile() throws Exception {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        pool.shutdown();
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        Assertions.assertThrows(RejectedExecutionException.class,
                () -> MysqlLoadManager.uploadLocalFile(pool, channel, client, new HttpPut(), "f"));
        StmtExecutor executor = executorFor(channel);
        MysqlLoadManager manager = managerWithPool(pool);
        executor.cancel(Status.CANCELLED, false);
        IOException error = Assertions.assertThrows(IOException.class,
                () -> manager.uploadFile(executor, client, new HttpPut(), true, "f"));
        Assertions.assertEquals("Cancelled", error.getMessage());
        Mockito.verifyNoInteractions(channel, client);
    }

    @Test
    void cancellationWithdrawsAQueuedFileWithoutWaitingForAnotherClient() throws Exception {
        CountDownLatch queued = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>() {
                    @Override
                    public boolean offer(Runnable runnable) {
                        boolean accepted = super.offer(runnable);
                        queued.countDown();
                        return accepted;
                    }
                });
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        HttpPut request = new HttpPut();
        StmtExecutor executor = executorFor(channel);
        MysqlLoadManager manager = managerWithPool(pool);
        try {
            pool.submit(() -> releaseWorker.await(15, TimeUnit.SECONDS));
            FutureTask<IOException> command = new FutureTask<>(() -> Assertions.assertThrows(IOException.class,
                    () -> manager.uploadFile(executor, client, request, true, "queued")));
            new Thread(command).start();
            Assertions.assertTrue(queued.await(10, TimeUnit.SECONDS));
            executor.cancel(Status.CANCELLED, false);
            Assertions.assertEquals("Cancelled", command.get(5, TimeUnit.SECONDS).getMessage());
            Assertions.assertTrue(pool.getQueue().isEmpty());
            Mockito.verifyNoInteractions(channel, client);
        } finally {
            releaseWorker.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void partialFileRequestFailureClosesTheChannelWithoutStartingTheUpload() throws Exception {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        Mockito.when(channel.getSerializer()).thenReturn(MysqlSerializer.newInstance());
        IOException failure = new IOException("partial file request");
        Mockito.doThrow(failure).when(channel).sendAndFlush(Mockito.any(ByteBuffer.class));
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        try {
            Assertions.assertSame(failure, Assertions.assertThrows(IOException.class,
                    () -> MysqlLoadManager.uploadLocalFile(pool, channel, client, new HttpPut(), "f")));
            Mockito.verify(channel).close();
            Mockito.verify(channel, Mockito.never()).fetchOnePacket();
            Mockito.verifyNoInteractions(client);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void failedDrainAfterHttpRejectionClosesTheChannel() throws Exception {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        Mockito.when(channel.getSerializer()).thenReturn(MysqlSerializer.newInstance());
        IOException readFailure = new IOException("incomplete client upload");
        Mockito.when(channel.fetchOnePacket()).thenThrow(readFailure);
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        HttpPut request = new HttpPut();
        IOException rejection = new IOException("backend rejected request");
        Mockito.when(client.execute(request)).thenThrow(rejection);
        try {
            IOException error = Assertions.assertThrows(IOException.class,
                    () -> MysqlLoadManager.uploadLocalFile(pool, channel, client, request, "f"));
            Assertions.assertSame(rejection, error);
            Assertions.assertArrayEquals(new Throwable[] {readFailure}, error.getSuppressed());
            Mockito.verify(channel).close();
            Mockito.verify(channel).fetchOnePacket();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void connectionLossWhileWritingTheBodyFailsTheUpload() throws Exception {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        Mockito.when(channel.getSerializer()).thenReturn(MysqlSerializer.newInstance());
        Mockito.when(channel.fetchOnePacket()).thenReturn(ByteBuffer.wrap(new byte[] {5}), (ByteBuffer) null);
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        HttpPut request = new HttpPut();
        Mockito.when(client.execute(request)).thenAnswer(invocation -> {
            request.getEntity().writeTo(new ByteArrayOutputStream());
            throw new AssertionError("upload unexpectedly completed");
        });
        try {
            Assertions.assertThrows(EOFException.class,
                    () -> MysqlLoadManager.uploadLocalFile(pool, channel, client, request, "f"));
            Mockito.verify(channel).close();
        } finally {
            pool.shutdownNow();
        }
    }

    private StmtExecutor executorFor(MysqlChannel channel) {
        ConnectContext context = Mockito.spy(new ConnectContext());
        Mockito.doReturn(channel).when(context).getMysqlChannel();
        return new StmtExecutor(context, "LOAD DATA");
    }

    private MysqlLoadManager managerWithPool(ThreadPoolExecutor pool) {
        MysqlLoadManager manager = new MysqlLoadManager();
        Deencapsulation.setField(manager, "mysqlLoadPool", pool);
        return manager;
    }

    private String invokeSelectBackendForMySqlLoad(MysqlLoadManager manager, ConnectContext context,
            String database, String table)
            throws Exception {
        Method method = MysqlLoadManager.class.getDeclaredMethod("selectBackendForMySqlLoad",
                ConnectContext.class, String.class, String.class);
        method.setAccessible(true);
        return (String) method.invoke(manager, context, database, table);
    }
}

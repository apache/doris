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

package org.apache.doris.mysql;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.LifecycleState;
import org.apache.doris.qe.ConnectPoolTestSupport;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.ConnectScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.xnio.StreamConnection;
import org.xnio.XnioIoThread;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;

public class AcceptListenerTest {
    private boolean savedRunningUnitTest;

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @Test
    public void testLoginResponseFailureUnregistersWithoutAChannelCallback() throws Exception {
        Env env = ConnectPoolTestSupport.envAllowing(10);
        ConnectContext ctx = ConnectPoolTestSupport.mysqlConnection(env, UserIdentity.ROOT);
        ConnectScheduler scheduler = new ConnectScheduler(10);
        scheduler.submit(ctx);
        StreamConnection connection = Mockito.mock(StreamConnection.class);
        Mockito.when(connection.isOpen()).thenReturn(true);
        try (MockedStatic<MysqlProto> protocol = Mockito.mockStatic(MysqlProto.class)) {
            protocol.when(() -> MysqlProto.negotiate(ctx)).thenReturn(true);
            protocol.when(() -> MysqlProto.sendResponsePacket(ctx)).thenAnswer(call -> {
                Assertions.assertSame(ctx, scheduler.getContext(ctx.getConnectionId()));
                Mockito.verify(connection).setCloseListener(Mockito.any());
                throw new IOException("client disconnected during login response");
            });
            new AcceptListener(scheduler).handleConnection(ctx, connection);
        }
        Assertions.assertEquals(0, scheduler.getConnectionNum());
        Assertions.assertEquals(LifecycleState.CLOSED, ctx.getLifecycleState());
        ctx.getCleanupCompletion().toCompletableFuture().join();
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    public void testWorkerRejectionStillCleansUpWhenErrorResponseFails() throws Exception {
        Env env = ConnectPoolTestSupport.envAllowing(10);
        ConnectContext ctx = Mockito.spy(ConnectPoolTestSupport.mysqlConnection(env, UserIdentity.ROOT));
        StreamConnection connection = Mockito.mock(StreamConnection.class);
        AcceptingChannel<StreamConnection> channel = Mockito.mock(AcceptingChannel.class);
        XnioWorker worker = Mockito.mock(XnioWorker.class);
        Mockito.when(channel.accept()).thenReturn(connection);
        Mockito.when(channel.getWorker()).thenReturn(worker);
        Mockito.doThrow(new RejectedExecutionException("worker queue full")).when(worker)
                .execute(Mockito.any(Runnable.class));
        try (MockedStatic<ConnectContext> factory = Mockito.mockStatic(ConnectContext.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<Env> currentEnv = Mockito.mockStatic(Env.class);
                MockedStatic<MysqlProto> protocol = Mockito.mockStatic(MysqlProto.class)) {
            factory.when(() -> ConnectContext.forMysql(connection)).thenReturn(ctx);
            currentEnv.when(Env::getCurrentEnv).thenReturn(env);
            protocol.when(() -> MysqlProto.sendResponsePacket(ctx)).thenThrow(new IOException("broken socket"));
            new AcceptListener(new ConnectScheduler(10)).handleEvent(channel);
            Assertions.assertEquals(LifecycleState.CLOSED, ctx.getLifecycleState());
            Mockito.verify(ctx).cleanup();
        }
    }

    @Test
    public void testReadFailureClosesTheSessionAfterTheCommandExits() throws Exception {
        MysqlProtocolAdapter adapter = Mockito.mock(MysqlProtocolAdapter.class);
        Mockito.when(adapter.type()).thenReturn(ConnectContext.ConnectType.MYSQL);
        ConnectContext ctx = new ConnectContext(adapter);
        ctx.activate();
        ConnectProcessor processor = Mockito.mock(ConnectProcessor.class);
        Mockito.doAnswer(call -> {
            Assertions.assertSame(ctx, ConnectContext.get());
            throw new IOException("read failed");
        }).when(processor).processOnce();
        ConduitStreamSourceChannel channel = Mockito.mock(ConduitStreamSourceChannel.class);
        XnioWorker worker = Mockito.mock(XnioWorker.class);
        Mockito.when(channel.getWorker()).thenReturn(worker);
        Mockito.doAnswer(call -> {
            call.<Runnable>getArgument(0).run();
            return null;
        }).when(worker).execute(Mockito.any(Runnable.class));
        try (MockedStatic<XnioIoThread> ioThread = Mockito.mockStatic(XnioIoThread.class)) {
            new ReadListener(ctx, processor).handleEvent(channel);
        }
        Assertions.assertEquals(LifecycleState.CLOSED, ctx.getLifecycleState());
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verify(adapter, Mockito.never()).resumeAcceptQuery();
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    public void testAlreadyClosedChannelCannotBeRegistered() {
        ConnectContext ctx = ConnectPoolTestSupport.mysqlConnection(ConnectPoolTestSupport.envAllowing(10),
                UserIdentity.ROOT);
        ConnectScheduler scheduler = new ConnectScheduler(10);
        scheduler.submit(ctx);
        StreamConnection connection = Mockito.mock(StreamConnection.class);
        Mockito.when(connection.isOpen()).thenReturn(false);
        try (MockedStatic<MysqlProto> protocol = Mockito.mockStatic(MysqlProto.class)) {
            new AcceptListener(scheduler).handleConnection(ctx, connection);
            protocol.verifyNoInteractions();
        }
        Assertions.assertEquals(0, scheduler.getConnectionNum());
        Assertions.assertEquals(LifecycleState.CLOSED, ctx.getLifecycleState());
    }
}

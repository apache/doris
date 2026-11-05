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
import org.apache.doris.qe.ConnectPoolTestSupport;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.MysqlConnectProcessor;
import org.apache.doris.qe.protocol.RecordingMysqlChannel;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
        ConnectContext ctx = Mockito.spy(ConnectPoolTestSupport.mysqlConnection(env, UserIdentity.ROOT));
        ConnectScheduler scheduler = new ConnectScheduler(10);
        scheduler.submit(ctx);
        StreamConnection connection = Mockito.mock(StreamConnection.class);
        Mockito.when(connection.isOpen()).thenReturn(true);
        AtomicBoolean listenerInstalled = new AtomicBoolean();
        AtomicBoolean listenerAtResponse = new AtomicBoolean();
        AtomicReference<ConnectContext> registrationAtResponse = new AtomicReference<>();
        Mockito.doAnswer(call -> {
            listenerInstalled.set(true);
            return null;
        }).when(connection).setCloseListener(Mockito.any());
        try (MockedStatic<MysqlProto> protocol = Mockito.mockStatic(MysqlProto.class)) {
            protocol.when(() -> MysqlProto.negotiate(ctx)).thenReturn(true);
            protocol.when(() -> MysqlProto.sendResponsePacket(ctx)).thenAnswer(call -> {
                registrationAtResponse.set(scheduler.getContext(ctx.getConnectionId()));
                listenerAtResponse.set(listenerInstalled.get());
                throw new IOException("client disconnected during login response");
            });
            new AcceptListener(scheduler).handleConnection(ctx, connection);
        }
        Assertions.assertSame(ctx, registrationAtResponse.get());
        Assertions.assertTrue(listenerAtResponse.get());
        Mockito.verify(connection).setCloseListener(Mockito.any());
        Assertions.assertEquals(0, scheduler.getConnectionNum());
        Assertions.assertFalse(ctx.tryBeginCommand());
        Mockito.verify(ctx).releaseProtocolSession();
        Mockito.verify(ctx).closeTxn();
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
            Assertions.assertFalse(ctx.tryBeginCommand());
            Mockito.verify(ctx).cleanup();
            Mockito.verify(ctx).releaseProtocolSession();
            Mockito.verify(ctx).closeTxn();
        }
    }

    @Test
    public void testReadFailureClosesTheSessionAfterTheCommandExits() throws Exception {
        verifyReadFailure(false);
    }

    @Test
    public void testResponseFailureUpgradesTheDeferredClose() throws Exception {
        verifyReadFailure(true);
    }

    private void verifyReadFailure(boolean closeAfterResponse) throws Exception {
        MysqlProtocolAdapter adapter = Mockito.mock(MysqlProtocolAdapter.class);
        Mockito.when(adapter.type()).thenReturn(ConnectContext.ConnectType.MYSQL);
        ConnectContext ctx = new ConnectContext(adapter);
        ctx.activate();
        ConnectProcessor processor = Mockito.mock(ConnectProcessor.class);
        AtomicReference<ConnectContext> commandBinding = new AtomicReference<>();
        AtomicBoolean transportClosed = new AtomicBoolean();
        AtomicReference<Boolean> closedDuringCommand = new AtomicReference<>();
        Mockito.doAnswer(call -> {
            transportClosed.set(true);
            return null;
        }).when(adapter).closeConnection(ctx);
        Mockito.doAnswer(call -> {
            commandBinding.set(ConnectContext.get());
            if (closeAfterResponse) {
                ctx.requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
            }
            closedDuringCommand.set(transportClosed.get());
            throw new IOException("read or response write failed");
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
        Assertions.assertSame(ctx, commandBinding.get());
        Assertions.assertEquals(Boolean.FALSE, closedDuringCommand.get());
        Assertions.assertFalse(ctx.tryBeginCommand());
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
        Assertions.assertFalse(ctx.isClosingAfterResponse());
        Mockito.verify(adapter, Mockito.never()).resumeAcceptQuery();
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    public void testQuitResponseIsFlushedBeforeTheChannelCloses() throws Exception {
        RecordingMysqlChannel mysqlChannel = Mockito.spy(new RecordingMysqlChannel());
        mysqlChannel.queueRequest(new byte[] {1}); // COM_QUIT
        MysqlProtocolAdapter adapter = Mockito.spy(new MysqlProtocolAdapter(mysqlChannel));
        Mockito.doNothing().when(adapter).suspendAcceptQuery();
        Mockito.doNothing().when(adapter).stopAcceptQuery();
        ConnectContext ctx = new ConnectContext(adapter);
        ctx.setEnv(ConnectPoolTestSupport.envAllowing(10));
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ConnectScheduler scheduler = new ConnectScheduler(10);
        scheduler.submit(ctx);
        ctx.setConnectScheduler(scheduler);
        Assertions.assertEquals(-1, ctx.activateClientSession(scheduler.getConnectPoolMgr()));
        AtomicInteger packetsAtClose = new AtomicInteger(-1);
        AtomicBoolean flushedAtClose = new AtomicBoolean();
        AtomicInteger connectionsAtClose = new AtomicInteger(-1);
        Mockito.doAnswer(call -> {
            packetsAtClose.set(mysqlChannel.getOutbound().size());
            flushedAtClose.set(mysqlChannel.getOutbound().stream().allMatch(packet -> packet.isFlushed()));
            connectionsAtClose.set(scheduler.getConnectionNum());
            return null;
        }).when(mysqlChannel).close();
        ConduitStreamSourceChannel channel = Mockito.mock(ConduitStreamSourceChannel.class);
        XnioWorker worker = Mockito.mock(XnioWorker.class);
        Mockito.when(channel.getWorker()).thenReturn(worker);
        Mockito.doAnswer(call -> {
            call.<Runnable>getArgument(0).run();
            return null;
        }).when(worker).execute(Mockito.any(Runnable.class));
        try (MockedStatic<XnioIoThread> ioThread = Mockito.mockStatic(XnioIoThread.class)) {
            new ReadListener(ctx, new MysqlConnectProcessor(ctx)).handleEvent(channel);
        }
        Assertions.assertFalse(ctx.tryBeginCommand());
        // Assert outside cleanup callbacks: cleanup deliberately catches failures from its hooks.
        Assertions.assertEquals(1, packetsAtClose.get());
        Assertions.assertTrue(flushedAtClose.get());
        Assertions.assertEquals(0, connectionsAtClose.get());
        Mockito.verify(mysqlChannel).close();
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verify(adapter, Mockito.never()).resumeAcceptQuery();
    }

    @Test
    public void testAlreadyClosedChannelCannotBeRegistered() {
        ConnectContext ctx = Mockito.spy(ConnectPoolTestSupport.mysqlConnection(
                ConnectPoolTestSupport.envAllowing(10), UserIdentity.ROOT));
        ConnectScheduler scheduler = new ConnectScheduler(10);
        scheduler.submit(ctx);
        StreamConnection connection = Mockito.mock(StreamConnection.class);
        Mockito.when(connection.isOpen()).thenReturn(false);
        try (MockedStatic<MysqlProto> protocol = Mockito.mockStatic(MysqlProto.class)) {
            new AcceptListener(scheduler).handleConnection(ctx, connection);
            protocol.verifyNoInteractions();
        }
        Assertions.assertEquals(0, scheduler.getConnectionNum());
        Assertions.assertFalse(ctx.tryBeginCommand());
        Mockito.verify(ctx).releaseProtocolSession();
        Mockito.verify(ctx).closeTxn();
    }
}

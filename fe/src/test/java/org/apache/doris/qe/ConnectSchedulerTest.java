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

package org.apache.doris.qe;

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlProto;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest({ConnectScheduler.class, MysqlProto.class, ConnectContext.class})
public class ConnectSchedulerTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectScheduler.class);
    private AtomicLong succSubmit;

    private ConnectProcessor mockProcessor() {
        ConnectProcessor processor;
        // mock processor loop
        processor = EasyMock.createMock(ConnectProcessor.class);
        processor.loop();
        EasyMock.expectLastCall().andDelegateTo(new ConnectProcessor(null) {
            @Override
            public void loop() {
                LOG.warn("starts loop");
                // Make cancel thread to work
                succSubmit.incrementAndGet();
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    LOG.warn("sleep exception");
                }
            }
        }).anyTimes();
        EasyMock.replay(processor);

        return processor;
    }

    private ConnectProcessor mockExceptionProcessor() {
        ConnectProcessor processor;
        // mock processor loop
        processor = EasyMock.createMock(ConnectProcessor.class);
        processor.loop();
        EasyMock.expectLastCall().andDelegateTo(new ConnectProcessor(null) {
            @Override
            public void loop() {
                throw new RuntimeException("failed");
            }
        }).anyTimes();
        EasyMock.replay(processor);

        return processor;
    }

    @Before
    public void setUp() throws Exception {
        succSubmit = new AtomicLong(0);

        MysqlChannel channel = EasyMock.createMock(MysqlChannel.class);
        EasyMock.expect(channel.getRemoteIp()).andReturn("192.168.1.1").anyTimes();
        EasyMock.replay(channel);
        PowerMock.expectNew(MysqlChannel.class, EasyMock.isA(SocketChannel.class)).andReturn(channel).anyTimes();
        PowerMock.replay(MysqlChannel.class);

        // mock negotiate
        PowerMock.mockStatic(MysqlProto.class);
        EasyMock.expect(MysqlProto.negotiate(EasyMock.anyObject(ConnectContext.class))).andReturn(true).anyTimes();
        MysqlProto.sendResponsePacket(EasyMock.anyObject(ConnectContext.class));
        EasyMock.expectLastCall().anyTimes();
        PowerMock.replay(MysqlProto.class);
    }

    @Test
    public void testSubmit() throws Exception {
        // mock new processor
        PowerMock.expectNew(ConnectProcessor.class, EasyMock.anyObject(ConnectContext.class))
                .andReturn(mockProcessor()).once();
        PowerMock.expectNew(ConnectProcessor.class, EasyMock.anyObject(ConnectContext.class))
                .andReturn(mockProcessor()).anyTimes();
        PowerMock.replay(ConnectProcessor.class);
        ConnectScheduler scheduler = new ConnectScheduler(10);

        for (int i = 0; i < 2; ++i) {
            ConnectContext context = new ConnectContext(EasyMock.createMock(SocketChannel.class));
            if (i == 1) {
                context.setCatalog(AccessTestUtil.fetchBlockCatalog());
            } else {
                context.setCatalog(AccessTestUtil.fetchAdminCatalog());
            }
            context.setQualifiedUser("root");
            Assert.assertTrue(scheduler.submit(context));
            Assert.assertEquals(i, context.getConnectionId());
        }
    }

    @Test
    public void testProcessException() throws Exception {
        PowerMock.expectNew(ConnectProcessor.class, EasyMock.anyObject(ConnectContext.class))
                .andReturn(mockExceptionProcessor()).once();
        PowerMock.replay(ConnectProcessor.class);
        ConnectScheduler scheduler = new ConnectScheduler(10);

        ConnectContext context = new ConnectContext(EasyMock.createMock(SocketChannel.class));
        context.setCatalog(AccessTestUtil.fetchAdminCatalog());
        context.setQualifiedUser("root");
        Assert.assertTrue(scheduler.submit(context));
        Assert.assertEquals(0, context.getConnectionId());

        Thread.sleep(1000);
        Assert.assertNull(scheduler.getContext(0));
    }

    @Test
    public void testSubmitFail() throws InterruptedException {
        ConnectScheduler scheduler = new ConnectScheduler(10);
        Assert.assertFalse(scheduler.submit(null));
    }

    @Test
    public void testSubmitTooMany() throws InterruptedException {
        ConnectScheduler scheduler = new ConnectScheduler(0);
        ConnectContext context = new ConnectContext(EasyMock.createMock(SocketChannel.class));
        Assert.assertTrue(scheduler.submit(context));
    }

    @Test
    public void testNegotiateFail() throws InterruptedException, IOException {
        ConnectScheduler scheduler = new ConnectScheduler(10);
        PowerMock.mockStatic(MysqlProto.class);
        EasyMock.expect(MysqlProto.negotiate(EasyMock.anyObject(ConnectContext.class))).andReturn(false).anyTimes();
        PowerMock.replay(MysqlProto.class);
        Assert.assertTrue(scheduler.submit(new ConnectContext(EasyMock.createMock(SocketChannel.class))));
    }

    @Test
    public void testNegotiateIOException() throws InterruptedException, IOException {
        ConnectScheduler scheduler = new ConnectScheduler(10);
        PowerMock.mockStatic(MysqlProto.class);
        EasyMock.expect(MysqlProto.negotiate(EasyMock.anyObject(ConnectContext.class)))
                .andThrow(new IOException("failed")).anyTimes();
        PowerMock.replay(MysqlProto.class);
        Assert.assertTrue(scheduler.submit(new ConnectContext(EasyMock.createMock(SocketChannel.class))));
    }
}

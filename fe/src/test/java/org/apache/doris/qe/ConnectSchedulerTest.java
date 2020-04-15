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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;

public class ConnectSchedulerTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectScheduler.class);
    private static AtomicLong succSubmit;
    @Mocked
    SocketChannel socketChannel;
    @Mocked
    MysqlChannel channel;
    @Mocked
    MysqlProto mysqlProto;

    @Before
    public void setUp() throws Exception {
        succSubmit = new AtomicLong(0);
        new Expectations() {
            {
                channel.getRemoteIp();
                minTimes = 0;
                result = "192.168.1.1";

                // mock negotiate
                MysqlProto.negotiate((ConnectContext) any);
                minTimes = 0;
                result = true;

                MysqlProto.sendResponsePacket((ConnectContext) any);
                minTimes = 0;
            }
        };
    }

    @Test
    public void testSubmit(@Mocked ConnectProcessor processor) throws Exception {
        // mock new processor
        new Expectations() {
            {
                processor.loop();
                result = new Delegate() {
                    void fakeLoop() {
                        LOG.warn("starts loop");
                        // Make cancel thread to work
                        succSubmit.incrementAndGet();
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            LOG.warn("sleep exception");
                        }
                    }
                };
            }
        };

        ConnectScheduler scheduler = new ConnectScheduler(10);
        for (int i = 0; i < 2; ++i) {
            ConnectContext context = new ConnectContext(socketChannel);
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
    public void testProcessException(@Mocked ConnectProcessor processor) throws Exception {
        new Expectations() {
            {
                processor.loop();
                result = new RuntimeException("failed");
            }
        };

        ConnectScheduler scheduler = new ConnectScheduler(10);

        ConnectContext context = new ConnectContext(socketChannel);
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
        ConnectContext context = new ConnectContext(socketChannel);
        Assert.assertTrue(scheduler.submit(context));
    }
}

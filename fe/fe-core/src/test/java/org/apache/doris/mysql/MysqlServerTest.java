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

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class MysqlServerTest {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlServerTest.class);

    private int submitNum;
    private int submitFailNum;
    @Mocked
    private ConnectScheduler scheduler;
    @Mocked
    private ConnectScheduler badScheduler;

    @Before
    public void setUp() {
        submitNum = 0;
        submitFailNum = 0;
        new Expectations() {
            {
                scheduler.submit((ConnectContext) any);
                minTimes = 0;
                result = new Delegate() {
                    public Boolean answer() throws Throwable {
                        LOG.info("answer.");
                        synchronized (MysqlServerTest.this) {
                            submitNum++;
                        }
                        return Boolean.TRUE;
                    }
                };

                badScheduler.submit((ConnectContext) any);
                minTimes = 0;
                result = new Delegate() {
                    public Boolean answer() throws Throwable {
                        LOG.info("answer.");
                        synchronized (MysqlServerTest.this) {
                            submitFailNum++;
                        }
                        return Boolean.FALSE;
                    }
                };
            }
        };
    }

    @Test
    public void testNormal() throws IOException, InterruptedException {
        int port = TestWithFeService.findValidPort();
        MysqlServer server = new MysqlServer(port, scheduler);
        Assert.assertTrue(server.start());

        // submit
        SocketChannel channel = SocketChannel.open();
        channel.connect(new InetSocketAddress("127.0.0.1", port));
        // sleep to wait mock process
        Thread.sleep(2000);
        channel.close();

        // submit twice
        channel = SocketChannel.open();
        channel.connect(new InetSocketAddress("127.0.0.1", port));
        // sleep to wait mock process
        Thread.sleep(2000);
        channel.close();

        // stop and join
        server.stop();

        Assert.assertEquals(2, submitNum);
    }

    @Test
    public void testBindFail() throws IOException {
        int port = TestWithFeService.findValidPort();
        MysqlServer server = new MysqlServer(port, scheduler);
        Assert.assertTrue(server.start());
        MysqlServer server1 = new MysqlServer(port, scheduler);
        Assert.assertFalse(server1.start());

        server.stop();
    }

    @Test
    public void testSubFail() throws IOException, InterruptedException {
        int port = TestWithFeService.findValidPort();
        MysqlServer server = new MysqlServer(port, badScheduler);
        Assert.assertTrue(server.start());

        // submit
        SocketChannel channel = SocketChannel.open();
        channel.connect(new InetSocketAddress("127.0.0.1", port));
        // sleep to wait mock process
        Thread.sleep(1000);
        channel.close();

        // submit twice
        channel = SocketChannel.open();
        channel.connect(new InetSocketAddress("127.0.0.1", port));
        // sleep to wait mock process
        Thread.sleep(1000);
        channel.close();

        // stop and join
        server.stop();

        Assert.assertEquals(2, submitFailNum);
    }

}

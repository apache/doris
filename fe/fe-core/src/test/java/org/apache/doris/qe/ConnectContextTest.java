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

import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.thrift.TUniqueId;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.channels.SocketChannel;
import java.util.List;

public class ConnectContextTest {
    @Mocked
    private MysqlChannel channel;
    @Mocked
    private StmtExecutor executor;
    @Mocked
    private SocketChannel socketChannel;
    @Mocked
    private Env env;
    @Mocked
    private ConnectScheduler connectScheduler;
    @Mocked
    private PaloAuth paloAuth;
    @Mocked
    private String qualifiedUser;

    @Before
    public void setUp() throws Exception {
        new Expectations() {
            {
                channel.getRemoteHostPortString();
                minTimes = 0;
                result = "127.0.0.1:12345";

                channel.close();
                minTimes = 0;

                channel.getRemoteIp();
                minTimes = 0;
                result = "192.168.1.1";

                executor.cancel();
                minTimes = 0;
            }
        };
    }

    @Test
    public void testNormal() {
        ConnectContext ctx = new ConnectContext(socketChannel);

        // State
        Assert.assertNotNull(ctx.getState());

        // Capability
        Assert.assertEquals(MysqlCapability.DEFAULT_CAPABILITY, ctx.getServerCapability());
        ctx.setCapability(new MysqlCapability(10));
        Assert.assertEquals(new MysqlCapability(10), ctx.getCapability());

        // Kill flag
        Assert.assertFalse(ctx.isKilled());
        ctx.setKilled();
        Assert.assertTrue(ctx.isKilled());

        // Current cluster
        Assert.assertEquals("", ctx.getClusterName());
        ctx.setCluster("testCluster");
        Assert.assertEquals("testCluster", ctx.getClusterName());

        // Current db
        Assert.assertEquals("", ctx.getDatabase());
        ctx.setDatabase("testCluster:testDb");
        Assert.assertEquals("testCluster:testDb", ctx.getDatabase());

        // User
        ctx.setQualifiedUser("testCluster:testUser");
        Assert.assertEquals("testCluster:testUser", ctx.getQualifiedUser());

        // Serializer
        Assert.assertNotNull(ctx.getSerializer());

        // Session variable
        Assert.assertNotNull(ctx.getSessionVariable());

        // connect scheduler
        Assert.assertNull(ctx.getConnectScheduler());
        ctx.setConnectScheduler(connectScheduler);
        Assert.assertNotNull(ctx.getConnectScheduler());

        // connection id
        ctx.setConnectionId(101);
        Assert.assertEquals(101, ctx.getConnectionId());

        // command
        ctx.setCommand(MysqlCommand.COM_PING);
        Assert.assertEquals(MysqlCommand.COM_PING, ctx.getCommand());

        // Thread info
        Assert.assertNotNull(ctx.toThreadInfo(false));
        List<String> row = ctx.toThreadInfo(false).toRow(1000);
        Assert.assertEquals(9, row.size());
        Assert.assertEquals("101", row.get(0));
        Assert.assertEquals("testUser", row.get(1));
        Assert.assertEquals("127.0.0.1:12345", row.get(2));
        Assert.assertEquals("testCluster", row.get(3));
        Assert.assertEquals("testDb", row.get(4));
        Assert.assertEquals("Ping", row.get(5));
        Assert.assertEquals("1", row.get(6));
        Assert.assertEquals("", row.get(7));
        Assert.assertEquals("", row.get(8));

        // Start time
        Assert.assertEquals(0, ctx.getStartTime());
        ctx.setStartTime();
        Assert.assertNotSame(0, ctx.getStartTime());

        // query id
        ctx.setQueryId(new TUniqueId(100, 200));
        Assert.assertEquals(new TUniqueId(100, 200), ctx.queryId());

        // Catalog
        Assert.assertNull(ctx.getEnv());
        ctx.setEnv(env);
        Assert.assertNotNull(ctx.getEnv());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testSleepTimeout() {
        ConnectContext ctx = new ConnectContext(socketChannel);
        ctx.setCommand(MysqlCommand.COM_SLEEP);

        // sleep no time out
        ctx.setStartTime();
        Assert.assertFalse(ctx.isKilled());
        long now = ctx.getStartTime() + ctx.getSessionVariable().getWaitTimeoutS() * 1000 - 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Timeout
        ctx.setStartTime();
        now = ctx.getStartTime() + ctx.getSessionVariable().getWaitTimeoutS() * 1000 + 1;
        ctx.setExecutor(executor);
        ctx.checkTimeout(now);
        Assert.assertTrue(ctx.isKilled());

        // user query timeout
        ctx.setStartTime();
        now = ctx.getStartTime() + paloAuth.getQueryTimeout(qualifiedUser) * 1000 + 1;
        ctx.setExecutor(executor);
        ctx.checkTimeout(now);
        Assert.assertTrue(ctx.isKilled());

        // Kill
        ctx.kill(true);
        Assert.assertTrue(ctx.isKilled());
        ctx.kill(false);
        Assert.assertTrue(ctx.isKilled());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testOtherTimeout() {
        ConnectContext ctx = new ConnectContext(socketChannel);
        ctx.setCommand(MysqlCommand.COM_QUERY);

        // sleep no time out
        Assert.assertFalse(ctx.isKilled());
        long now = ctx.getSessionVariable().getQueryTimeoutS() * 1000 - 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Timeout
        now = ctx.getSessionVariable().getQueryTimeoutS() * 1000 + 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Kill
        ctx.kill(true);
        Assert.assertTrue(ctx.isKilled());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testThreadLocal() {
        ConnectContext ctx = new ConnectContext(socketChannel);
        Assert.assertNull(ConnectContext.get());
        ctx.setThreadLocalInfo();
        Assert.assertNotNull(ConnectContext.get());
        Assert.assertEquals(ctx, ConnectContext.get());
    }
}

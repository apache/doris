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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlEofPacket;
import org.apache.doris.mysql.MysqlErrPacket;
import org.apache.doris.mysql.MysqlOkPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.plugin.AuditEvent.AuditEventBuilder;
import org.apache.doris.proto.Data;
import org.apache.doris.thrift.TUniqueId;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import mockit.Expectations;
import mockit.Mocked;

public class ConnectProcessorTest {
    private static ByteBuffer initDbPacket;
    private static ByteBuffer pingPacket;
    private static ByteBuffer quitPacket;
    private static ByteBuffer queryPacket;
    private static ByteBuffer fieldListPacket;
    private static AuditEventBuilder auditBuilder = new AuditEventBuilder();
    private static ConnectContext myContext;

    @Mocked
    private static SocketChannel socketChannel;

    private static Data.PQueryStatistics statistics = Data.PQueryStatistics.newBuilder().build();

    @BeforeClass
    public static void setUpClass() {
        // Init Database packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(2);
            serializer.writeEofString("testCluster:testDb");
            initDbPacket = serializer.toByteBuffer();
        }

        // Ping packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(14);
            pingPacket = serializer.toByteBuffer();
        }

        // Quit packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(1);
            quitPacket = serializer.toByteBuffer();
        }

        // Query packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(3);
            serializer.writeEofString("select * from a");
            queryPacket = serializer.toByteBuffer();
        }

        // Field list packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(4);
            serializer.writeNulTerminateString("testTbl");
            serializer.writeEofString("");
            fieldListPacket = serializer.toByteBuffer();
        }
        statistics = statistics.toBuilder().setCpuMs(0L).setScanRows(0).setScanBytes(0).build();

        MetricRepo.init();
    }

    @Before
    public void setUp() throws Exception {
        initDbPacket.clear();
        pingPacket.clear();
        quitPacket.clear();
        queryPacket.clear();
        fieldListPacket.clear();
        // Mock
        MysqlChannel channel = new MysqlChannel(socketChannel);
        new Expectations(channel) {
            {
                channel.getRemoteHostPortString();
                minTimes = 0;
                result = "127.0.0.1:12345";
            }
        };
        myContext = new ConnectContext(socketChannel);
        Deencapsulation.setField(myContext, "mysqlChannel", channel);
    }

    private static MysqlChannel mockChannel(ByteBuffer packet) {
        try {
            MysqlChannel channel = new MysqlChannel(socketChannel);
            new Expectations(channel) {
                {
                    // Mock receive
                    channel.fetchOnePacket();
                    minTimes = 0;
                    result = packet;

                    // Mock reset
                    channel.setSequenceId(0);
                    times = 1;

                    // Mock send
                    // channel.sendOnePacket((ByteBuffer) any);
                    // minTimes = 0;
                    channel.sendAndFlush((ByteBuffer) any);
                    minTimes = 0;

                    channel.getRemoteHostPortString();
                    minTimes = 0;
                    result = "127.0.0.1:12345";
                }
            };
            return channel;
        } catch (IOException e) {
            return null;
        }
    }

    private static ConnectContext initMockContext(MysqlChannel channel, Catalog catalog) {
        ConnectContext context = new ConnectContext(socketChannel) {
            private boolean firstTimeToSetCommand = true;
            @Override
            public void setKilled() {
                myContext.setKilled();
            }
            @Override
            public MysqlSerializer getSerializer() {
                return myContext.getSerializer();
            }
            @Override
            public QueryState getState() {
                return myContext.getState();
            }
            @Override
            public void setStartTime() {
                myContext.setStartTime();
            }
            @Override
            public String getDatabase() {
                return myContext.getDatabase();
            }
            @Override
            public void setCommand(MysqlCommand command) {
                if (firstTimeToSetCommand) {
                    myContext.setCommand(command);
                    firstTimeToSetCommand = false;
                } else {
                    super.setCommand(command);
                }
            }
        };

        new Expectations(context) {
            {
                context.getMysqlChannel();
                minTimes = 0;
                result = channel;

                context.isKilled();
                minTimes = 0;
                maxTimes = 3;
                returns(false, true, false);

                context.getCatalog();
                minTimes = 0;
                result = catalog;

                context.getAuditEventBuilder();
                minTimes = 0;
                result = auditBuilder;

                context.getQualifiedUser();
                minTimes = 0;
                result = "testCluster:user";

                context.getClusterName();
                minTimes = 0;
                result = "testCluster";

                context.getStartTime();
                minTimes = 0;
                result = 0L;

                context.getReturnRows();
                minTimes = 0;
                result = 1L;

                context.setStmtId(anyLong);
                minTimes = 0;

                context.getStmtId();
                minTimes = 0;
                result = 1L;

                context.queryId();
                minTimes = 0;
                result = new TUniqueId();
            }
        };

        return context;
    }

    @Test
    public void testQuit() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(quitPacket), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_QUIT, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assert.assertTrue(myContext.isKilled());
    }

    @Test
    public void testInitDb() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(initDbPacket), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_INIT_DB, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
    }

    @Test
    public void testInitDbFail() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(initDbPacket), AccessTestUtil.fetchBlockCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_INIT_DB, myContext.getCommand());
        Assert.assertFalse(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
    }

    @Test
    public void testPing() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(pingPacket), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_PING, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assert.assertFalse(myContext.isKilled());
    }

    @Test
    public void testPingLoop() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(pingPacket), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.loop();
        Assert.assertEquals(MysqlCommand.COM_PING, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assert.assertFalse(myContext.isKilled());
    }

    @Test
    public void testQuery(@Mocked StmtExecutor executor) throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Mock statement executor
        new Expectations() {
            {
                executor.getQueryStatisticsForAuditLog();
                minTimes = 0;
                result = statistics;
            }
        };

        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
    }

    @Test
    public void testQueryWithUserInfo(@Mocked StmtExecutor executor) throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Mock statement executor
        new Expectations() {
            {
                executor.getQueryStatisticsForAuditLog();
                minTimes = 0;
                result = statistics;
            }
        };
        processor.processOnce();
        StmtExecutor er = Deencapsulation.getField(processor, "executor");
        Assert.assertTrue(er.getParsedStmt().getUserInfo() != null);
    }

    @Test
    public void testQueryFail(@Mocked StmtExecutor executor) throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Mock statement executor
        new Expectations() {
            {
                executor.execute();
                minTimes = 0;
                result = new IOException("Fail");

                executor.getQueryStatisticsForAuditLog();
                minTimes = 0;
                result = statistics;
            }
        };
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
    }

    @Test
    public void testQueryFail2(@Mocked StmtExecutor executor) throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Mock statement executor
        new Expectations() {
            {
                executor.execute();
                minTimes = 0;
                result = new NullPointerException("Fail");

                executor.getQueryStatisticsForAuditLog();
                minTimes = 0;
                result = statistics;
            }
        };
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
    }

    @Test
    public void testFieldList() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(fieldListPacket), AccessTestUtil.fetchAdminCatalog());

        myContext.setDatabase("testCluster:testDb");
        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_FIELD_LIST, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlEofPacket);
    }

    @Test
    public void testFieldListFailEmptyTable() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(4);
        serializer.writeNulTerminateString("");
        serializer.writeEofString("");

        ConnectContext ctx = initMockContext(mockChannel(serializer.toByteBuffer()),
                AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_FIELD_LIST, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assert.assertEquals("Empty tableName", myContext.getState().getErrorMessage());
    }

    @Test
    public void testFieldListFailNoDb() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(4);
        serializer.writeNulTerminateString("testTable");
        serializer.writeEofString("");

        myContext.setDatabase("testCluster:emptyDb");
        ConnectContext ctx = initMockContext(mockChannel(serializer.toByteBuffer()),
                AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_FIELD_LIST, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assert.assertEquals("Unknown database(testCluster:emptyDb)", myContext.getState().getErrorMessage());
    }

    @Test
    public void testFieldListFailNoTable() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(4);
        serializer.writeNulTerminateString("emptyTable");
        serializer.writeEofString("");

        myContext.setDatabase("testCluster:testDb");
        ConnectContext ctx = initMockContext(mockChannel(serializer.toByteBuffer()),
                AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_FIELD_LIST, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assert.assertEquals("Unknown table(emptyTable)", myContext.getState().getErrorMessage());
    }

    @Test
    public void testUnsupportedCommand() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(5);
        ByteBuffer packet = serializer.toByteBuffer();
        ConnectContext ctx = initMockContext(mockChannel(packet), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_CREATE_DB, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assert.assertFalse(myContext.isKilled());
    }

    @Test
    public void testUnknownCommand() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(101);
        ByteBuffer packet = serializer.toByteBuffer();
        ConnectContext ctx = initMockContext(mockChannel(packet), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_SLEEP, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assert.assertFalse(myContext.isKilled());
    }

    @Test
    public void testNullPacket() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(null), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.loop();
        Assert.assertTrue(myContext.isKilled());
    }
}

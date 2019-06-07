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
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlEofPacket;
import org.apache.doris.mysql.MysqlErrPacket;
import org.apache.doris.mysql.MysqlOkPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.proto.PQueryStatistics;
import org.apache.doris.thrift.TUniqueId;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*"})
@PrepareForTest({Catalog.class, StmtExecutor.class, ConnectProcessor.class, ConnectContext.class})
public class ConnectProcessorTest {
    private static ByteBuffer initDbPacket;
    private static ByteBuffer pingPacket;
    private static ByteBuffer quitPacket;
    private static ByteBuffer queryPacket;
    private static ByteBuffer fieldListPacket;
    private static AuditBuilder auditBuilder = new AuditBuilder();
    ConnectContext myContext;

    private static PQueryStatistics statistics = new PQueryStatistics();

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

        statistics.scan_bytes = 0L;
        statistics.scan_rows = 0L;

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
        MysqlChannel channel = EasyMock.createNiceMock(MysqlChannel.class);
        PowerMock.expectNew(MysqlChannel.class, EasyMock.isA(SocketChannel.class)).andReturn(channel).anyTimes();
        EasyMock.expect(channel.getRemoteHostPortString()).andReturn("127.0.0.1:12345").anyTimes();
        PowerMock.replay(MysqlChannel.class);
        myContext = new ConnectContext(EasyMock.createMock(SocketChannel.class));
    }

    private MysqlChannel mockChannel(ByteBuffer packet) {
        try {
            MysqlChannel channel = EasyMock.createNiceMock(MysqlChannel.class);
            // Mock receive
            EasyMock.expect(channel.fetchOnePacket()).andReturn(packet).once();
            // Mock reset
            channel.setSequenceId(0);
            EasyMock.expectLastCall().once();
            // Mock send
            // channel.sendOnePacket(EasyMock.isA(ByteBuffer.class));
            channel.sendAndFlush(EasyMock.isA(ByteBuffer.class));
            EasyMock.expectLastCall().anyTimes();

            EasyMock.expect(channel.getRemoteHostPortString()).andReturn("127.0.0.1:12345").anyTimes();

            EasyMock.replay(channel);

            return channel;
        } catch (IOException e) {
            return null;
        }
    }

    private ConnectContext initMockContext(MysqlChannel channel, Catalog catalog) {
        ConnectContext context = EasyMock.createMock(ConnectContext.class);
        EasyMock.expect(context.getMysqlChannel()).andReturn(channel).anyTimes();
        EasyMock.expect(context.isKilled()).andReturn(false).once();
        EasyMock.expect(context.isKilled()).andReturn(true).once();
        EasyMock.expect(context.getCatalog()).andReturn(catalog).anyTimes();
        EasyMock.expect(context.getState()).andReturn(myContext.getState()).anyTimes();
        EasyMock.expect(context.getAuditBuilder()).andReturn(auditBuilder).anyTimes();
        EasyMock.expect(context.getQualifiedUser()).andReturn("testCluster:user").anyTimes();
        EasyMock.expect(context.getClusterName()).andReturn("testCluster").anyTimes();
        EasyMock.expect(context.getStartTime()).andReturn(0L).anyTimes();
        EasyMock.expect(context.getSerializer()).andDelegateTo(myContext).anyTimes();
        EasyMock.expect(context.getReturnRows()).andReturn(1L).anyTimes();
        EasyMock.expect(context.isKilled()).andReturn(false).anyTimes();
        context.setKilled();
        EasyMock.expectLastCall().andDelegateTo(myContext).anyTimes();
        context.setCommand(EasyMock.anyObject(MysqlCommand.class));
        EasyMock.expectLastCall().andDelegateTo(myContext).once();
        context.setCommand(EasyMock.anyObject(MysqlCommand.class));
        EasyMock.expectLastCall().anyTimes();
        context.setStartTime();
        EasyMock.expectLastCall().andDelegateTo(myContext).anyTimes();
        context.getDatabase();
        EasyMock.expectLastCall().andDelegateTo(myContext).anyTimes();
        context.setStmtId(EasyMock.anyLong());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(context.getStmtId()).andReturn(1L).anyTimes();
        EasyMock.expect(context.queryId()).andReturn(new TUniqueId()).anyTimes();
        EasyMock.replay(context);

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
    public void testQuery() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Mock statement executor
        StmtExecutor qe = EasyMock.createNiceMock(StmtExecutor.class);
        qe.execute();
        EasyMock.expect(qe.getQueryStatisticsForAuditLog()).andReturn(statistics);
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(qe);
        PowerMock.expectNew(
                StmtExecutor.class,
                EasyMock.isA(ConnectContext.class),
                EasyMock.isA(String.class)).andReturn(qe).anyTimes();

        PowerMock.replay(StmtExecutor.class);

        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
    }

    @Test
    public void testQueryFail() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Mock statement executor
        StmtExecutor qe = EasyMock.createNiceMock(StmtExecutor.class);
        qe.execute();
        EasyMock.expectLastCall().andThrow(new IOException("Fail")).anyTimes();
        EasyMock.expect(qe.getQueryStatisticsForAuditLog()).andReturn(statistics);
        EasyMock.replay(qe);
        PowerMock.expectNew(StmtExecutor.class, EasyMock.isA(ConnectContext.class), EasyMock.isA(String.class))
                .andReturn(qe).anyTimes();
        PowerMock.replay(StmtExecutor.class);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
    }

    @Test
    public void testQueryFail2() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Mock statement executor
        StmtExecutor qe = EasyMock.createNiceMock(StmtExecutor.class);
        qe.execute();
        EasyMock.expect(qe.getQueryStatisticsForAuditLog()).andReturn(statistics);
        EasyMock.expectLastCall().andThrow(new NullPointerException("Fail")).anyTimes();
        EasyMock.replay(qe);
        PowerMock.expectNew(StmtExecutor.class, EasyMock.isA(ConnectContext.class), EasyMock.isA(String.class))
                .andReturn(qe).anyTimes();
        PowerMock.replay(StmtExecutor.class);

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

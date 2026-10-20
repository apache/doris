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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.mysql.DummyMysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.CommonResultSet.CommonResultSetMetaData;
import org.apache.doris.thrift.TMasterOpRequest;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MysqlPreparedStatementProtocolTest {
    @Test
    public void testEmptyCursorDoesNotDependOnClientAttributes() throws Exception {
        for (Map<String, String> attributes : Arrays.asList(Collections.<String, String>emptyMap(),
                ImmutableMap.of("_client_name", "proxy", "_client_version", "0"),
                ImmutableMap.of("_client_name", "MySQL Connector/J", "_client_version", "8.0.27"),
                ImmutableMap.of("_client_name", "MySQL Connector/J", "_client_version", "9.7.0"))) {
            ConnectContext context = context();
            context.setConnectAttributes(attributes);
            RecordingChannel channel = (RecordingChannel) context.getMysqlChannel();
            PreparedStatementContext prepared = prepared(context);
            StmtExecutor executor = new StmtExecutor(context, new OriginStatement("select 1", 0), true);
            executor.sendResultSet(result(Collections.emptyList()));
            MysqlConnectProcessor processor = new MysqlConnectProcessor(context);
            processor.finalizeCommand();

            // Column count, one column definition, and one unambiguous cursor-opening packet.
            Assertions.assertEquals(3, channel.packets.size());
            Assertions.assertEquals(0x40, Byte.toUnsignedInt(channel.last().get(3)) & 0xC0);
            Assertions.assertTrue(prepared.isEmptyCursorOpen());
            context.clear();
            Assertions.assertNull(context.getExecutingPreparedStatement());
            Assertions.assertTrue(prepared.isEmptyCursorOpen());

            context.getState().reset();
            processor.handleStmtFetch(7);
            processor.finalizeCommand();
            Assertions.assertEquals(0x80, Byte.toUnsignedInt(channel.last().get(3)) & 0xC0);
            Assertions.assertFalse(prepared.isEmptyCursorOpen());
        }
    }

    @Test
    public void testBinaryNullRowIsNotAnEmptyCursor() throws Exception {
        ConnectContext context = context();
        PreparedStatementContext prepared = prepared(context);
        StmtExecutor executor = new StmtExecutor(context, new OriginStatement("select null", 0), true);
        executor.sendBinaryResultRow(result(Collections.singletonList(Collections.singletonList(null))));
        Assertions.assertArrayEquals(new byte[] {0, 4},
                ((RecordingChannel) context.getMysqlChannel()).last().array());
        context.getState().setEof();
        new MysqlConnectProcessor(context).finalizeCommand();
        Assertions.assertTrue(context.hasMysqlResultRows());
        Assertions.assertFalse(prepared.isEmptyCursorOpen());

        context.resetMysqlResult();
        Assertions.assertFalse(context.hasMysqlResultRows());
        context.getState().reset();
        executor.sendResultSet(result(Collections.emptyList()));
        new MysqlConnectProcessor(context).finalizeCommand();
        Assertions.assertTrue(prepared.isEmptyCursorOpen());
    }

    @Test
    public void testForwardedCursorIsRetainedOnClientFacingFrontend() throws Exception {
        ConnectContext context = context();
        PreparedStatementContext prepared = prepared(context);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.hasForwardedToMaster()).thenReturn(true);
        Mockito.when(executor.getOutputPacket()).thenReturn(ByteBuffer.wrap(
                new byte[] {(byte) 0xFE, 0, 0, 0x40, 0, 0, 0, 0}));
        MysqlConnectProcessor processor = new MysqlConnectProcessor(context);
        processor.executor = executor;
        processor.finalizeCommand();
        Assertions.assertTrue(prepared.isEmptyCursorOpen());
        Mockito.verify(executor).sendProxyQueryResult();
        context.beginPreparedExecute(prepared);
        Assertions.assertFalse(prepared.isEmptyCursorOpen());
    }

    @Test
    public void testOldFrontendKeepsLegacyMetadataFraming() throws Exception {
        ConnectContext context = context();
        context.setConnectAttributes(ImmutableMap.of(
                "_client_name", "MySQL Connector/J", "_client_version", "8.0.27"));
        TMasterOpRequest request = new TMasterOpRequest();
        request.setClientDeprecatedEOF(true);
        request.setCursorFetchRequested(true);
        ConnectProcessor.restoreForwardedMysqlContext(context, request);
        Assertions.assertFalse(context.isEmptyCursorSupported());
        PreparedStatementContext prepared = prepared(context);
        new StmtExecutor(context, new OriginStatement("select 1", 0), true)
                .sendResultSet(result(Collections.emptyList()));
        new MysqlConnectProcessor(context).finalizeCommand();
        Assertions.assertEquals(4, ((RecordingChannel) context.getMysqlChannel()).packets.size());
        Assertions.assertFalse(prepared.isEmptyCursorOpen());
    }

    private ConnectContext context() {
        ConnectContext context = new ConnectContext();
        context.mysqlChannel = new RecordingChannel();
        context.getMysqlChannel().setClientDeprecatedEOF();
        context.setCursorFetchRequested(true);
        context.setCommand(MysqlCommand.COM_STMT_EXECUTE);
        return context;
    }

    private PreparedStatementContext prepared(ConnectContext context) throws Exception {
        PreparedStatementContext prepared = new PreparedStatementContext(null, context, new StatementContext(), "7");
        context.addPreparedStatementContext("7", prepared);
        context.beginPreparedExecute(prepared);
        return prepared;
    }

    private CommonResultSet result(List<List<String>> rows) {
        return new CommonResultSet(new CommonResultSetMetaData(
                Collections.singletonList(new Column("v", PrimitiveType.INT))), rows);
    }

    private static class RecordingChannel extends DummyMysqlChannel {
        private final List<ByteBuffer> packets = new ArrayList<>();

        private RecordingChannel() {
            serializer = MysqlSerializer.newInstance();
        }

        @Override
        public void sendOnePacket(ByteBuffer packet) {
            byte[] copy = new byte[packet.remaining()];
            packet.duplicate().get(copy);
            packets.add(ByteBuffer.wrap(copy));
        }

        @Override
        public void sendAndFlush(ByteBuffer packet) {
            sendOnePacket(packet);
        }

        private ByteBuffer last() {
            return packets.get(packets.size() - 1);
        }
    }
}
